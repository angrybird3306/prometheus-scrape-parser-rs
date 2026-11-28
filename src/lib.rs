// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The prometheus-scrape-parser-rs Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A parser for the two Prometheus exposition formats — classic text and
//! delimited protobuf ([`Parser`] drives both) — a port of `expfmt` from
//! Go's `prometheus/common`.

use std::borrow::Cow;

mod assemble;
mod error;
mod lexer;
mod mode;
mod model;
mod parse_proto;
mod view;
mod parse_text;
mod parser;
pub mod proto;
mod search;

pub use crate::error::{Disposition, Error, ErrorKind};
pub use crate::lexer::{LineView, SampleLine, Token};
pub use crate::mode::{LenientMode, Mode, Scheme, StrictMode};
pub use crate::model::{
    Bucket, HistogramValue, IntOrFloat, Label, Metric, MetricFamily, MetricType, MetricValue,
    Quantile, SummaryValue,
};
pub use crate::view::{BucketsRef, HistogramValueRef, MetricValueRef, QuantilesRef, SummaryValueRef};
pub use crate::parse_proto::{ProtoBufParser, ProtoFamily, ProtoFamilyView, ProtoMetricView};
pub use crate::parse_text::{StageArea, TextParser};
pub use crate::parser::Parser;

/// Counters describing what the parser saw and dropped. Error information for
/// skipped input lives here because no `Mode` yields `Err` items.
///
/// Line-grain fields belong to the text parser alone — the protobuf
/// format has no lines; [`Stats::metrics_total`] is the unified account
/// of what both formats parsed successfully.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Stats {
    /// Text format only: input lines processed — every line folded,
    /// rejected ones included.
    pub lines_total: u64,
    /// Metrics parsed successfully in both formats: model Metric rows
    /// created by the text parser, protobuf Metric entries carrying the
    /// value field their family type names. An aggregation family a
    /// chunk boundary leaves still gathering its sample group rides to
    /// the next call, so its rows are created once.
    pub metrics_total: u64,
    /// Text format only: input lines the mode rejected. Every rejected
    /// line counts, whether or not its family was dropped.
    pub skipped_lines: u64,
    /// Protobuf format only: Metric entries skipped — a Metric without
    /// the value field its family type names.
    pub skipped_metrics: u64,
    /// Family-level skips: whole dropped family messages (protobuf: an
    /// unknown type or a wire-level failure). The text format drops
    /// single lines, not families, so lenient text parsing leaves this at
    /// zero. Go implementation instead aborts at the first error and
    /// keeps the incomplete family in its partial map, so this counter
    /// has no Go counterpart.
    pub skipped_families: u64,
    pub pruned_families: u64,
    /// Families the facade's [`Parser::reset`] fold completed without
    /// yielding: a chunked feed's tail completes only half of its family,
    /// so the facade folds it for the stats and drops the rows — drive
    /// [`TextParser`] directly when the tail rows matter. So does an
    /// aggregation family the stream ends on while it still gathers its
    /// sample group (a histogram without its `_sum`, say): rows never
    /// yield half a sample group.
    pub discarded_families: u64,
    pub last_error: Option<Error>,
}

/// Zero-copy read interface over one parsed metric family. Both
/// exposition formats yield it: text parsing yields the data model
/// ([`MetricFamily`]) directly, protobuf parsing yields its wire-backed
/// [`ProtoFamily`], and one generic consumer drives either through the
/// same methods.
///
/// The projections borrow `&self` while their strings borrow the input
/// (`'a`), so neither carries an outlives bound: the borrow checker
/// tracks the `&self` loan through the captured lifetime.
pub trait MetricFamilyView<'a> {
    /// The metric view this family hands out, borrowing `&self`: generic
    /// so the strings inside can borrow the input (`'a`) while the view
    /// itself borrows the family (`'m`).
    type Metric<'m>: MetricView<'a>
    where
        Self: 'm;

    fn name(&self) -> Cow<'a, str>;
    fn help(&self) -> Option<Cow<'a, str>>;
    fn metric_type(&self) -> MetricType;
    /// The number of metrics this family carries; entries the protobuf
    /// parser skipped (no value field matching the family type) do not
    /// count.
    fn metric_count(&self) -> usize;
    /// The metrics in input order; the projection allocates nothing.
    fn metrics<'m>(&'m self) -> impl Iterator<Item = Self::Metric<'m>>;
}

/// Zero-copy read interface over one metric row. [`MetricView::value`]
/// returns the value by value: sample rows are plain copies, aggregation
/// rows (summary/histogram) materialize their quantile/bucket lists.
pub trait MetricView<'a> {
    fn labels(&self) -> impl Iterator<Item = LabelView<'a>>;
    fn timestamp(&self) -> Option<i64>;
    fn value(&self) -> MetricValueRef<'_>;
}

/// One label pair as a read view. Both formats' fast paths borrow the
/// input; escaped text tokens own their decoded bytes.
#[derive(Debug, Clone, PartialEq)]
pub struct LabelView<'a> {
    pub name: Cow<'a, str>,
    pub value: Cow<'a, str>,
}

/// A reference to any metric view is itself one, so owned rows project
/// through the interface without copying.
impl<'a, T> MetricView<'a> for &T
where
    T: MetricView<'a> + ?Sized,
{
    fn labels(&self) -> impl Iterator<Item = LabelView<'a>> {
        (**self).labels()
    }

    fn timestamp(&self) -> Option<i64> {
        (**self).timestamp()
    }

    fn value(&self) -> MetricValueRef<'_> {
        (**self).value()
    }
}
