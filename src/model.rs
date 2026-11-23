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

use std::borrow::Cow;

/// Classic text format (0.0.4) metric types. A family with no TYPE line is
/// UNTYPED.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricType {
    Counter,
    Gauge,
    Summary,
    Untyped,
    Histogram,
    GaugeHistogram,
}

/// The value of one metric row. Counter/gauge/untyped rows carry a plain
/// sample; summary and histogram rows aggregate every input line that
/// resolved to them.
#[derive(Debug, Clone)]
pub enum MetricValue {
    Sample(f64),
    Summary(SummaryValue),
    Histogram(HistogramValue),
}

/// Aggregated summary, mirroring dto.Summary. Sample counts never occur as
/// floats in the classic text format, so negative input counts wrap like
/// Go's `uint64(value)` conversion instead of switching fields.
#[derive(Debug, Clone, Default)]
pub struct SummaryValue {
    pub sample_count: Option<u64>,
    pub sample_sum: Option<f64>,
    pub quantiles: Vec<Quantile>,
}

/// Aggregated histogram, mirroring dto.Histogram. Any fractional or negative
/// count switches the whole histogram to float representation, like Go's
/// `normalizeHistogram`.
#[derive(Debug, Clone, Default)]
pub struct HistogramValue {
    pub sample_count: Option<IntOrFloat>,
    pub sample_sum: Option<f64>,
    pub buckets: Vec<Bucket>,
}

#[derive(Debug, Clone)]
pub struct Quantile {
    pub quantile: f64,
    pub value: f64,
}

#[derive(Debug, Clone)]
pub struct Bucket {
    pub upper_bound: f64,
    pub cumulative_count: IntOrFloat,
}

/// Integral or float counter, mirroring the dto int/float field pair.
#[derive(Debug, Clone, Copy)]
pub enum IntOrFloat {
    Int(u64),
    Float(f64),
}

/// A view of one metric family. Text fields are `Cow<'a, str>`: tokens
/// without escape sequences borrow the input (zero-copy fast path), escaped
/// tokens own their decoded bytes. Families are yielded in creation order,
/// which is deterministic where Go's map output is not.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricFamily<'a> {
    pub name: Cow<'a, str>,
    pub help: Option<Cow<'a, str>>,
    pub metric_type: MetricType,
    pub metrics: Vec<Metric<'a>>,
}

/// One metric row folded into a family. Labels keep their input order;
/// duplicate rows (same name, same label set) are preserved.
#[derive(Debug, Clone)]
pub struct Metric<'a> {
    pub labels: Vec<Label<'a>>,
    pub timestamp: Option<i64>,
    pub value: MetricValue,
}

/// f64 equality treating NaN as equal to itself so parsed values can be
/// compared structurally.
fn feq(a: f64, b: f64) -> bool {
    a == b || (a.is_nan() && b.is_nan())
}

fn opt_feq(a: Option<f64>, b: Option<f64>) -> bool {
    match (a, b) {
        (Some(x), Some(y)) => feq(x, y),
        (None, None) => true,
        _ => false,
    }
}

impl PartialEq for IntOrFloat {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (IntOrFloat::Int(a), IntOrFloat::Int(b)) => a == b,
            (IntOrFloat::Float(a), IntOrFloat::Float(b)) => feq(*a, *b),
            _ => false,
        }
    }
}

impl PartialEq for Quantile {
    fn eq(&self, other: &Self) -> bool {
        feq(self.quantile, other.quantile) && feq(self.value, other.value)
    }
}

impl PartialEq for Bucket {
    fn eq(&self, other: &Self) -> bool {
        feq(self.upper_bound, other.upper_bound) && self.cumulative_count == other.cumulative_count
    }
}

impl PartialEq for SummaryValue {
    fn eq(&self, other: &Self) -> bool {
        self.sample_count == other.sample_count
            && opt_feq(self.sample_sum, other.sample_sum)
            && self.quantiles == other.quantiles
    }
}

impl PartialEq for HistogramValue {
    fn eq(&self, other: &Self) -> bool {
        self.sample_count == other.sample_count
            && opt_feq(self.sample_sum, other.sample_sum)
            && self.buckets == other.buckets
    }
}

impl PartialEq for MetricValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (MetricValue::Sample(a), MetricValue::Sample(b)) => feq(*a, *b),
            (MetricValue::Summary(a), MetricValue::Summary(b)) => a == b,
            (MetricValue::Histogram(a), MetricValue::Histogram(b)) => a == b,
            _ => false,
        }
    }
}

impl PartialEq for Metric<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.labels == other.labels
            && self.timestamp == other.timestamp
            && self.value == other.value
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Label<'a> {
    pub name: Cow<'a, str>,
    pub value: Cow<'a, str>,
}

impl<'a> crate::MetricFamilyView<'a> for MetricFamily<'a> {
    type Metric<'m>
        = &'m Metric<'a>
    where
        Self: 'm;

    fn name(&self) -> Cow<'a, str> {
        self.name.clone()
    }

    fn help(&self) -> Option<Cow<'a, str>> {
        self.help.clone()
    }

    fn metric_type(&self) -> MetricType {
        self.metric_type
    }

    fn metric_count(&self) -> usize {
        self.metrics.len()
    }

    fn metrics<'m>(&'m self) -> impl Iterator<Item = Self::Metric<'m>> {
        self.metrics.iter()
    }
}

impl<'a> crate::MetricView<'a> for Metric<'a> {
    fn labels(&self) -> impl Iterator<Item = crate::LabelView<'a>> {
        self.labels.iter().map(|label| crate::LabelView {
            name: label.name.clone(),
            value: label.value.clone(),
        })
    }

    fn timestamp(&self) -> Option<i64> {
        self.timestamp
    }

    fn value(&self) -> MetricValue {
        self.value.clone()
    }
}
