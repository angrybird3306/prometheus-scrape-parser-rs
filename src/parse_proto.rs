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

//! Parser for the protobuf exposition format: a streaming handle over
//! varint-delimited `io.prometheus.client.MetricFamily` messages.

use std::borrow::Cow;

use crate::proto::io::prometheus::client as dto;
use crate::proto::io::prometheus::client::MetricType as ProtoType;
use buffa::MessageView;

use crate::Stats;
use crate::error::{Error, ErrorKind};
use crate::model::{
    Bucket, HistogramValue, IntOrFloat, Label, Metric, MetricFamily, MetricType, MetricValue,
    Quantile, SummaryValue,
};
use crate::{LabelView, MetricFamilyView, MetricView};

pub struct ProtoBufParser {
    /// Bytes of a message a chunk boundary cut in half, waiting for the
    /// continuation; at most one message is ever staged.
    pending: Vec<u8>,
    /// 1-based ordinal of the last attempted message, stream-global.
    message_no: u64,
    stats: Stats,
}

impl Default for ProtoBufParser {
    fn default() -> Self {
        Self::new()
    }
}

impl ProtoBufParser {
    pub fn new() -> Self {
        Self {
            pending: Vec::new(),
            message_no: 0,
            stats: Stats::default(),
        }
    }

    /// Feeds one borrowed chunk of the stream and yields the families of
    /// every message it completes. Failures skip the offending message or
    /// metric, record into [`stats`](Self::stats), and the loop continues
    /// — `None` means this chunk is exhausted (possibly mid-message; see
    /// [`is_waiting`](Self::is_waiting)), not that an error occurred.
    pub fn parse<'a>(&mut self, buf: &'a [u8]) -> impl Iterator<Item = ProtoFamily<'a>> {
        ProtoIter::new(self, buf)
    }

    /// [`parse`](Self::parse) with a stats sink that receives a snapshot
    /// each time the returned iteration ends (or is dropped), so a driver
    /// can surface the running totals without holding the parser's own
    /// stats.
    pub(crate) fn parse_into<'a, 'p>(
        &'p mut self,
        buf: &'a [u8],
        sink: &'p mut Stats,
    ) -> ProtoIter<'a, 'p> {
        ProtoIter::with_sink(self, buf, sink)
    }

    /// Whether a message staged by an earlier chunk is still waiting for
    /// its continuation bytes.
    pub fn is_waiting(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Discards the staged partial message and the accumulated stats; the
    /// next [`parse`](Self::parse) call starts a fresh stream. Staged
    /// buffer capacity is kept.
    pub fn reset(&mut self) {
        self.pending.clear();
        self.message_no = 0;
        self.stats = Stats::default();
    }

    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    /// Records one dropped message whose name never became known: a
    /// wire-level failure skips the whole family message.
    fn record_message_error(&mut self, got: &str) {
        self.message_no += 1;
        self.stats.skipped_families += 1;
        self.stats.last_error = Some(Error {
            line: self.message_no,
            kind: ErrorKind::InvalidProtoMessage {
                got: got.to_owned(),
            },
        });
    }

    /// Records a whole dropped message: the family's TYPE carried an
    /// unknown enum value.
    fn record_unknown_type(stats: &mut Stats, got: i32, line: u64) {
        stats.skipped_families += 1;
        stats.last_error = Some(Error {
            line,
            kind: ErrorKind::UnknownProtoMetricType { got },
        });
    }
}

pub(crate) struct ProtoIter<'a, 'p> {
    parser: &'p mut ProtoBufParser,
    buf: &'a [u8],
    pos: usize,
    done: bool,
    /// Receives a stats snapshot whenever this round of iteration ends, so
    /// the facade can surface the running totals of the format it last
    /// drove.
    sink: Option<&'p mut Stats>,
}

impl<'a, 'p> ProtoIter<'a, 'p> {
    pub(crate) fn new(parser: &'p mut ProtoBufParser, buf: &'a [u8]) -> Self {
        Self {
            parser,
            buf,
            pos: 0,
            done: false,
            sink: None,
        }
    }

    pub(crate) fn with_sink(
        parser: &'p mut ProtoBufParser,
        buf: &'a [u8],
        sink: &'p mut Stats,
    ) -> Self {
        Self {
            parser,
            buf,
            pos: 0,
            done: false,
            sink: Some(sink),
        }
    }

    fn sync_sink(&mut self) {
        if let Some(sink) = &mut self.sink {
            **sink = self.parser.stats.clone();
        }
    }

    /// Decodes the staged message (now complete inside `pending`, whose
    /// first `body_start` bytes are the length prefix) and clears the
    /// staging. Its strings materialize as owned: the view borrows the
    /// staging, which cannot escape the parser.
    fn take_staged(&mut self, body_start: usize) -> Option<ProtoFamily<'a>> {
        // Disjoint field borrows: the view holds `pending` while the
        // stats bookkeeping mutates `stats`.
        let ProtoBufParser {
            pending,
            message_no,
            stats,
        } = &mut self.parser;
        *message_no += 1;
        let line = *message_no;
        let view = match dto::MetricFamilyView::decode_view(&pending[body_start..]) {
            Ok(view) => view,
            Err(e) => {
                pending.clear();
                stats.skipped_families += 1;
                stats.last_error = Some(Error {
                    line,
                    kind: ErrorKind::InvalidProtoMessage { got: e.to_string() },
                });
                return None;
            }
        };
        let Some(proto_type) = view.r#type.as_known() else {
            ProtoBufParser::record_unknown_type(stats, view.r#type.to_i32(), line);
            pending.clear();
            return None;
        };
        let Some(wire) = ProtoFamilyView::build(view, proto_type, stats) else {
            pending.clear();
            return None;
        };
        let family = wire.to_owned_family();
        pending.clear();
        Some(ProtoFamily::Owned(family))
    }
}

impl<'a> Iterator for ProtoIter<'a, '_> {
    type Item = ProtoFamily<'a>;

    fn next(&mut self) -> Option<ProtoFamily<'a>> {
        let buf = self.buf;
        loop {
            if self.done {
                return None;
            }

            if !self.parser.pending.is_empty() {
                match read_length_prefix(&self.parser.pending) {
                    Prefix::Malformed => {
                        self.parser.pending.clear();
                        self.parser.record_message_error("malformed length prefix");
                        self.done = true;
                        self.sync_sink();
                        return None;
                    }
                    Prefix::Incomplete => {
                        // The staged prefix itself is unfinished; pull one
                        // byte at a time until it decodes (at most ten).
                        if self.pos < buf.len() {
                            let byte = buf[self.pos];
                            self.pos += 1;
                            self.parser.pending.push(byte);
                            continue;
                        }
                        return None;
                    }
                    Prefix::Len(len, used) => {
                        let have = (self.parser.pending.len() - used) as u64;
                        if have < len {
                            let need = (len - have).min((buf.len() - self.pos) as u64) as usize;
                            self.parser
                                .pending
                                .extend_from_slice(&buf[self.pos..self.pos + need]);
                            self.pos += need;
                            if have + (need as u64) < len {
                                return None;
                            }
                            continue;
                        }
                        if let Some(family) = self.take_staged(used) {
                            return Some(family);
                        }
                        continue;
                    }
                }
            }

            if self.pos == buf.len() {
                self.done = true;
                self.sync_sink();
                return None;
            }
            match read_length_prefix(&buf[self.pos..]) {
                Prefix::Malformed => {
                    self.pos = buf.len();
                    self.parser.record_message_error("malformed length prefix");
                    self.done = true;
                    self.sync_sink();
                    return None;
                }
                Prefix::Incomplete => {
                    self.parser.pending.extend_from_slice(&buf[self.pos..]);
                    self.pos = buf.len();
                    return None;
                }
                Prefix::Len(len, prefix_len) => {
                    let body = self.pos + prefix_len;
                    if len as u128 > (buf.len() - body) as u128 {
                        self.parser.pending.extend_from_slice(&buf[self.pos..]);
                        self.pos = buf.len();
                        return None;
                    }
                    let end = body + len as usize;
                    self.parser.message_no += 1;
                    let line = self.parser.message_no;
                    let view = match dto::MetricFamilyView::decode_view(&buf[body..end]) {
                        Ok(view) => view,
                        Err(e) => {
                            self.pos = end;
                            self.parser.stats.skipped_families += 1;
                            self.parser.stats.last_error = Some(Error {
                                line,
                                kind: ErrorKind::InvalidProtoMessage { got: e.to_string() },
                            });
                            continue;
                        }
                    };
                    self.pos = end;
                    let Some(proto_type) = view.r#type.as_known() else {
                        ProtoBufParser::record_unknown_type(
                            &mut self.parser.stats,
                            view.r#type.to_i32(),
                            line,
                        );
                        continue;
                    };
                    match ProtoFamilyView::build(view, proto_type, &mut self.parser.stats) {
                        Some(wire) => return Some(ProtoFamily::Wire(wire)),
                        None => continue,
                    }
                }
            }
        }
    }
}

impl Drop for ProtoIter<'_, '_> {
    fn drop(&mut self) {
        self.sync_sink();
    }
}

/// One family the protobuf parser yields. A message that completed inside
/// the fed chunk projects its borrowed dto views
/// ([`ProtoFamily::Wire`], zero-copy); a message completed across a chunk
/// boundary carries owned strings, because its view borrowed the parser's
/// staging ([`ProtoFamily::Owned`]). Both read through
/// [`MetricFamilyView`].
#[derive(Clone, Debug)]
pub enum ProtoFamily<'a> {
    Wire(ProtoFamilyView<'a>),
    Owned(MetricFamily<'static>),
}

impl<'a> MetricFamilyView<'a> for ProtoFamily<'a> {
    type Metric<'m>
        = ProtoFamilyMetric<'a, 'm>
    where
        'a: 'm;

    fn name(&self) -> Cow<'a, str> {
        match self {
            Self::Wire(view) => view.name(),
            Self::Owned(family) => family.name(),
        }
    }

    fn help(&self) -> Option<Cow<'a, str>> {
        match self {
            Self::Wire(view) => view.help(),
            Self::Owned(family) => family.help(),
        }
    }

    fn metric_type(&self) -> MetricType {
        match self {
            Self::Wire(view) => view.metric_type(),
            Self::Owned(family) => family.metric_type(),
        }
    }

    fn metric_count(&self) -> usize {
        match self {
            Self::Wire(view) => view.metric_count(),
            Self::Owned(family) => family.metrics.len(),
        }
    }

    fn metrics<'m>(&'m self) -> impl Iterator<Item = Self::Metric<'m>> {
        match self {
            Self::Wire(view) => ProtoMetricsIter::Wire(WireMetrics {
                view: &view.view,
                indices: view.valid.iter(),
                proto_type: view.proto_type,
            }),
            Self::Owned(family) => ProtoMetricsIter::Owned(family.metrics.iter()),
        }
    }
}

/// Iterates a [`ProtoFamily`]'s metrics: wire-backed projections for the
/// zero-copy variant, model row references for the owned one.
enum ProtoMetricsIter<'a, 'm> {
    Wire(WireMetrics<'a, 'm>),
    Owned(std::slice::Iter<'m, Metric<'static>>),
}

impl<'a, 'm> Iterator for ProtoMetricsIter<'a, 'm> {
    type Item = ProtoFamilyMetric<'a, 'm>;

    fn next(&mut self) -> Option<ProtoFamilyMetric<'a, 'm>> {
        match self {
            Self::Wire(iter) => iter.next().map(ProtoFamilyMetric::Wire),
            Self::Owned(iter) => iter.next().map(ProtoFamilyMetric::Owned),
        }
    }
}

/// One metric row of a [`ProtoFamily`], reading through [`MetricView`].
#[derive(Clone, Debug)]
pub enum ProtoFamilyMetric<'a, 'm> {
    Wire(ProtoMetricView<'a, 'm>),
    Owned(&'m Metric<'static>),
}

impl<'a> MetricView<'a> for ProtoFamilyMetric<'a, '_> {
    fn labels(&self) -> impl Iterator<Item = LabelView<'a>> {
        match self {
            Self::Wire(metric) => ProtoLabels::Wire(metric.metric.label.iter()),
            Self::Owned(metric) => ProtoLabels::Owned(metric.labels.iter()),
        }
    }

    fn timestamp(&self) -> Option<i64> {
        match self {
            Self::Wire(metric) => metric.timestamp(),
            Self::Owned(metric) => metric.timestamp,
        }
    }

    fn value(&self) -> MetricValue {
        match self {
            Self::Wire(metric) => metric.value(),
            Self::Owned(metric) => metric.value.clone(),
        }
    }
}

/// Iterates a [`ProtoFamilyMetric`]'s labels into [`LabelView`]s.
enum ProtoLabels<'a, 'm> {
    Wire(std::slice::Iter<'m, dto::LabelPairView<'a>>),
    Owned(std::slice::Iter<'m, Label<'static>>),
}

impl<'a, 'm> Iterator for ProtoLabels<'a, 'm> {
    type Item = LabelView<'a>;

    fn next(&mut self) -> Option<LabelView<'a>> {
        match self {
            Self::Wire(iter) => iter.next().map(|label| LabelView {
                name: Cow::Borrowed(label.name),
                value: Cow::Borrowed(label.value),
            }),
            Self::Owned(iter) => iter.next().map(|label| LabelView {
                name: label.name.clone(),
                value: label.value.clone(),
            }),
        }
    }
}

/// The zero-copy view over one family message: the borrowed dto view plus
/// the indices of the Metric entries convertible under the family's type.
#[derive(Clone, Debug)]
pub struct ProtoFamilyView<'a> {
    view: dto::MetricFamilyView<'a>,
    proto_type: ProtoType,
    valid: Vec<usize>,
}

impl<'a> ProtoFamilyView<'a> {
    /// Projects a decoded dto view and settles the stats: every Metric
    /// entry counts one line, entries without the value field their
    /// family type names are skipped, and a family left without any
    /// convertible metric is pruned (`None`).
    fn build(
        view: dto::MetricFamilyView<'a>,
        proto_type: ProtoType,
        stats: &mut Stats,
    ) -> Option<Self> {
        let mut valid = Vec::with_capacity(view.metric.len());
        for (i, metric) in view.metric.iter().enumerate() {
            if has_value(metric, proto_type) {
                stats.metrics_total += 1;
                valid.push(i);
            } else {
                stats.skipped_metrics += 1;
                stats.last_error = Some(Error {
                    line: stats.metrics_total + stats.skipped_metrics,
                    kind: ErrorKind::MissingMetricValue {
                        family: view.name.to_owned(),
                    },
                });
            }
        }
        if valid.is_empty() && !view.metric.is_empty() {
            stats.pruned_families += 1;
            return None;
        }
        Some(Self {
            view,
            proto_type,
            valid,
        })
    }

    /// Materializes the view into the owned data model: its strings
    /// borrowed the parser's staging, which cannot escape the parser as a
    /// view, so a boundary-completed family copies instead.
    fn to_owned_family(&self) -> MetricFamily<'static> {
        MetricFamily {
            name: Cow::Owned(self.name().into_owned()),
            help: self.help().map(|help| Cow::Owned(help.into_owned())),
            metric_type: self.metric_type(),
            metrics: self
                .metrics()
                .map(|metric| Metric {
                    labels: metric
                        .labels()
                        .map(|label| Label {
                            name: Cow::Owned(label.name.into_owned()),
                            value: Cow::Owned(label.value.into_owned()),
                        })
                        .collect(),
                    timestamp: metric.timestamp(),
                    value: metric.value(),
                })
                .collect(),
        }
    }
}

impl<'a> MetricFamilyView<'a> for ProtoFamilyView<'a> {
    type Metric<'m>
        = ProtoMetricView<'a, 'm>
    where
        Self: 'm;

    fn name(&self) -> Cow<'a, str> {
        Cow::Borrowed(self.view.name)
    }

    fn help(&self) -> Option<Cow<'a, str>> {
        (!self.view.help.is_empty()).then_some(Cow::Borrowed(self.view.help))
    }

    fn metric_type(&self) -> MetricType {
        metric_type_of(self.proto_type)
    }

    fn metric_count(&self) -> usize {
        self.valid.len()
    }

    fn metrics<'m>(&'m self) -> impl Iterator<Item = Self::Metric<'m>> {
        WireMetrics {
            view: &self.view,
            indices: self.valid.iter(),
            proto_type: self.proto_type,
        }
    }
}

/// The wire-backed view over one dto Metric, projected through its
/// family's type: value-field selection happens at read time. Only
/// construction ([`ProtoFamilyView::metrics`]) hands these out, and the
/// construction-time scan validated the value field the type names.
#[derive(Clone, Debug)]
pub struct ProtoMetricView<'a, 'm> {
    metric: &'m dto::MetricView<'a>,
    proto_type: ProtoType,
}

impl<'a> MetricView<'a> for ProtoMetricView<'a, '_> {
    fn labels(&self) -> impl Iterator<Item = LabelView<'a>> {
        WireLabels {
            iter: self.metric.label.iter(),
        }
    }

    fn timestamp(&self) -> Option<i64> {
        (self.metric.timestamp_ms != 0).then_some(self.metric.timestamp_ms)
    }

    fn value(&self) -> MetricValue {
        metric_value_of(self.metric, self.proto_type).expect("value field validated at build")
    }
}

/// Iterates a [`ProtoFamilyView`]'s convertible dto metrics by index.
struct WireMetrics<'a, 'm> {
    view: &'m dto::MetricFamilyView<'a>,
    indices: std::slice::Iter<'m, usize>,
    proto_type: ProtoType,
}

impl<'a, 'm> Iterator for WireMetrics<'a, 'm> {
    type Item = ProtoMetricView<'a, 'm>;

    fn next(&mut self) -> Option<ProtoMetricView<'a, 'm>> {
        let &i = self.indices.next()?;
        Some(ProtoMetricView {
            metric: &self.view.metric[i],
            proto_type: self.proto_type,
        })
    }
}

/// Iterates a wire metric's labels, borrowed from the chunk.
struct WireLabels<'a, 'm> {
    iter: std::slice::Iter<'m, dto::LabelPairView<'a>>,
}

impl<'a, 'm> Iterator for WireLabels<'a, 'm> {
    type Item = LabelView<'a>;

    fn next(&mut self) -> Option<LabelView<'a>> {
        let label = self.iter.next()?;
        Some(LabelView {
            name: Cow::Borrowed(label.name),
            value: Cow::Borrowed(label.value),
        })
    }
}

/// Whether the metric carries the value field its family type names — the
/// construction-time validity scan; value extraction itself is lazy.
fn has_value(metric: &dto::MetricView<'_>, proto_type: ProtoType) -> bool {
    match proto_type {
        ProtoType::COUNTER => metric.counter.as_option().is_some(),
        ProtoType::GAUGE => metric.gauge.as_option().is_some(),
        ProtoType::UNTYPED => metric.untyped.as_option().is_some(),
        ProtoType::SUMMARY => metric.summary.as_option().is_some(),
        ProtoType::HISTOGRAM | ProtoType::GAUGE_HISTOGRAM => metric.histogram.as_option().is_some(),
    }
}

/// The dto value field the family type names, with the dto's int/float
/// twin rule for histogram counts; `None` = the metric carries no value
/// field matching its family type.
fn metric_value_of(metric: &dto::MetricView<'_>, proto_type: ProtoType) -> Option<MetricValue> {
    match proto_type {
        ProtoType::COUNTER => metric
            .counter
            .as_option()
            .map(|v| MetricValue::Sample(v.value)),
        ProtoType::GAUGE => metric
            .gauge
            .as_option()
            .map(|v| MetricValue::Sample(v.value)),
        ProtoType::UNTYPED => metric
            .untyped
            .as_option()
            .map(|v| MetricValue::Sample(v.value)),
        ProtoType::SUMMARY => metric.summary.as_option().map(|s| {
            MetricValue::Summary(SummaryValue {
                sample_count: Some(s.sample_count),
                sample_sum: Some(s.sample_sum),
                quantiles: s
                    .quantile
                    .iter()
                    .map(|q| Quantile {
                        quantile: q.quantile,
                        value: q.value,
                    })
                    .collect(),
            })
        }),
        ProtoType::HISTOGRAM | ProtoType::GAUGE_HISTOGRAM => {
            metric.histogram.as_option().map(|h| {
                MetricValue::Histogram(HistogramValue {
                    sample_count: Some(count(h.sample_count, h.sample_count_float)),
                    sample_sum: Some(h.sample_sum),
                    buckets: h
                        .bucket
                        .iter()
                        .map(|b| Bucket {
                            upper_bound: b.upper_bound,
                            cumulative_count: count(b.cumulative_count, b.cumulative_count_float),
                        })
                        .collect(),
                })
            })
        }
    }
}

/// Maps a known dto type onto the model type; the classic dto has no
/// gauge-histogram message, and its own field doc directs GAUGE_HISTOGRAM
/// families at the histogram field.
fn metric_type_of(proto_type: ProtoType) -> MetricType {
    match proto_type {
        ProtoType::COUNTER => MetricType::Counter,
        ProtoType::GAUGE => MetricType::Gauge,
        ProtoType::SUMMARY => MetricType::Summary,
        ProtoType::UNTYPED => MetricType::Untyped,
        ProtoType::HISTOGRAM => MetricType::Histogram,
        ProtoType::GAUGE_HISTOGRAM => MetricType::GaugeHistogram,
    }
}

/// The dto int/float count pair: a positive float twin overrides the int
/// field (the dto's own field docs), mirroring Go's normalizeHistogram.
fn count(int_value: u64, float_value: f64) -> IntOrFloat {
    if float_value > 0.0 {
        IntOrFloat::Float(float_value)
    } else {
        IntOrFloat::Int(int_value)
    }
}

/// One uvarint length prefix.
enum Prefix {
    /// `(length, prefix bytes)`.
    Len(u64, usize),
    /// The input ends with continuation bits still set: the prefix itself
    /// is incomplete, not malformed.
    Incomplete,
    /// The prefix overruns the 10-byte varint bound: the stream is
    /// garbage from here on.
    Malformed,
}

fn read_length_prefix(input: &[u8]) -> Prefix {
    let mut length = 0u64;
    let mut shift = 0;
    for (i, &byte) in input.iter().enumerate() {
        length |= u64::from(byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            return Prefix::Len(length, i + 1);
        }
        shift += 7;
        if shift >= 64 {
            return Prefix::Malformed;
        }
    }
    Prefix::Incomplete
}
