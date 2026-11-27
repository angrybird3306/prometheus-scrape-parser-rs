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
use std::collections::{HashMap, HashSet};

use crate::error::ErrorKind;
use crate::lexer::{SampleLine, Token, parse_float_token};
use crate::model::{
    Bucket, HistogramValue, IntOrFloat, Label, Metric, MetricFamily, MetricType, MetricValue,
    Quantile, SummaryValue,
};

/// Mirrors Go's setOrCreateCurrentMF name resolution:
/// exact name first, then the summary/histogram parent family when its type
/// matches, else the name itself. `lookup` reports entry existence with the
/// possibly-absent family type.
pub(crate) fn resolve_family_name(
    raw: &str,
    mut lookup: impl FnMut(&str) -> Option<Option<MetricType>>,
) -> &str {
    if lookup(raw).is_some() {
        return raw;
    }
    if let Some(base) = summary_parent(raw)
        && lookup(base) == Some(Some(MetricType::Summary))
    {
        return base;
    }
    if let Some(base) = histogram_parent(raw)
        && matches!(
            lookup(base),
            Some(Some(MetricType::Histogram)) | Some(Some(MetricType::GaugeHistogram))
        )
    {
        return base;
    }
    raw
}

fn is_count(name: &str) -> bool {
    name.len() > 6 && name.ends_with("_count")
}

fn is_sum(name: &str) -> bool {
    name.len() > 4 && name.ends_with("_sum")
}

fn is_bucket(name: &str) -> bool {
    name.len() > 7 && name.ends_with("_bucket")
}

/// Go sets the summary/histogram count/sum flags only through the suffix
/// resolution path: the name is the family name plus the suffix. An exact
/// family-name hit leaves the flags false, so the bare rows of a
/// suffix-named family fold by their quantile/le label instead.
fn is_family_count(family: &str, name: &str) -> bool {
    is_count(name) && &name[..name.len() - 6] == family
}

fn is_family_sum(family: &str, name: &str) -> bool {
    is_sum(name) && &name[..name.len() - 4] == family
}

fn summary_parent(name: &str) -> Option<&str> {
    if is_count(name) {
        Some(&name[..name.len() - 6])
    } else if is_sum(name) {
        Some(&name[..name.len() - 4])
    } else {
        None
    }
}

fn histogram_parent(name: &str) -> Option<&str> {
    if is_count(name) {
        Some(&name[..name.len() - 6])
    } else if is_sum(name) {
        Some(&name[..name.len() - 4])
    } else if is_bucket(name) {
        Some(&name[..name.len() - 7])
    } else {
        None
    }
}

#[derive(Default)]
struct PendingMetricFamily<'a> {
    name: Cow<'a, str>,
    help: Option<Cow<'a, str>>,
    metric_type: Option<MetricType>,
    metrics: Vec<Metric<'a>>,
}

impl<'a> From<PendingMetricFamily<'a>> for MetricFamily<'a> {
    fn from(mut val: PendingMetricFamily<'a>) -> Self {
        for m in &mut val.metrics {
            if let MetricValue::Histogram(h) = &mut m.value {
                normalize_histogram(h);
            }
        }
        MetricFamily {
            name: val.name,
            help: val.help,
            metric_type: val.metric_type.unwrap_or(MetricType::Untyped),
            metrics: val.metrics,
        }
    }
}

/// Mirrors Go's normalizeHistogram: one float count switches every counter
/// in the histogram to float representation.
fn normalize_histogram(h: &mut HistogramValue) {
    let any_float = matches!(h.sample_count, Some(IntOrFloat::Float(_)))
        || h.buckets
            .iter()
            .any(|b| matches!(b.cumulative_count, IntOrFloat::Float(_)));
    if !any_float {
        return;
    }
    h.sample_count = h.sample_count.map(to_float);
    for b in &mut h.buckets {
        b.cumulative_count = to_float(b.cumulative_count);
    }
}

fn to_float(c: IntOrFloat) -> IntOrFloat {
    match c {
        IntOrFloat::Int(v) => IntOrFloat::Float(v as f64),
        f => f,
    }
}

/// Mirrors Go's `uint64(value)` summary count conversion: truncates
/// in-range values, wraps negatives like the amd64 float-to-int conversion.
fn count_as_u64(value: f64) -> u64 {
    if value >= 0.0 && value <= u64::MAX as f64 {
        value as u64
    } else {
        (value as i64) as u64
    }
}

/// Histogram count routing: exact non-negative integers
/// stay integral, everything else goes float, negatives are rejected.
fn histogram_count(value: f64) -> Result<IntOrFloat, ()> {
    if value < 0.0 {
        return Err(());
    }
    let uv = value as u64;
    if uv as f64 == value {
        Ok(IntOrFloat::Int(uv))
    } else {
        Ok(IntOrFloat::Float(value))
    }
}

/// Whether a stripped label set names the same aggregation row as an
/// existing metric's labels: equal sizes and equal canonical (sorted) pair
/// streams, input order insignificant. Allocation-free; the linear scan
/// mirrors Go's summary/histogram row merge.
fn same_label_set(stored: &[Label<'_>], probe: &[Label<'_>]) -> bool {
    stored.len() == probe.len()
        && (0..stored.len()).all(|rank| pair_at(stored, rank) == pair_at(probe, rank))
}

/// The lexicographically `rank`-th (name, value) pair, input order breaking
/// ties; the O(n²) scan leaves the slice unmodified so stored rows keep the
/// line's label order.
fn pair_at<'b>(labels: &'b [Label<'b>], rank: usize) -> (&'b str, &'b str) {
    ranked_pair_at(labels.len(), rank, |i| {
        let l = &labels[i];
        (l.name.as_ref(), l.value.as_ref())
    })
}

fn ranked_pair_at<'b>(
    len: usize,
    rank: usize,
    at: impl Fn(usize) -> (&'b str, &'b str),
) -> (&'b str, &'b str) {
    for i in 0..len {
        let pair = at(i);
        let mut r = 0;
        for j in 0..len {
            if j == i {
                continue;
            }
            let other = at(j);
            if other < pair || (other == pair && j < i) {
                r += 1;
            }
        }
        if r == rank {
            return pair;
        }
    }
    unreachable!("rank within 0..len");
}

fn duplicate_label_names(labels: &[Label<'_>]) -> bool {
    for (i, l) in labels.iter().enumerate() {
        if labels[..i].iter().any(|p| p.name == l.name) {
            return true;
        }
    }
    false
}

/// Partitions a sample's label set in place: regular labels keep their
/// order packed at the front, special `quantile`/`le` entries are parsed
/// into the bound (last one wins) and dropped. Returns the bound and the
/// packed regular prefix, so row creation moves the original Vec without
/// a second allocation.
fn strip_special_labels<'a, 'v>(
    labels: &'v mut Vec<Label<'a>>,
    special: &str,
) -> (Option<Result<f64, String>>, &'v [Label<'a>]) {
    let mut bound: Option<Result<f64, String>> = None;
    let mut write = 0;
    let mut read = 0;
    while read < labels.len() {
        if labels[read].name == special {
            match parse_float_token(&labels[read].value) {
                Ok(v) => bound = Some(Ok(v)),
                Err(()) => bound = Some(Err(labels[read].value.to_string())),
            }
        } else {
            labels.swap(write, read);
            write += 1;
        }
        read += 1;
    }
    labels.truncate(write);
    (bound, &labels[..write])
}

/// HELP/TYPE metadata currently recorded for a family name — the shadow of
/// what a whole-input family map would hold. A family that resumes in a
/// later instance seeds from here, so it folds and yields exactly as the
/// whole-input output has it; a lenient skip drops the entry with the
/// family, mirroring that map's removal.
#[derive(Default, Clone)]
struct KnownMeta<'a> {
    help: Option<Cow<'a, str>>,
    metric_type: Option<MetricType>,
}

/// The owned-layer value of the metadata memory: the same shape as
/// [`KnownMeta`] with fully owned text.
#[derive(Default, Clone)]
struct KnownOwnedMeta {
    help: Option<String>,
    metric_type: Option<MetricType>,
}

/// The metadata memory's owned layer (see [`KnownMeta`]): entries that
/// survived an instance boundary, keyed by owned family name. Transfers
/// between instances as a whole; only the entries each instance added
/// convert (see [`Assembler::release_memory`]).
#[derive(Default)]
pub struct KnownMemory(HashMap<String, KnownOwnedMeta>);

impl KnownMemory {
    pub(crate) fn clear(&mut self) {
        self.0.clear();
    }
}

/// Fully owned copy detached from any input lifetime: the zero-copy
/// parser's carried families must survive the instance that read them.
fn into_owned_static(text: &str) -> Cow<'static, str> {
    Cow::Owned(text.to_owned())
}

#[derive(Default)]
pub(crate) struct Assembler<'a> {
    pending_metric_families: Vec<PendingMetricFamily<'a>>,
    index: HashMap<Cow<'a, str>, usize>,
    /// Metadata shadow keyed by resolved family name: the borrowed layer
    /// holds this instance's entries, [`KnownMemory`] the ones that
    /// survived earlier instances.
    known: HashMap<Cow<'a, str>, KnownMeta<'a>>,
    known_owned: KnownMemory,
    /// Every resolved family name ever created, yielded families included:
    /// they keep shadowing the metadata memory's type for suffix
    /// resolution, exactly as a whole-input family map would.
    created: HashSet<Cow<'a, str>>,
    created_owned: HashSet<String>,
    /// Family names whose type is assigned stream-wide — by a TYPE line or
    /// a sample's implicit `Untyped` default (first wins) — so a TYPE line
    /// landing after the family's samples stays as ineffective as the
    /// whole-input fold's, even in a later instance where the slot is a
    /// fresh re-seed.
    typed: HashSet<Cow<'a, str>>,
    typed_owned: HashSet<String>,
    /// Indices of the families surviving pruning, in creation order; filled
    /// by [`Assembler::flush_ready`], consumed by [`Assembler::take_next`].
    ready_indices: Vec<u32>,
    /// Old slot -> new slot of the last compaction, reused across flushes.
    remap: Vec<Option<u32>>,
    cursor: usize,
    /// The slot the last folded line touched: the carry fallback when the
    /// staged cut line names no open aggregation family.
    last_slot: Option<usize>,
    /// Metric rows created since the last drain, folded into
    /// `Stats::metrics_total`; aggregation lines folding into an existing
    /// row count nothing.
    created_metrics: u64,
}

impl<'a> Assembler<'a> {
    fn lookup_type(&self, family_name: &str) -> Option<Option<MetricType>> {
        if let Some(&i) = self.index.get(family_name)
            && !self.pending_metric_families[i].name.as_ref().is_empty()
        {
            return Some(self.pending_metric_families[i].metric_type);
        }
        if let Some(m) = self.known_owned.0.get(family_name) {
            return Some(m.metric_type);
        }
        if let Some(m) = self.known.get(family_name) {
            return Some(m.metric_type);
        }
        // A family that already yielded: a whole-input fold still holds it, so
        // an untyped entry keeps shadowing a parent name's known type.
        if self.created.contains(family_name) || self.created_owned.contains(family_name) {
            return Some(None);
        }
        None
    }

    pub(crate) fn family_name<'r>(&self, raw: &'r str) -> &'r str {
        resolve_family_name(raw, |name| self.lookup_type(name))
    }

    fn family_idx(&mut self, resolved: Cow<'a, str>) -> usize {
        // A slot whose family was already yielded or dropped holds an
        // all-default shell (empty name); its stale key must not fold into
        // it — overwrite it with a fresh family seeded from the metadata
        // memory instead, so a family resuming in a later instance keeps
        // the HELP/TYPE the whole-input fold carries.
        if let Some(&i) = self.index.get(resolved.as_ref())
            && !self.pending_metric_families[i].name.as_ref().is_empty()
        {
            return i;
        }
        let seed = match self.known_owned.0.get(resolved.as_ref()) {
            Some(m) => Some(KnownMeta {
                help: m.help.clone().map(Cow::Owned),
                metric_type: m.metric_type,
            }),
            None => self.known.get(resolved.as_ref()).cloned(),
        };
        let i = self.pending_metric_families.len();
        self.pending_metric_families.push(PendingMetricFamily {
            name: resolved.clone(),
            help: seed.as_ref().and_then(|m| m.help.clone()),
            metric_type: seed.and_then(|m| m.metric_type),
            metrics: Vec::new(),
        });
        // Insert is idempotent: the shadow crosses instances while the
        // index is per-instance, so a family re-created in a later
        // instance is already shadowed.
        if !self.created_owned.contains(resolved.as_ref()) {
            self.created.insert(resolved.clone());
        }
        self.index.insert(resolved, i);
        i
    }

    /// Resolves the raw name and returns its family slot, creating the
    /// family if absent; fold paths hold the slot instead of the name so
    /// the borrowed name never crosses a sample move.
    fn resolve_idx(&mut self, name: &Token<'a>) -> usize {
        match &name.decoded {
            Cow::Borrowed(s) => {
                // `s` derefs to a true 'a input slice, so the resolved key
                // can stay borrowed; resolving on the Cow would tie it to
                // this call and force an owned key.
                let resolved = resolve_family_name(s, |n| self.lookup_type(n));
                self.family_idx(Cow::Borrowed(resolved))
            }
            Cow::Owned(_) => {
                let resolved = resolve_family_name(&name.decoded, |n| self.lookup_type(n));
                self.family_idx(Cow::Owned(resolved.to_string()))
            }
        }
    }

    /// Removes one family by final name without yielding it: the slot turns
    /// into a default shell and the name leaves the index and the metadata
    /// memory — a whole-input fold loses the family here too. Slots never
    /// shift outside [`Assembler::flush_ready`], so every key keeps pointing
    /// at its own family.
    pub(crate) fn drop_family(&mut self, resolved: &str) {
        if let Some(&i) = self.index.get(resolved) {
            self.pending_metric_families[i] = PendingMetricFamily::default();
            self.index.remove(resolved);
        }
        self.created.remove(resolved);
        self.created_owned.remove(resolved);
        self.typed.remove(resolved);
        self.typed_owned.remove(resolved);
        self.known.remove(resolved);
        self.known_owned.0.remove(resolved);
    }

    /// Folds a HELP line: first wins, on the instance and in the memory.
    pub(crate) fn fold_help(&mut self, name: Token<'a>, text: Token<'a>) {
        let i = self.resolve_idx(&name);
        self.last_slot = Some(i);
        if self.pending_metric_families[i].help.is_none() {
            let remembered = text.decoded.clone();
            self.pending_metric_families[i].help = Some(text.decoded);
            let key = self.pending_metric_families[i].name.clone();
            if let Some(meta) = self.known_owned.0.get_mut(key.as_ref()) {
                meta.help = Some(remembered.into_owned());
            } else {
                let meta = self.known.entry(key).or_default();
                meta.help = Some(remembered);
            }
        }
    }

    /// Folds a TYPE line: first wins, on the instance and in the memory.
    pub(crate) fn fold_type(&mut self, name: Token<'a>, metric_type: MetricType) {
        // The whole-input fold resists a TYPE line once anything assigned its
        // type — a previous TYPE line, or a sample's implicit Untyped
        // default. Check before `resolve_idx`, whose creation would put
        // the name into the assigned set itself.
        let assigned = {
            let resolved = self.family_name(&name.decoded);
            self.typed.contains(resolved) || self.typed_owned.contains(resolved)
        };
        let i = self.resolve_idx(&name);
        self.last_slot = Some(i);
        if !assigned {
            let slot_name = self.pending_metric_families[i].name.clone();
            if self.pending_metric_families[i].metric_type.is_none() {
                self.pending_metric_families[i].metric_type = Some(metric_type);
                if let Some(meta) = self.known_owned.0.get_mut(slot_name.as_ref()) {
                    meta.metric_type = Some(metric_type);
                } else {
                    let meta = self.known.entry(slot_name.clone()).or_default();
                    meta.metric_type = Some(metric_type);
                }
            }
            if !self.typed_owned.contains(slot_name.as_ref()) {
                self.typed.insert(slot_name);
            }
        }
    }

    /// Drains the created-metric count accumulated since the last call.
    pub(crate) fn take_created_metrics(&mut self) -> u64 {
        std::mem::take(&mut self.created_metrics)
    }

    pub(crate) fn fold_sample(
        &mut self,
        sample: &mut SampleLine<'a>,
        strict: bool,
    ) -> Result<(), ErrorKind> {
        let idx = self.resolve_idx(&sample.name);
        self.last_slot = Some(idx);
        let assigned_here = {
            let fam = &mut self.pending_metric_families[idx];
            if fam.metric_type.is_none() {
                // The implicit default assigns the type stream-wide: a
                // later TYPE line must not override it.
                fam.metric_type = Some(MetricType::Untyped);
                true
            } else {
                false
            }
        };
        if assigned_here {
            let slot_name = self.pending_metric_families[idx].name.clone();
            if !self.typed_owned.contains(slot_name.as_ref()) {
                self.typed.insert(slot_name);
            }
        }
        let metric_type = self.pending_metric_families[idx].metric_type.unwrap();
        match metric_type {
            MetricType::Summary => self.fold_summary(sample, idx, strict),
            MetricType::Histogram | MetricType::GaugeHistogram => {
                self.fold_histogram(sample, idx, strict)
            }
            _ => {
                if strict && duplicate_label_names(&sample.labels) {
                    Err(ErrorKind::DuplicateLabelNames {
                        family: self.pending_metric_families[idx].name.to_string(),
                    })
                } else {
                    self.fold_plain(sample, idx, strict)
                }
            }
        }
    }

    fn fold_plain(
        &mut self,
        sample: &mut SampleLine<'a>,
        idx: usize,
        strict: bool,
    ) -> Result<(), ErrorKind> {
        let timestamp = resolve_timestamp(sample.timestamp_raw.as_deref(), strict)?;
        self.created_metrics += 1;
        self.pending_metric_families[idx].metrics.push(Metric {
            labels: std::mem::take(&mut sample.labels),
            timestamp,
            value: MetricValue::Sample(sample.value),
        });
        Ok(())
    }

    fn fold_summary(
        &mut self,
        sample: &mut SampleLine<'a>,
        idx: usize,
        strict: bool,
    ) -> Result<(), ErrorKind> {
        let (bound, rest) = strip_special_labels(&mut sample.labels, "quantile");
        if strict && duplicate_label_names(rest) {
            return Err(ErrorKind::DuplicateLabelNames {
                family: self.pending_metric_families[idx].name.to_string(),
            });
        }
        let quantile = match bound {
            None => None,
            Some(Ok(q)) => Some(q),
            Some(Err(got)) => {
                if strict {
                    return Err(ErrorKind::ExpectedQuantileFloat { got });
                }
                // Lenient keeps the whole line as a plain sample, labels intact.
                let timestamp = resolve_timestamp(sample.timestamp_raw.as_deref(), strict)?;
                self.created_metrics += 1;
                self.pending_metric_families[idx].metrics.push(Metric {
                    labels: std::mem::take(&mut sample.labels),
                    timestamp,
                    value: MetricValue::Sample(sample.value),
                });
                return Ok(());
            }
        };
        // Only aggregation rows merge; lenient fallback rows are plain samples.
        let row_idx = {
            let fam = &mut self.pending_metric_families[idx];
            match fam.metrics.iter().position(|m| {
                matches!(m.value, MetricValue::Summary(_)) && same_label_set(&m.labels, rest)
            }) {
                Some(r) => r,
                None => {
                    self.created_metrics += 1;
                    fam.metrics.push(Metric {
                        labels: std::mem::take(&mut sample.labels),
                        timestamp: None,
                        value: MetricValue::Summary(SummaryValue::default()),
                    });
                    fam.metrics.len() - 1
                }
            }
        };
        let family_name = &self.pending_metric_families[idx].name;
        let count_row = is_family_count(family_name, &sample.name.decoded);
        let sum_row = is_family_sum(family_name, &sample.name.decoded);
        if count_row {
            if sample.value < 0.0 {
                return Err(ErrorKind::NegativeSummaryCount {
                    family: self.pending_metric_families[idx].name.to_string(),
                });
            }
            let MetricValue::Summary(summary) =
                &mut self.pending_metric_families[idx].metrics[row_idx].value
            else {
                unreachable!("row created as Summary");
            };
            summary.sample_count = Some(count_as_u64(sample.value));
        } else if sum_row {
            let MetricValue::Summary(summary) =
                &mut self.pending_metric_families[idx].metrics[row_idx].value
            else {
                unreachable!("row created as Summary");
            };
            summary.sample_sum = Some(sample.value);
        } else if let Some(q) = quantile
            && !q.is_nan()
        {
            let MetricValue::Summary(summary) =
                &mut self.pending_metric_families[idx].metrics[row_idx].value
            else {
                unreachable!("row created as Summary");
            };
            summary.quantiles.push(Quantile {
                quantile: q,
                value: sample.value,
            });
        }
        if let Some(ts) = resolve_timestamp(sample.timestamp_raw.as_deref(), strict)? {
            self.pending_metric_families[idx].metrics[row_idx].timestamp = Some(ts);
        }
        Ok(())
    }

    fn fold_histogram(
        &mut self,
        sample: &mut SampleLine<'a>,
        idx: usize,
        strict: bool,
    ) -> Result<(), ErrorKind> {
        let (bound, rest) = strip_special_labels(&mut sample.labels, "le");
        if strict && duplicate_label_names(rest) {
            return Err(ErrorKind::DuplicateLabelNames {
                family: self.pending_metric_families[idx].name.to_string(),
            });
        }
        let le = match bound {
            None => None,
            Some(Ok(l)) => Some(l),
            Some(Err(got)) => {
                if strict {
                    return Err(ErrorKind::ExpectedLeFloat { got });
                }
                let timestamp = resolve_timestamp(sample.timestamp_raw.as_deref(), strict)?;
                self.created_metrics += 1;
                self.pending_metric_families[idx].metrics.push(Metric {
                    labels: std::mem::take(&mut sample.labels),
                    timestamp,
                    value: MetricValue::Sample(sample.value),
                });
                return Ok(());
            }
        };
        // Only aggregation rows merge; lenient fallback rows are plain samples.
        let row_idx = {
            let fam = &mut self.pending_metric_families[idx];
            match fam.metrics.iter().position(|m| {
                matches!(m.value, MetricValue::Histogram(_)) && same_label_set(&m.labels, rest)
            }) {
                Some(r) => r,
                None => {
                    self.created_metrics += 1;
                    fam.metrics.push(Metric {
                        labels: std::mem::take(&mut sample.labels),
                        timestamp: None,
                        value: MetricValue::Histogram(HistogramValue::default()),
                    });
                    fam.metrics.len() - 1
                }
            }
        };
        let family_name = &self.pending_metric_families[idx].name;
        let count_row = is_family_count(family_name, &sample.name.decoded);
        let sum_row = is_family_sum(family_name, &sample.name.decoded);
        if count_row {
            let count =
                histogram_count(sample.value).map_err(|()| ErrorKind::NegativeHistogramCount {
                    family: self.pending_metric_families[idx].name.to_string(),
                })?;
            let MetricValue::Histogram(hist) =
                &mut self.pending_metric_families[idx].metrics[row_idx].value
            else {
                unreachable!("row created as Histogram");
            };
            hist.sample_count = Some(count);
        } else if sum_row {
            let MetricValue::Histogram(hist) =
                &mut self.pending_metric_families[idx].metrics[row_idx].value
            else {
                unreachable!("row created as Histogram");
            };
            hist.sample_sum = Some(sample.value);
        } else if let Some(lb) = le
            && !lb.is_nan()
        {
            let count =
                histogram_count(sample.value).map_err(|()| ErrorKind::NegativeHistogramBucket {
                    family: self.pending_metric_families[idx].name.to_string(),
                })?;
            let MetricValue::Histogram(hist) =
                &mut self.pending_metric_families[idx].metrics[row_idx].value
            else {
                unreachable!("row created as Histogram");
            };
            hist.buckets.push(Bucket {
                upper_bound: lb,
                cumulative_count: count,
            });
        }
        if let Some(ts) = resolve_timestamp(sample.timestamp_raw.as_deref(), strict)? {
            self.pending_metric_families[idx].metrics[row_idx].timestamp = Some(ts);
        }
        Ok(())
    }

    /// Decides which families yield and drops the rest (families without
    /// metrics are pruned, in Go's end-of-parse spirit); returns the pruned
    /// count. Yield order is creation order. The only place slots move:
    /// pruned families and shells compact away through the remap, and
    /// stale keys of yielded shells drop out here.
    pub(crate) fn flush_ready(&mut self) -> u64 {
        self.cursor = 0;
        self.last_slot = None;
        self.ready_indices.clear();
        self.ready_indices
            .reserve(self.pending_metric_families.len());
        self.remap.clear();
        let mut pruned = 0;
        let mut kept = 0usize;
        for pending in self.pending_metric_families.iter() {
            let shell = pending.name.as_ref().is_empty();
            if shell || pending.metrics.is_empty() {
                if !shell {
                    pruned += 1;
                }
                self.remap.push(None);
            } else {
                self.ready_indices.push(kept as u32);
                self.remap.push(Some(kept as u32));
                kept += 1;
            }
        }
        let remap = &self.remap;
        let mut old = 0;
        self.pending_metric_families.retain(|_| {
            let keep = remap[old].is_some();
            old += 1;
            keep
        });
        self.index.retain(|_, v| match remap[*v] {
            Some(slot) => {
                *v = slot as usize;
                true
            }
            None => false,
        });
        pruned
    }

    /// Pops the next surviving family, histogram-normalized and with its
    /// type defaulted. The slot becomes a shell that keeps its name keyed
    /// — dead to folds — until the next flush compacts it away.
    pub(crate) fn take_next(&mut self) -> Option<MetricFamily<'a>> {
        let index = *self.ready_indices.get(self.cursor)?;
        self.cursor += 1;
        Some(std::mem::take(&mut self.pending_metric_families[index as usize]).into())
    }

    /// The open slot of the family `resolved` names, when that family is
    /// aggregation-typed (summary/histogram): the zero-copy parser's carry
    /// target when the staged cut line names it.
    pub(crate) fn aggregation_slot_of(&self, resolved: &str) -> Option<usize> {
        let &i = self.index.get(resolved)?;
        if self.pending_metric_families[i].name.as_ref().is_empty() {
            return None;
        }
        matches!(
            self.pending_metric_families[i].metric_type,
            Some(MetricType::Summary | MetricType::Histogram | MetricType::GaugeHistogram)
        )
        .then_some(i)
    }

    /// Whether the slot still gathers a sample group: an open aggregation
    /// family with a row whose `_count`/`_sum` (histograms: their buckets
    /// too) have not all arrived. Plain fallback rows a lenient aggregation
    /// family holds are whole as they are.
    pub(crate) fn gathers_sample_group(&self, slot: usize) -> bool {
        let Some(family) = self.pending_metric_families.get(slot) else {
            return false;
        };
        if family.name.as_ref().is_empty() {
            return false;
        }
        match family.metric_type {
            Some(MetricType::Summary) => family.metrics.iter().any(|m| {
                matches!(
                    &m.value,
                    MetricValue::Summary(s)
                        if s.sample_count.is_none() || s.sample_sum.is_none()
                )
            }),
            Some(MetricType::Histogram | MetricType::GaugeHistogram) => {
                family.metrics.iter().any(|m| {
                    matches!(
                        &m.value,
                        MetricValue::Histogram(h)
                            if h.buckets.is_empty()
                                || h.sample_count.is_none()
                                || h.sample_sum.is_none()
                    )
                })
            }
            _ => false,
        }
    }

    pub(crate) fn last_slot(&self) -> Option<usize> {
        self.last_slot
    }

    /// Takes one slot's family as fully owned text when it is
    /// aggregation-typed — an aggregation row cut at an instance boundary
    /// must move whole, and only aggregation rows span sample lines.
    /// Anything else returns None and the slot flushes normally.
    pub(crate) fn take_aggregation_slot_owned(
        &mut self,
        slot: usize,
    ) -> Option<MetricFamily<'static>> {
        if !matches!(
            self.pending_metric_families[slot].metric_type,
            Some(MetricType::Summary | MetricType::Histogram | MetricType::GaugeHistogram)
        ) {
            return None;
        }

        // The slot's index key is removed so nothing folds into the shell;
        // the metadata memory is untouched (it transfers separately).
        let family = MetricFamily::from(std::mem::take(&mut self.pending_metric_families[slot]));
        if family.name.as_ref().is_empty() {
            return None;
        }
        self.index.remove(family.name.as_ref());
        Some(MetricFamily {
            name: into_owned_static(&family.name),
            help: family.help.as_deref().map(into_owned_static),
            metric_type: family.metric_type,
            metrics: family
                .metrics
                .into_iter()
                .map(|m| {
                    let labels = m
                        .labels
                        .into_iter()
                        .map(|l| Label {
                            name: into_owned_static(&l.name),
                            value: into_owned_static(&l.value),
                        })
                        .collect();
                    Metric {
                        labels,
                        timestamp: m.timestamp,
                        value: m.value,
                    }
                })
                .collect(),
        })
    }

    /// Seeds a carried family back in as an open slot: the zero-copy
    /// parser's next instance continues it with borrowed lines.
    pub(crate) fn restore_carried(&mut self, family: MetricFamily<'static>) {
        let name = family.name.clone();
        let i = self.pending_metric_families.len();
        self.pending_metric_families.push(PendingMetricFamily {
            name: family.name,
            help: family.help,
            metric_type: Some(family.metric_type),
            metrics: family.metrics,
        });
        self.index.insert(name, i);
    }

    /// Materializes the metadata memory at the instance boundary: the
    /// entries this instance added convert to owned — the only per-boundary
    /// cost — and the owned layers move out as a whole, so transfers stay
    /// linear in what each instance adds.
    pub(crate) fn release_memory(&mut self) -> (KnownMemory, HashSet<String>, HashSet<String>) {
        for (key, meta) in self.known.drain() {
            let KnownMeta { help, metric_type } = meta;
            let entry = self.known_owned.0.entry(key.into_owned()).or_default();
            if entry.help.is_none() {
                entry.help = help.map(Cow::into_owned);
            }
            if entry.metric_type.is_none() {
                entry.metric_type = metric_type;
            }
        }
        for key in self.created.drain() {
            self.created_owned.insert(key.into_owned());
        }
        for key in self.typed.drain() {
            self.typed_owned.insert(key.into_owned());
        }
        (
            std::mem::take(&mut self.known_owned),
            std::mem::take(&mut self.created_owned),
            std::mem::take(&mut self.typed_owned),
        )
    }

    /// Adopts the previous instance's owned memory wholesale.
    pub(crate) fn restore_owned_memory(
        &mut self,
        known: KnownMemory,
        created: HashSet<String>,
        typed: HashSet<String>,
    ) {
        self.known_owned = known;
        self.created_owned = created;
        self.typed_owned = typed;
    }
}

/// StrictMode rejects anything but a plain integer; LenientMode truncates
/// fractional values to whole milliseconds.
fn resolve_timestamp(raw: Option<&str>, strict: bool) -> Result<Option<i64>, ErrorKind> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    if let Ok(v) = raw.parse::<i64>() {
        return Ok(Some(v));
    }
    if strict {
        return Err(ErrorKind::ExpectedTimestamp {
            got: raw.to_string(),
        });
    }
    match raw.parse::<f64>() {
        Ok(v) => Ok(Some(v as i64)),
        Err(_) => Err(ErrorKind::ExpectedTimestamp {
            got: raw.to_string(),
        }),
    }
}
