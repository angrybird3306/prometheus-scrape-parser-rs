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
use std::hash::Hasher;

use rustc_hash::{FxHashMap, FxHasher};

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
    /// Aggregation families: label-set signature -> row index. A pure
    /// accelerator — every hit is verified exactly, misses fall back to
    /// the linear scan, so correctness never depends on it.
    rows: FxHashMap<u64, usize>,
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

/// Mirrors Go's amd64 `uint64(value)` summary count conversion: exact
/// truncation on [0, 2^64), i64-bit wraparound on negatives, and the
/// integer-indefinite 2^63 for NaN, infinities, and >= 2^64.
fn count_as_u64(value: f64) -> u64 {
    if value.is_nan() || value < -9223372036854775808.0 || value >= 18446744073709551616.0 {
        9223372036854775808
    } else if value < 0.0 {
        (value as i64) as u64
    } else {
        value as u64
    }
}

/// Histogram count routing: exact non-negative integers
/// stay integral, everything else goes float, negatives are rejected.
fn histogram_count(value: f64) -> Result<IntOrFloat, ()> {
    if value < 0.0 {
        return Err(());
    }
    // Go's amd64 uint64(value) round-trip fails for every float at or above
    // 2^64, so they land float, not a saturated cast.
    if value < 18446744073709551616.0 {
        let uv = value as u64;
        if uv as f64 == value {
            return Ok(IntOrFloat::Int(uv));
        }
    }
    Ok(IntOrFloat::Float(value))
}

/// Whether a stripped label set names the same aggregation row as an
/// existing metric's labels: equal sizes and equal canonical (sorted) pair
/// streams, input order insignificant. Allocation-free; the exact fallback
/// behind the signature index (`label_signature` mirrors Go's
/// LabelsToSignature row merge).
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

/// Order-insensitive hash of a label set (Go's LabelsToSignature shape):
/// per-pair name and value hashes mixed and summed, so input order never
/// changes the signature. Collisions are allowed; the exact
/// [`label_sets_equal`] check gates every index hit.
fn label_signature(labels: &[Label<'_>]) -> u64 {
    let mut sig = 0u64;
    for l in labels {
        let mut h = FxHasher::default();
        h.write(l.name.as_bytes());
        h.write_u8(0xff);
        let n = h.finish();
        let mut h = FxHasher::default();
        h.write(l.value.as_bytes());
        h.write_u8(0xff);
        sig = sig.wrapping_add(n.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ h.finish().rotate_left(23));
    }
    sig
}

/// Takes the sample's labels for a freshly created row. An empty set
/// (plain or pure-special lines) leaves the spare capacity with the
/// caller's recycle instead of an empty-capacity Vec inside the row.
fn take_row_labels<'a>(sample: &mut SampleLine<'a>) -> Vec<Label<'a>> {
    let labels = std::mem::take(&mut sample.labels);
    if labels.is_empty() {
        sample.labels = labels;
        Vec::new()
    } else {
        labels
    }
}

/// Exact label-set equality; the same-order case is a linear compare,
/// different orders fall to the canonical sorted compare.
fn label_sets_equal(stored: &[Label<'_>], probe: &[Label<'_>]) -> bool {
    stored.len() == probe.len()
        && (stored
            .iter()
            .zip(probe)
            .all(|(a, b)| a.name == b.name && a.value == b.value)
            || same_label_set(stored, probe))
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
/// into the bound and dropped. Go validates every special label, so the
/// first invalid value wins; otherwise the last valid one does. Returns
/// the bound and the packed regular prefix, so row creation moves the
/// original Vec without a second allocation.
fn strip_special_labels<'a, 'v>(
    labels: &'v mut Vec<Label<'a>>,
    special: &str,
) -> (Option<Result<f64, String>>, &'v [Label<'a>]) {
    let mut bound: Option<Result<f64, String>> = None;
    let mut write = 0;
    let mut read = 0;
    while read < labels.len() {
        if labels[read].name == special {
            let parsed = parse_float_token(&labels[read].value);
            if bound.as_ref().is_none_or(|b| b.is_ok()) {
                bound = Some(match parsed {
                    Ok(v) => Ok(v),
                    Err(()) => Err(labels[read].value.to_string()),
                });
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

/// One family's stream-wide state in the single family table: the live
/// pending slot when the generation matches, first-wins metadata, and
/// whether a type was assigned. Key presence is the created shadow.
#[derive(Default)]
struct FamilyEntry {
    stamp: u32,
    slot: Option<usize>,
    help: Option<String>,
    metric_type: Option<MetricType>,
    typed: bool,
}

impl FamilyEntry {
    /// The slot this entry's family currently folds into, when the entry
    /// was seeded by the current instance generation.
    fn live_slot(&self, stamp: u32) -> Option<usize> {
        if self.stamp == stamp { self.slot } else { None }
    }
}

/// The single name-keyed family table, held across instances in the
/// [`StageArea`](crate::parse_text::StageArea). One probe replaces the
/// former index/known/created/typed stack and their owned mirrors; the
/// generation stamp invalidates every slot at an instance boundary in
/// O(1), and the map's capacity persists across instances.
#[derive(Default)]
pub(crate) struct FamilyTable {
    map: FxHashMap<String, FamilyEntry>,
    stamp: u32,
}

impl FamilyTable {
    pub(crate) fn clear(&mut self) {
        self.map.clear();
        self.stamp = 0;
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
    /// The single name-keyed family table: live slots, first-wins
    /// metadata, the assigned-type shadow, and the created shadow (key
    /// presence) — everything the former seven-table stack tracked.
    families: FamilyTable,
    /// The family of the previous sample line: consecutive same-family
    /// lines skip suffix resolution and hashing entirely. Cleared by any
    /// metadata fold, yield, or compaction — anything that can change a
    /// name's resolution or slot.
    last: Option<(Cow<'a, str>, usize)>,
    /// Indices of the families surviving pruning, in creation order; filled
    /// by [`Assembler::flush_ready`], consumed by [`Assembler::take_next`].
    ready_indices: Vec<u32>,
    /// Old slot -> new slot of the last compaction, reused across flushes.
    remap: Vec<Option<u32>>,
    cursor: usize,
    /// Metric rows created since the last drain, folded into
    /// `Stats::metrics_total`; aggregation lines folding into an existing
    /// row count nothing.
    created_metrics: u64,
}

impl<'a> Assembler<'a> {
    fn lookup_type(&self, family_name: &str) -> Option<Option<MetricType>> {
        // Key presence is the created shadow: an existing entry answers
        // with its type (or none), whatever layer recorded it.
        self.families.map.get(family_name).map(|e| e.metric_type)
    }

    pub(crate) fn family_name<'r>(&self, raw: &'r str) -> &'r str {
        resolve_family_name(raw, |name| self.lookup_type(name))
    }

    // The Cow preserves the borrowed/owned name distinction: a borrowed
    // pending name stays zero-copy, an owned one outlives its token.
    #[allow(clippy::ptr_arg)]
    fn family_idx(&mut self, resolved: &Cow<'a, str>) -> usize {
        let stamp = self.families.stamp;
        match self.families.map.get_mut(resolved.as_ref()) {
            Some(entry) => {
                if let Some(slot) = entry.live_slot(stamp) {
                    return slot;
                }
                // The previous slot yielded or the entry crossed an
                // instance: seed a fresh pending family from the entry's
                // first-wins metadata, exactly as a whole-input family map
                // would resume it.
                let seed_help = entry.help.clone();
                let seed_type = entry.metric_type;
                entry.stamp = stamp;
                entry.slot = Some(self.pending_metric_families.len());
                let i = self.pending_metric_families.len();
                self.pending_metric_families.push(PendingMetricFamily {
                    name: resolved.clone(),
                    help: seed_help.map(Cow::Owned),
                    metric_type: seed_type,
                    metrics: Vec::new(),
                    rows: FxHashMap::default(),
                });
                i
            }
            None => {
                let i = self.pending_metric_families.len();
                self.families.map.insert(
                    resolved.as_ref().to_string(),
                    FamilyEntry {
                        stamp,
                        slot: Some(i),
                        help: None,
                        metric_type: None,
                        typed: false,
                    },
                );
                self.pending_metric_families.push(PendingMetricFamily {
                    name: resolved.clone(),
                    help: None,
                    metric_type: None,
                    metrics: Vec::new(),
                    rows: FxHashMap::default(),
                });
                i
            }
        }
    }

    /// Resolves the raw name and returns its family slot, creating the
    /// family if absent; fold paths hold the slot instead of the name so
    /// the borrowed name never crosses a sample move.
    fn resolve_idx(&mut self, name: &Token<'a>) -> usize {
        // The previous sample line's family, matched on the raw name:
        // consecutive same-family lines are the dominant pattern and skip
        // suffix resolution and hashing entirely. A raw-name match is
        // sound because resolution only changes when a metadata fold
        // creates or retypes a candidate — and those clear the memory.
        if let Some((last, slot)) = &self.last
            && last.as_ref() == name.decoded.as_ref()
        {
            return *slot;
        }
        self.last = None;
        let resolved = match &name.decoded {
            Cow::Borrowed(s) => {
                // `s` derefs to a true 'a input slice, so the resolved key
                // can stay borrowed; resolving on the Cow would tie it to
                // this call and force an owned key.
                Cow::Borrowed(resolve_family_name(s, |n| self.lookup_type(n)))
            }
            Cow::Owned(_) => {
                let resolved = resolve_family_name(&name.decoded, |n| self.lookup_type(n));
                Cow::Owned(resolved.to_string())
            }
        };
        let slot = self.family_idx(&resolved);
        self.last = Some((resolved, slot));
        slot
    }

    /// Folds a HELP line: first wins, on the instance and in the table.
    pub(crate) fn fold_help(&mut self, name: Token<'a>, text: Token<'a>) {
        self.last = None;
        let i = self.resolve_idx(&name);
        if self.pending_metric_families[i].help.is_none() {
            let remembered = text.decoded.into_owned();
            let fam = &mut self.pending_metric_families[i];
            let key = fam.name.as_ref();
            if let Some(entry) = self.families.map.get_mut(key)
                && entry.help.is_none()
            {
                entry.help = Some(remembered.clone());
                fam.help = Some(Cow::Owned(remembered));
            }
        }
    }

    /// Folds a TYPE line: first wins, on the instance and in the table.
    pub(crate) fn fold_type(&mut self, name: Token<'a>, metric_type: MetricType) {
        // The whole-input fold resists a TYPE line once anything assigned its
        // type — a previous TYPE line, or a sample's implicit `Untyped`
        // default. Check before `resolve_idx`, whose creation would put
        // the name into the assigned set itself.
        self.last = None;
        let assigned = {
            let resolved = self.family_name(&name.decoded);
            self.families.map.get(resolved).is_some_and(|e| e.typed)
        };
        let i = self.resolve_idx(&name);
        if !assigned {
            let fam = &mut self.pending_metric_families[i];
            if fam.metric_type.is_none() {
                fam.metric_type = Some(metric_type);
            }
            let key = fam.name.as_ref();
            if let Some(entry) = self.families.map.get_mut(key) {
                if entry.metric_type.is_none() {
                    entry.metric_type = Some(metric_type);
                }
                entry.typed = true;
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
        {
            let fam = &mut self.pending_metric_families[idx];
            if fam.metric_type.is_none() {
                // The implicit default assigns the type stream-wide: a
                // later TYPE line must not override it.
                fam.metric_type = Some(MetricType::Untyped);
                let key = fam.name.as_ref();
                if let Some(entry) = self.families.map.get_mut(key) {
                    entry.typed = true;
                }
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
        let labels = take_row_labels(sample);
        self.pending_metric_families[idx].metrics.push(Metric {
            labels,
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
            Some(Err(got)) => return Err(ErrorKind::ExpectedQuantileFloat { got }),
        };
        // Only aggregation rows merge.
        let row_idx = {
            let sig = label_signature(rest);
            let fam = &mut self.pending_metric_families[idx];
            let known = fam.rows.get(&sig).copied().filter(|&r| {
                matches!(fam.metrics[r].value, MetricValue::Summary(_))
                    && label_sets_equal(&fam.metrics[r].labels, rest)
            });
            match known {
                Some(r) => r,
                None => match fam.metrics.iter().position(|m| {
                    matches!(m.value, MetricValue::Summary(_)) && same_label_set(&m.labels, rest)
                }) {
                    Some(r) => {
                        fam.rows.insert(sig, r);
                        r
                    }
                    None => {
                        self.created_metrics += 1;
                        let labels = take_row_labels(sample);
                        fam.metrics.push(Metric {
                            labels,
                            timestamp: None,
                            value: MetricValue::Summary(SummaryValue::default()),
                        });
                        let r = fam.metrics.len() - 1;
                        fam.rows.insert(sig, r);
                        r
                    }
                },
            }
        };
        let family_name = &self.pending_metric_families[idx].name;
        let count_row = is_family_count(family_name, &sample.name.decoded);
        let sum_row = is_family_sum(family_name, &sample.name.decoded);
        if count_row {
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
            Some(Err(got)) => return Err(ErrorKind::ExpectedLeFloat { got }),
        };
        // Only aggregation rows merge.
        let row_idx = {
            let sig = label_signature(rest);
            let fam = &mut self.pending_metric_families[idx];
            let known = fam.rows.get(&sig).copied().filter(|&r| {
                matches!(fam.metrics[r].value, MetricValue::Histogram(_))
                    && label_sets_equal(&fam.metrics[r].labels, rest)
            });
            match known {
                Some(r) => r,
                None => match fam.metrics.iter().position(|m| {
                    matches!(m.value, MetricValue::Histogram(_)) && same_label_set(&m.labels, rest)
                }) {
                    Some(r) => {
                        fam.rows.insert(sig, r);
                        r
                    }
                    None => {
                        self.created_metrics += 1;
                        let labels = take_row_labels(sample);
                        fam.metrics.push(Metric {
                            labels,
                            timestamp: None,
                            value: MetricValue::Histogram(HistogramValue::default()),
                        });
                        let r = fam.metrics.len() - 1;
                        fam.rows.insert(sig, r);
                        r
                    }
                },
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
        for entry in self.families.map.values_mut() {
            if let Some(slot) = entry.slot {
                entry.slot = remap[slot].map(|new| new as usize);
            }
        }
        self.last = None;
        pruned
    }

    /// Pops the next surviving family, histogram-normalized and with its
    /// type defaulted. The slot becomes a shell that keeps its name keyed
    /// — dead to folds — until the next flush compacts it away.
    pub(crate) fn take_next(&mut self) -> Option<MetricFamily<'a>> {
        let index = *self.ready_indices.get(self.cursor)?;
        self.cursor += 1;
        let name = self.pending_metric_families[index as usize].name.clone();
        if let Some(entry) = self.families.map.get_mut(name.as_ref()) {
            // The family yields but keeps shadowing: its entry stays with
            // the slot cleared.
            entry.slot = None;
        }
        self.last = None;
        Some(std::mem::take(&mut self.pending_metric_families[index as usize]).into())
    }

    /// The open slot of the family `resolved` names, when that family is
    /// aggregation-typed (summary/histogram): the zero-copy parser's carry
    /// target when the staged cut line names it.
    pub(crate) fn aggregation_slot_of(&self, resolved: &str) -> Option<usize> {
        let entry = self.families.map.get(resolved)?;
        let i = entry.live_slot(self.families.stamp)?;
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

    /// Every open aggregation slot still gathering its sample group, in
    /// creation order: the families a boundary carries over.
    pub(crate) fn gathering_aggregation_slots(&self) -> Vec<usize> {
        (0..self.pending_metric_families.len())
            .filter(|&i| self.gathers_sample_group(i))
            .collect()
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

        // The slot closes so nothing folds into the shell; the table's
        // metadata transfers separately.
        let family = MetricFamily::from(std::mem::take(&mut self.pending_metric_families[slot]));
        if family.name.as_ref().is_empty() {
            return None;
        }
        if let Some(entry) = self.families.map.get_mut(family.name.as_ref()) {
            entry.slot = None;
        }
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
        let MetricFamily {
            name,
            help,
            metric_type,
            metrics,
        } = family;
        let i = self.pending_metric_families.len();
        match self.families.map.get_mut(name.as_ref()) {
            Some(entry) => {
                entry.stamp = self.families.stamp;
                entry.slot = Some(i);
            }
            None => {
                self.families.map.insert(
                    name.as_ref().to_string(),
                    FamilyEntry {
                        stamp: self.families.stamp,
                        slot: Some(i),
                        help: help.as_deref().map(str::to_owned),
                        metric_type: Some(metric_type),
                        typed: true,
                    },
                );
            }
        }
        self.pending_metric_families.push(PendingMetricFamily {
            name,
            help,
            metric_type: Some(metric_type),
            metrics,
            // The carried family's rows rebuild lazily: the first probe
            // per label set falls back to the scan and backfills.
            rows: FxHashMap::default(),
        });
    }

    /// Materializes the metadata memory at the instance boundary: the
    /// entries this instance added convert to owned — the only per-boundary
    /// cost — and the owned layers move out as a whole, so transfers stay
    /// linear in what each instance adds.
    /// Hands the family table to the next instance wholesale: the
    /// generation stamp retires every slot in O(1) and the map's capacity
    /// persists across instances.
    pub(crate) fn release_family_table(&mut self) -> FamilyTable {
        std::mem::take(&mut self.families)
    }

    /// Adopts the previous instance's family table wholesale.
    pub(crate) fn restore_family_table(&mut self, families: FamilyTable) {
        self.families = families;
        self.last = None;
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
