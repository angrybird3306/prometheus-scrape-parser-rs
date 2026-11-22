//! Shared helpers for the parser test files. Each test binary pulls in
//! the whole module, so unused helpers are expected.
#![allow(dead_code)]

use std::borrow::Cow;

use prometheus_scrape_parser_rs::{
    IntOrFloat, Label, LenientMode, Metric, MetricFamily, MetricFamilyView, MetricType,
    MetricValue, MetricView, Scheme, StageArea, Stats, StrictMode, TextParser,
};

pub mod scenarios;

/// Materializes one family view into the owned model through the shared
/// trait, for whole-content comparisons independent of a view's
/// borrowed/owned provenance.
pub fn owned_family<'a, F: MetricFamilyView<'a>>(family: &F) -> MetricFamily<'static> {
    MetricFamily {
        name: Cow::Owned(family.name().into_owned()),
        help: family.help().map(|help| Cow::Owned(help.into_owned())),
        metric_type: family.metric_type(),
        metrics: family
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

/// Materializes every family view into the owned model.
pub fn owned<'a, F: MetricFamilyView<'a>>(families: &[F]) -> Vec<MetricFamily<'static>> {
    families.iter().map(|family| owned_family(family)).collect()
}

/// Every mode combination the chunked-parse tests exercise.
#[derive(Clone, Copy)]
pub enum TestMode {
    Lenient,
    StrictUtf8,
    StrictLegacy,
}

pub fn modes() -> Vec<(&'static str, TestMode)> {
    vec![
        ("lenient", TestMode::Lenient),
        ("strict-utf8", TestMode::StrictUtf8),
        ("strict-legacy", TestMode::StrictLegacy),
    ]
}

/// Reference run: families plus stats for one mode, folded by one
/// whole-slice [`TextParser`] instance.
pub fn parse(input: &[u8], mode: TestMode) -> (Vec<MetricFamily<'_>>, Stats) {
    fn run<'a, M: prometheus_scrape_parser_rs::Mode>(
        input: &'a [u8],
        mode: M,
    ) -> (Vec<MetricFamily<'a>>, Stats) {
        let mut parser = TextParser::new(StageArea::default(), mode, input);
        let mut families = Vec::new();
        while let Some(family) = parser.next_family() {
            families.push(family);
        }
        let stats = parser.finish();
        while let Some(family) = parser.next_family() {
            families.push(family);
        }
        (families, stats)
    }
    match mode {
        TestMode::Lenient => run(input, LenientMode),
        TestMode::StrictUtf8 => run(input, StrictMode::utf8()),
        TestMode::StrictLegacy => run(input, StrictMode::legacy()),
    }
}

/// Strict one-shot reference shaped like Go's TextToMetricFamilies: the
/// families accumulated before the first error (Go returns a partial map
/// alongside the error) together with that error.
pub fn parse_strict(
    input: &[u8],
    scheme: Scheme,
) -> (
    Vec<MetricFamily<'_>>,
    Option<prometheus_scrape_parser_rs::Error>,
) {
    let mode = match scheme {
        Scheme::Utf8 => TestMode::StrictUtf8,
        Scheme::Legacy => TestMode::StrictLegacy,
    };
    let (families, stats) = parse(input, mode);
    (families, stats.last_error)
}

/// One histogram family, real-scrape shaped: per handler one 11-line row
/// (nine `le` buckets, `_sum`, `_count`), the handler label the only
/// varying part, sized so a chunk boundary falls mid-row and the
/// aggregation row must move whole.
pub fn mid_row_cut_histogram() -> Vec<u8> {
    const LES: [&str; 9] = [
        "100", "1000", "10000", "100000", "1e+06", "1e+07", "1e+08", "1e+09", "+Inf",
    ];
    let mut input = String::from(
        "# HELP http_response_size_bytes Histogram of response size for HTTP requests.\n\
         # TYPE http_response_size_bytes histogram\n",
    );
    for i in 0..100u64 {
        let handler = format!("/api/v1/label/:{i}/values");
        for le in LES {
            input.push_str(&format!(
                "http_response_size_bytes_bucket{{handler=\"{handler}\",le=\"{le}\"}} 1\n"
            ));
        }
        input.push_str(&format!(
            "http_response_size_bytes_sum{{handler=\"{handler}\"}} {i}.5\n"
        ));
        input.push_str(&format!(
            "http_response_size_bytes_count{{handler=\"{handler}\"}} {}\n",
            i + 1
        ));
    }
    input.into_bytes()
}

/// A histogram family whose rows are scattered across many instances by a
/// padding family: every resumed instance must inherit the HELP/TYPE the
/// whole-input fold carries, so folding and the merged output match it.
pub fn scattered_histogram() -> Vec<u8> {
    let mut input = String::from("# HELP h Histogram.\n# TYPE h histogram\n");
    for i in 0..2000u64 {
        let handler = format!("/api/v{i}");
        for le in ["100", "1000", "+Inf"] {
            input.push_str(&format!(
                "h_bucket{{handler=\"{handler}\",le=\"{le}\"}} 1\n"
            ));
        }
        input.push_str(&format!("h_sum{{handler=\"{handler}\"}} {i}.5\n"));
        input.push_str(&format!("h_count{{handler=\"{handler}\"}} {}\n", i + 1));
        for _ in 0..30 {
            input.push_str("p 1\n");
        }
    }
    input.into_bytes()
}

/// Merges per-instance pieces of the same family: the first instance wins
/// the metadata, metrics append in input order.
pub fn merge_by_name<'a>(mut families: Vec<MetricFamily<'a>>) -> Vec<MetricFamily<'a>> {
    let mut merged: Vec<MetricFamily<'a>> = Vec::new();
    for family in families.drain(..) {
        match merged.iter().position(|m| m.name == family.name) {
            Some(i) => merged[i].metrics.extend(family.metrics),
            None => merged.push(family),
        }
    }
    merged
}

/// Canonical family order for comparisons whose yield order may follow
/// instance boundaries instead of global creation order (the zero-copy
/// carry yields a family from its continuation instance).
pub fn sorted_by_name<'a>(mut families: Vec<MetricFamily<'a>>) -> Vec<MetricFamily<'a>> {
    families.sort_by(|a, b| a.name.as_ref().cmp(b.name.as_ref()));
    families
}

/// The cross-instance twin of [`merge_by_name`]: instances additionally fold
/// summary/histogram rows that a feed boundary split back into one metric,
/// and backfill family metadata. A clean line boundary carries nothing, so
/// an aggregation row's continuation can land in a fresh instance; its
/// fragments carry disjoint halves (buckets/quantiles vs
/// sample_count/sample_sum) under one label set, and the fold reconstructs
/// the whole-input row. A HELP/TYPE line placed after a family's samples
/// enriches only the instances after it — the merged family takes the
/// first metadata any instance carries, which is exactly the whole-input
/// fold's.
pub fn merge_instances_by_name<'a>(mut families: Vec<MetricFamily<'a>>) -> Vec<MetricFamily<'a>> {
    let mut merged: Vec<MetricFamily<'a>> = Vec::new();
    for family in families.drain(..) {
        match merged.iter().position(|m| m.name == family.name) {
            Some(i) => {
                if merged[i].help.is_none() {
                    merged[i].help = family.help;
                }
                if merged[i].metric_type == MetricType::Untyped
                    && family.metric_type != MetricType::Untyped
                {
                    merged[i].metric_type = family.metric_type;
                }
                merged[i].metrics.extend(family.metrics);
            }
            None => merged.push(family),
        }
    }
    for family in merged.iter_mut() {
        // Only aggregation-typed families can hold split rows; plain
        // families stay untouched (their samples are distinct metrics).
        if !matches!(
            family.metric_type,
            MetricType::Summary | MetricType::Histogram | MetricType::GaugeHistogram
        ) {
            continue;
        }
        let mut rows: Vec<Metric<'a>> = Vec::with_capacity(family.metrics.len());
        for metric in family.metrics.drain(..) {
            let matched = rows
                .iter_mut()
                .find(|row| same_label_set(&row.labels, &metric.labels));
            match matched {
                Some(row) => fold_row_fragment(row, metric),
                None => rows.push(metric),
            }
        }
        family.metrics = rows;
    }
    renormalize_merged(&mut merged);
    merged
}

/// Replicates the parser's histogram normalization across a row the merge
/// reassembled: one float counter switches every counter in the histogram
/// to float. A fragment yields normalized with only the counters it saw,
/// so a bucket fragment whose float count lived in another instance keeps
/// Int counters; the reassembled row must match the whole-input fold's
/// whole-row
/// normalization, and the rule is monotone in the merge order.
fn renormalize_merged(families: &mut [MetricFamily<'_>]) {
    let to_float = |c: IntOrFloat| match c {
        IntOrFloat::Int(v) => IntOrFloat::Float(v as f64),
        f => f,
    };
    for family in families.iter_mut() {
        for m in &mut family.metrics {
            if let MetricValue::Histogram(h) = &mut m.value {
                let any_float = matches!(h.sample_count, Some(IntOrFloat::Float(_)))
                    || h.buckets
                        .iter()
                        .any(|b| matches!(b.cumulative_count, IntOrFloat::Float(_)));
                if any_float {
                    h.sample_count = h.sample_count.map(to_float);
                    for b in &mut h.buckets {
                        b.cumulative_count = to_float(b.cumulative_count);
                    }
                }
            }
        }
    }
}

/// Label-set equality in the order-insensitive sense the folder's row
/// matching uses.
fn same_label_set(a: &[Label<'_>], b: &[Label<'_>]) -> bool {
    a.len() == b.len() && {
        let mut ka: Vec<_> = a
            .iter()
            .map(|l| (l.name.as_ref(), l.value.as_ref()))
            .collect();
        let mut kb: Vec<_> = b
            .iter()
            .map(|l| (l.name.as_ref(), l.value.as_ref()))
            .collect();
        ka.sort_unstable();
        kb.sort_unstable();
        ka == kb
    }
}

fn fold_row_fragment(row: &mut Metric<'_>, fragment: Metric<'_>) {
    // The value kinds agree by construction: only aggregation-typed
    // families reach the fold, and a family's type fixes the row kind.
    match (&mut row.value, fragment.value) {
        (MetricValue::Summary(r), MetricValue::Summary(f)) => {
            r.quantiles.extend(f.quantiles);
            if r.sample_count.is_none() {
                r.sample_count = f.sample_count;
            }
            if r.sample_sum.is_none() {
                r.sample_sum = f.sample_sum;
            }
        }
        (MetricValue::Histogram(r), MetricValue::Histogram(f)) => {
            r.buckets.extend(f.buckets);
            if r.sample_count.is_none() {
                r.sample_count = f.sample_count;
            }
            if r.sample_sum.is_none() {
                r.sample_sum = f.sample_sum;
            }
        }
        _ => {}
    }
    // The folder's rows keep the last sample line's timestamp; fragments
    // replay in input order, so last wins here too.
    if fragment.timestamp.is_some() {
        row.timestamp = fragment.timestamp;
    }
}

/// Cross-family interleaving (Go scenario 3 shape): lines of one family
/// separated by other families, merged across the whole input.
pub const CROSS_FAMILY: &[u8] = b"# HELP http_requests_total requests\n\
# TYPE http_requests_total counter\n\
http_requests_total{method=\"get\"} 1027\n\
other_family 5\n\
http_requests_total{method=\"get\"} 1028\n\
http_requests_total{method=\"post\"} 3\n\
# TYPE foo summary\n\
foo_count 7\n\
fake_sum 5\n\
foo_sum 4\n";

/// Summary and histogram aggregation with escape sequences and timestamps.
pub const AGGREGATES: &[u8] = b"# TYPE latency summary\n\
latency{quantile=\"0.5\"} 1.5\n\
latency_sum 100\n\
latency_count 10\n\
# TYPE size histogram\n\
size_bucket{le=\"+Inf\"} 20\n\
size_bucket{le=\"10.0\"} 8\n\
size_count 20\n\
size_sum 3.5e2\n\
quoted{label=\"a\\nb\"} 1 1234567890\n";

/// Quoted UTF-8 names, comment lines, and blank lines.
pub const UTF8_AND_BLANKS: &str = "\n\
# TYPE \"métrique\" gauge\n\
{\"métrique\"} 42\n\
{\"名字\"}{\"标签\"=\"值\"} 7\n\
# plain comment\n\n\n";

/// Lenient-only input: an invalid float drops one line, the family survives.
pub const LENIENT_PARTIAL: &[u8] = b"# TYPE keep gauge\n\
keep 1\n\
keep not_a_float\n\
keep 3\n\
unrelated 9\n";

/// Strict-only truncation: content without a trailing newline.
pub const TRUNCATED: &[u8] = b"# TYPE t counter\nt 1";

/// Skipped lines whose labels were already staged before the error, then a
/// label-less success: the skipped labels must not leak into it.
pub const SKIPPED_LABELED: &[u8] = b"broken{a=\"1\"} not_a_float\n\
broken{c=\"3\"} 1 2 3\n\
good 1\n\
good{b=\"2\"} 2\n\
@also_broken{x=\"4\"}\n\
good 3\n";

pub fn payloads() -> Vec<(&'static str, &'static [u8])> {
    vec![
        ("cross_family", CROSS_FAMILY),
        ("aggregates", AGGREGATES),
        ("utf8_and_blanks", UTF8_AND_BLANKS.as_bytes()),
        ("lenient_partial", LENIENT_PARTIAL),
        ("truncated", TRUNCATED),
        ("skipped_labeled", SKIPPED_LABELED),
        ("empty", b""),
    ]
}
