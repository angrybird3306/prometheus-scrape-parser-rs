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

//! The unified [`Parser`] facade: chunk-streaming over both formats with
//! the mode chaining through the handle, `reset` ending the stream —
//! asserted against the whole-buffer reference parses.

mod common;

use common::{
    AGGREGATES, CROSS_FAMILY, INTERLEAVED_AGGREGATES, TestMode, merge_by_name,
    merge_instances_by_name, mid_row_cut_histogram, modes, owned, parse, parse_strict, rows_whole,
    sorted_by_name,
};
use prometheus_scrape_parser_rs::{
    Error, ErrorKind, IntOrFloat, LenientMode, MetricFamily, MetricFamilyView, MetricType,
    MetricValue, MetricView, Mode, Parser, ProtoFamily, Scheme, Stats, StrictMode,
};
use std::borrow::Cow;

/// Splits the input into `parts` newline-terminated pieces (clean line
/// boundaries: no line stages between calls; a family still gathering its
/// sample group carries across them).
fn split_lines(input: &[u8], parts: usize) -> Vec<&[u8]> {
    let mut out = Vec::new();
    let mut start = 0;
    let target = (input.len() / parts).max(1);
    for i in 0..input.len() {
        if i - start >= target && input[i] == b'\n' {
            out.push(&input[start..=i]);
            start = i + 1;
        }
    }
    if start < input.len() {
        out.push(&input[start..]);
    }
    out
}

fn merge_into<'a, I>(families: I, into: &mut Vec<MetricFamily<'a>>)
where
    I: Iterator<Item = MetricFamily<'a>>,
{
    for family in families {
        into.push(family);
    }
}

/// Drains any format's families through the shared view trait into the
/// owned model — one generic consumer over both yielded types.
fn merge_view_into<'a, F: MetricFamilyView<'a>>(
    families: impl Iterator<Item = F>,
    into: &mut Vec<MetricFamily<'static>>,
) {
    for family in families {
        into.push(common::owned_family(&family));
    }
}

// ======================== chunked text chaining ============================

#[test]
fn parser_text_chunked_chain_equals_whole_reference() {
    for payload in [CROSS_FAMILY, AGGREGATES, INTERLEAVED_AGGREGATES] {
        let (whole, whole_stats) = parse(payload, TestMode::Lenient);
        // Yield order may follow instance boundaries; compare sorted.
        let whole = sorted_by_name(whole);
        let chunks = split_lines(payload, 3);
        let mut parser = Parser::new(LenientMode);
        let mut families = Vec::new();
        for chunk in &chunks {
            merge_into(parser.parse_text(chunk), &mut families);
        }
        // An aggregation family a boundary leaves still gathering its
        // sample group rides to the next call; a family split between
        // whole rows lands as per-chunk pieces the merge by name
        // reassembles.
        assert_eq!(
            sorted_by_name(common::merge_instances_by_name(families)),
            whole
        );
        // Line and skip counters are instance-independent; pruned counts
        // per-flush empty slots, which boundaries create and the whole
        // parse does not.
        let stats = parser.stats();
        assert_eq!(stats.lines_total, whole_stats.lines_total);
        assert_eq!(stats.skipped_lines, whole_stats.skipped_lines);
        assert_eq!(stats.skipped_families, whole_stats.skipped_families);
        assert_eq!(stats.last_error, whole_stats.last_error);
        assert!(!parser.is_waiting());
    }
}

#[test]
fn parser_text_clean_boundary_inside_a_histogram_should_yield_one_complete_family() {
    // A chunk ending exactly on a newline mid-histogram-family must not
    // flush the half-folded family: the aggregation family stays open
    // across the clean boundary and yields once, complete.
    let part1 = b"# HELP rpc_duration RPC duration.\n\
                  # TYPE rpc_duration histogram\n\
                  rpc_duration_bucket{le=\"0.1\"} 1\n\
                  rpc_duration_bucket{le=\"0.5\"} 3\n";
    let part2 = b"rpc_duration_bucket{le=\"1.0\"} 4\n\
                  rpc_duration_bucket{le=\"+Inf\"} 5\n\
                  rpc_duration_count 5\n\
                  rpc_duration_sum 7.5\n";

    let mut parser = Parser::new(StrictMode::legacy());
    let mut families = Vec::new();
    merge_into(parser.parse_text(part1), &mut families);
    merge_into(parser.parse_text(part2), &mut families);
    assert_eq!(parser.reset(), Ok(()));

    assert_eq!(
        families.len(),
        1,
        "a clean boundary must not split the family in two yields"
    );
    assert_eq!(families[0].name.as_ref(), "rpc_duration");
    assert_eq!(families[0].metric_type, MetricType::Histogram);
    assert_eq!(families[0].help.as_deref(), Some("RPC duration."));
    assert_eq!(families[0].metrics.len(), 1);
    assert!(families[0].metrics[0].labels.is_empty());
    assert_eq!(families[0].metrics[0].timestamp, None);
    let MetricValue::Histogram(histogram) = &families[0].metrics[0].value else {
        panic!("the family must hold the histogram");
    };
    assert_eq!(histogram.sample_count, Some(IntOrFloat::Int(5)));
    assert_eq!(histogram.sample_sum, Some(7.5));
    assert_eq!(
        histogram
            .buckets
            .iter()
            .map(|b| (b.upper_bound, b.cumulative_count))
            .collect::<Vec<_>>(),
        [
            (0.1, IntOrFloat::Int(1)),
            (0.5, IntOrFloat::Int(3)),
            (1.0, IntOrFloat::Int(4)),
            (f64::INFINITY, IntOrFloat::Int(5))
        ]
    );
}

#[test]
fn parser_text_clean_boundary_inside_a_summary_should_yield_one_complete_family() {
    let part1 = b"# TYPE rpc_latencies summary\n\
                  rpc_latencies{quantile=\"0.5\"} 110\n\
                  rpc_latencies{quantile=\"0.9\"} 140\n";
    let part2 = b"rpc_latencies_sum 4711\n\
                  rpc_latencies_count 42\n";

    let mut parser = Parser::new(StrictMode::legacy());
    let mut families = Vec::new();
    merge_into(parser.parse_text(part1), &mut families);
    merge_into(parser.parse_text(part2), &mut families);
    assert_eq!(parser.reset(), Ok(()));

    assert_eq!(families.len(), 1, "the sample group must stay whole");
    assert_eq!(families[0].name.as_ref(), "rpc_latencies");
    assert_eq!(families[0].metric_type, MetricType::Summary);
    assert_eq!(families[0].metrics.len(), 1);
    let MetricValue::Summary(summary) = &families[0].metrics[0].value else {
        panic!("the family must hold the summary");
    };
    assert_eq!(summary.sample_count, Some(42));
    assert_eq!(summary.sample_sum, Some(4711.0));
    assert_eq!(
        summary
            .quantiles
            .iter()
            .map(|q| (q.quantile, q.value))
            .collect::<Vec<_>>(),
        [(0.5, 110.0), (0.9, 140.0)]
    );
}

#[test]
fn parser_text_clean_boundary_before_the_sum_should_hold_the_family() {
    // A row carrying its `_count` but not its `_sum` still gathers: the
    // boundary between the counter rows must not yield it half.
    let part1 = b"# TYPE rpc_duration histogram\n\
                  rpc_duration_bucket{le=\"+Inf\"} 5\n\
                  rpc_duration_count 5\n";
    let part2 = b"rpc_duration_sum 7.5\n";

    let mut parser = Parser::new(StrictMode::legacy());
    let mut families = Vec::new();
    merge_into(parser.parse_text(part1), &mut families);
    assert!(families.is_empty(), "the sample group is missing its sum");
    merge_into(parser.parse_text(part2), &mut families);
    assert_eq!(parser.reset(), Ok(()));

    assert_eq!(families.len(), 1);
    let MetricValue::Histogram(histogram) = &families[0].metrics[0].value else {
        panic!("the family must hold the histogram");
    };
    assert_eq!(histogram.sample_count, Some(IntOrFloat::Int(5)));
    assert_eq!(histogram.sample_sum, Some(7.5));
    assert_eq!(
        histogram
            .buckets
            .iter()
            .map(|b| (b.upper_bound, b.cumulative_count))
            .collect::<Vec<_>>(),
        [(f64::INFINITY, IntOrFloat::Int(5))]
    );
}

#[test]
fn parser_text_clean_boundary_after_a_whole_sample_group_should_yield_the_family_at_once() {
    let part1 = b"# TYPE rpc_duration histogram\n\
                  rpc_duration_bucket{le=\"+Inf\"} 5\n\
                  rpc_duration_sum 7.5\n\
                  rpc_duration_count 5\n";
    let part2 = b"other 1\n";

    let mut parser = Parser::new(StrictMode::legacy());
    let mut families = Vec::new();
    merge_into(parser.parse_text(part1), &mut families);
    assert_eq!(families.len(), 1, "a whole sample group never holds");
    let MetricValue::Histogram(histogram) = &families[0].metrics[0].value else {
        panic!("the family must hold the histogram");
    };
    assert_eq!(histogram.sample_count, Some(IntOrFloat::Int(5)));
    assert_eq!(histogram.sample_sum, Some(7.5));
    assert_eq!(
        histogram
            .buckets
            .iter()
            .map(|b| (b.upper_bound, b.cumulative_count))
            .collect::<Vec<_>>(),
        [(f64::INFINITY, IntOrFloat::Int(5))]
    );

    merge_into(parser.parse_text(part2), &mut families);
    assert_eq!(families.len(), 2);
    assert_eq!(families[1].name.as_ref(), "other");
    assert_eq!(parser.reset(), Ok(()));
}

#[test]
fn parser_text_clean_boundary_between_whole_rows_should_yield_complete_rows_per_chunk() {
    // A multi-row family splits between two whole rows: each chunk yields
    // its rows complete under the replayed metadata — rows never split,
    // whole families do.
    let part1 = b"# TYPE rpc_duration histogram\n\
                  rpc_duration_bucket{handler=\"/a\",le=\"+Inf\"} 1\n\
                  rpc_duration_sum{handler=\"/a\"} 1.5\n\
                  rpc_duration_count{handler=\"/a\"} 1\n";
    let part2 = b"rpc_duration_bucket{handler=\"/b\",le=\"+Inf\"} 2\n\
                  rpc_duration_sum{handler=\"/b\"} 2.5\n\
                  rpc_duration_count{handler=\"/b\"} 2\n";

    let mut parser = Parser::new(StrictMode::legacy());
    let mut families = Vec::new();
    merge_into(parser.parse_text(part1), &mut families);
    assert_eq!(families.len(), 1);
    let MetricValue::Histogram(head) = &families[0].metrics[0].value else {
        panic!("the family must hold the histogram");
    };
    assert_eq!(head.sample_count, Some(IntOrFloat::Int(1)));
    assert_eq!(head.sample_sum, Some(1.5));
    assert_eq!(
        families[0].metrics[0]
            .labels
            .iter()
            .map(|l| (l.name.as_ref(), l.value.as_ref()))
            .collect::<Vec<_>>(),
        [("handler", "/a")]
    );

    merge_into(parser.parse_text(part2), &mut families);
    assert_eq!(families.len(), 2);
    assert_eq!(families[1].name.as_ref(), "rpc_duration");
    assert_eq!(families[1].metric_type, MetricType::Histogram);
    let MetricValue::Histogram(tail) = &families[1].metrics[0].value else {
        panic!("the family must hold the histogram");
    };
    assert_eq!(tail.sample_count, Some(IntOrFloat::Int(2)));
    assert_eq!(tail.sample_sum, Some(2.5));
    assert_eq!(
        families[1].metrics[0]
            .labels
            .iter()
            .map(|l| (l.name.as_ref(), l.value.as_ref()))
            .collect::<Vec<_>>(),
        [("handler", "/b")]
    );
    assert_eq!(parser.reset(), Ok(()));
}

#[test]
fn parser_text_blank_and_comment_lines_should_not_break_the_hold() {
    // A blank line and a comment sit between the bucket rows and the
    // boundary: neither touches the family, the hold keeps waiting for
    // the rest of the sample group.
    let part1 = b"# TYPE rpc_duration histogram\n\
                  rpc_duration_bucket{le=\"+Inf\"} 5\n\
                  \n\
                  # a comment\n";
    let part2 = b"rpc_duration_sum 7.5\n\
                  rpc_duration_count 5\n";

    let mut parser = Parser::new(StrictMode::legacy());
    let mut families = Vec::new();
    merge_into(parser.parse_text(part1), &mut families);
    assert!(
        families.is_empty(),
        "the sample group is missing count and sum"
    );
    merge_into(parser.parse_text(part2), &mut families);
    assert_eq!(parser.reset(), Ok(()));

    assert_eq!(families.len(), 1);
    let MetricValue::Histogram(histogram) = &families[0].metrics[0].value else {
        panic!("the family must hold the histogram");
    };
    assert_eq!(histogram.sample_count, Some(IntOrFloat::Int(5)));
    assert_eq!(histogram.sample_sum, Some(7.5));
    assert_eq!(histogram.buckets.len(), 1);
}

#[test]
fn parser_text_complete_aggregation_family_yields_despite_a_staged_unrelated_tail() {
    // The staged cut line may continue the last family, so the boundary
    // carries it — but a whole sample group is dead to a differently
    // named row: the complete family yields, only the tail's own family
    // folds at reset.
    let input = b"# TYPE h histogram\n\
                  h_bucket{le=\"+Inf\"} 1\n\
                  h_count 2\n\
                  h_sum 3\n\
                  g 1";

    let mut parser = Parser::new(StrictMode::legacy());
    let mut families = Vec::new();
    merge_into(parser.parse_text(input), &mut families);
    assert_eq!(families.len(), 1, "the complete family must not hold");
    assert_eq!(families[0].name.as_ref(), "h");
    assert_eq!(families[0].metric_type, MetricType::Histogram);
    let MetricValue::Histogram(histogram) = &families[0].metrics[0].value else {
        panic!("the family must hold the histogram");
    };
    assert_eq!(histogram.sample_count, Some(IntOrFloat::Int(2)));
    assert_eq!(histogram.sample_sum, Some(3.0));
    assert_eq!(
        histogram
            .buckets
            .iter()
            .map(|b| (b.upper_bound, b.cumulative_count))
            .collect::<Vec<_>>(),
        [(f64::INFINITY, IntOrFloat::Int(1))]
    );

    assert_eq!(
        parser.reset(),
        Err(Error {
            line: 5,
            kind: ErrorKind::UnexpectedEof,
        })
    );
    let stats = parser.stats();
    assert_eq!(stats.lines_total, 5);
    assert_eq!(stats.metrics_total, 2);
    assert_eq!(stats.discarded_families, 1, "only the tail's own family");
    assert_eq!(
        stats.last_error,
        Some(Error {
            line: 5,
            kind: ErrorKind::UnexpectedEof,
        })
    );
    assert!(!parser.is_waiting());
}

#[test]
fn parser_text_complete_aggregation_family_yields_across_chunks_despite_a_staged_tail() {
    // The hold spans chunks while the group gathers; once whole, the
    // family yields even though a staged cut line sits behind it.
    let part1 = b"# TYPE h histogram\nh_bucket{le=\"+Inf\"} 1\n";
    let part2 = b"h_count 2\nh_sum 3\ng 1";

    let mut parser = Parser::new(StrictMode::legacy());
    let mut families = Vec::new();
    merge_into(parser.parse_text(part1), &mut families);
    assert!(
        families.is_empty(),
        "the sample group is missing count and sum"
    );
    merge_into(parser.parse_text(part2), &mut families);
    assert_eq!(families.len(), 1, "the whole group must not hold");
    assert_eq!(families[0].metric_type, MetricType::Histogram);
    let MetricValue::Histogram(histogram) = &families[0].metrics[0].value else {
        panic!("the family must hold the histogram");
    };
    assert_eq!(histogram.sample_count, Some(IntOrFloat::Int(2)));
    assert_eq!(histogram.sample_sum, Some(3.0));

    assert_eq!(
        parser.reset(),
        Err(Error {
            line: 5,
            kind: ErrorKind::UnexpectedEof,
        })
    );
    let stats = parser.stats();
    assert_eq!(stats.lines_total, 5);
    assert_eq!(stats.metrics_total, 2);
    assert_eq!(stats.discarded_families, 1, "only the tail's own family");
    assert!(!parser.is_waiting());
}

#[test]
fn parser_text_interleaved_aggregation_families_should_not_split_at_a_boundary() {
    // Two aggregation families gather at once and the boundary sits
    // between their rows: the carry must hold every gathering family,
    // not just the last — a family yields whole or not at all.
    fn run<M: Mode>(mode: M, part1: &[u8], part2: &[u8], count: u64, sum: f64, what: &str) {
        let mut parser = Parser::new(mode);
        let mut families = Vec::new();
        merge_into(parser.parse_text(part1), &mut families);
        assert!(
            families.is_empty(),
            "{what}: every family still gathers, nothing yields"
        );
        merge_into(parser.parse_text(part2), &mut families);
        assert_eq!(parser.reset(), Ok(()), "{what}");

        assert_eq!(families.len(), 2, "{what}: a and b yield exactly once");
        assert_eq!(families[0].name.as_ref(), "a", "{what}");
        assert_eq!(families[1].name.as_ref(), "b", "{what}");
        assert_eq!(families[0].metrics.len(), 1, "{what}");
        assert_eq!(families[1].metrics.len(), 1, "{what}");
        match &families[0].metrics[0].value {
            MetricValue::Summary(summary) => {
                assert_eq!(summary.sample_count, Some(count), "{what}");
                assert_eq!(summary.sample_sum, Some(sum), "{what}");
            }
            MetricValue::Histogram(histogram) => {
                assert_eq!(
                    histogram.sample_count,
                    Some(IntOrFloat::Int(count)),
                    "{what}"
                );
                assert_eq!(histogram.sample_sum, Some(sum), "{what}");
                assert!(!histogram.buckets.is_empty(), "{what}");
            }
            other => panic!("{what}: unexpected value {other:?}"),
        }
        assert!(rows_whole(&families), "{what}: no half sample group");
        let stats = parser.stats();
        assert_eq!(stats.lines_total, 8, "{what}");
        assert_eq!(stats.metrics_total, 2, "{what}");
        assert_eq!(stats.discarded_families, 0, "{what}");
        assert_eq!(stats.last_error, None, "{what}");
        assert!(!parser.is_waiting());
    }
    let summary1 =
        b"# TYPE a summary\n# TYPE b summary\na{quantile=\"0.5\"} 1\nb{quantile=\"0.5\"} 1\n";
    let summary2 = b"a_count 2\na_sum 1\nb_count 2\nb_sum 1\n";
    let histogram1 = b"# TYPE a histogram\n# TYPE b histogram\n\
                       a_bucket{le=\"+Inf\"} 1\n\
                       b_bucket{le=\"+Inf\"} 2\n";
    let histogram2 = b"a_count 1\na_sum 1.5\nb_count 2\nb_sum 2.5\n";
    run(LenientMode, summary1, summary2, 2, 1.0, "lenient summaries");
    run(
        StrictMode::legacy(),
        summary1,
        summary2,
        2,
        1.0,
        "strict summaries",
    );
    run(
        LenientMode,
        histogram1,
        histogram2,
        1,
        1.5,
        "lenient histograms",
    );
    run(
        StrictMode::legacy(),
        histogram1,
        histogram2,
        1,
        1.5,
        "strict histograms",
    );
}

/// Offsets just past every newline, the clean line boundaries of a
/// payload.
fn line_boundaries(input: &[u8]) -> Vec<usize> {
    (0..input.len())
        .filter(|&i| input[i] == b'\n')
        .map(|i| i + 1)
        .collect()
}

/// `# TYPE h histogram` plus one whole sample group per handler.
fn multi_row_histogram() -> Vec<u8> {
    let mut out = Vec::from(&b"# HELP h handlers.\n# TYPE h histogram\n"[..]);
    for handler in ["/a", "/b", "/c"] {
        for le in ["1", "10", "+Inf"] {
            out.extend_from_slice(
                format!("h_bucket{{handler=\"{handler}\",le=\"{le}\"}} 1\n").as_bytes(),
            );
        }
        out.extend_from_slice(format!("h_sum{{handler=\"{handler}\"}} 1.5\n").as_bytes());
        out.extend_from_slice(format!("h_count{{handler=\"{handler}\"}} 1\n").as_bytes());
    }
    out
}

fn run_cut<'a>(payload: &'a [u8], cut: usize, mode: TestMode) -> (Vec<MetricFamily<'a>>, Stats) {
    match mode {
        TestMode::Lenient => run_cut_with(payload, cut, LenientMode),
        TestMode::StrictUtf8 => run_cut_with(payload, cut, StrictMode::utf8()),
        TestMode::StrictLegacy => run_cut_with(payload, cut, StrictMode::legacy()),
    }
}

fn run_cut_with<'a, M: Mode>(
    payload: &'a [u8],
    cut: usize,
    mode: M,
) -> (Vec<MetricFamily<'a>>, Stats) {
    let mut parser = Parser::new(mode);
    let mut families: Vec<MetricFamily<'a>> = parser.parse_text(&payload[..cut]).collect();
    families.extend(parser.parse_text(&payload[cut..]));
    assert_eq!(parser.reset(), Ok(()), "{cut}: the stream ends cleanly");
    (families, parser.stats().clone())
}

/// Feeds `payload` split at every clean line boundary: an aggregation
/// family a boundary leaves still gathering its sample group must hold
/// instead of yielding a fragment.
fn assert_every_clean_boundary_keeps_sample_groups_whole(
    payload: &[u8],
    mode: TestMode,
    what: &str,
) {
    let (whole, whole_stats) = parse(payload, mode);
    let whole = sorted_by_name(whole);
    for cut in line_boundaries(payload) {
        let what = format!("{what}/{cut}");
        let (families, stats) = run_cut(payload, cut, mode);
        assert!(
            rows_whole(&families),
            "{what}: a sample group yielded split"
        );
        // Yield order may follow instance boundaries instead of global
        // creation order (an empty family prunes and re-creates later);
        // compare sorted.
        assert_eq!(
            sorted_by_name(merge_by_name(families)),
            whole,
            "{what}: the chunked pieces must merge to the whole"
        );
        assert_eq!(
            stats.metrics_total, whole_stats.metrics_total,
            "{what}: metrics"
        );
        assert_eq!(stats.discarded_families, 0, "{what}: discards");
    }
}

#[test]
fn parser_text_every_clean_boundary_lenient_should_never_split_a_sample_group() {
    let multi_row = multi_row_histogram();
    let mid_row = mid_row_cut_histogram();
    for (what, payload) in [
        ("aggregates", AGGREGATES),
        ("interleaved", INTERLEAVED_AGGREGATES),
        ("multi_row", &multi_row),
        ("mid_row", &mid_row),
    ] {
        assert_every_clean_boundary_keeps_sample_groups_whole(payload, TestMode::Lenient, what);
    }
}

#[test]
fn parser_text_every_clean_boundary_strict_utf8_should_never_split_a_sample_group() {
    let multi_row = multi_row_histogram();
    let mid_row = mid_row_cut_histogram();
    for (what, payload) in [
        ("aggregates", AGGREGATES),
        ("interleaved", INTERLEAVED_AGGREGATES),
        ("multi_row", &multi_row),
        ("mid_row", &mid_row),
    ] {
        assert_every_clean_boundary_keeps_sample_groups_whole(payload, TestMode::StrictUtf8, what);
    }
}

#[test]
fn parser_text_every_clean_boundary_strict_legacy_should_never_split_a_sample_group() {
    let multi_row = multi_row_histogram();
    let mid_row = mid_row_cut_histogram();
    for (what, payload) in [
        ("aggregates", AGGREGATES),
        ("interleaved", INTERLEAVED_AGGREGATES),
        ("multi_row", &multi_row),
        ("mid_row", &mid_row),
    ] {
        assert_every_clean_boundary_keeps_sample_groups_whole(
            payload,
            TestMode::StrictLegacy,
            what,
        );
    }
}

#[test]
fn parser_strict_abort_after_a_clean_boundary_hold_should_yield_the_held_family_with_the_staged() {
    // An abort defuses the hold like any staged family: whatever the
    // instance holds yields, incomplete rows included.
    let mut parser = Parser::new(StrictMode::legacy());
    let mut families = Vec::new();
    merge_into(
        parser.parse_text(b"# TYPE h histogram\nh_bucket{le=\"+Inf\"} 1\n"),
        &mut families,
    );
    merge_into(parser.parse_text(b"h_count -5\n"), &mut families);

    assert_eq!(families.len(), 1, "the abort yields the held family");
    let MetricValue::Histogram(histogram) = &families[0].metrics[0].value else {
        panic!("the family must hold the histogram");
    };
    assert_eq!(histogram.sample_count, None);
    assert_eq!(histogram.sample_sum, None);
    assert_eq!(histogram.buckets.len(), 1);
    assert_eq!(families[0].metrics[0].labels, []);

    let stats = parser.stats();
    assert_eq!(
        stats.last_error,
        Some(Error {
            line: 3,
            kind: ErrorKind::NegativeHistogramCount {
                family: "h".to_owned(),
            },
        })
    );
    assert_eq!(parser.reset(), Ok(()));
}

#[test]
fn parser_lenient_malformed_le_row_should_drop_itself_and_keep_the_sample_group_whole() {
    // A malformed row (bad `le`) drops itself whole — erroneous input never
    // enters the output — while the real rows still gather across a
    // boundary into one whole histogram.
    let part1 = b"# TYPE h histogram\n\
                  h_bucket{le=\"1\"} 1\n\
                  h_bucket{le=\"bla\"} 2\n";
    let part2 = b"h_sum 1.5\n\
                  h_count 1\n";

    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_into(parser.parse_text(part1), &mut families);
    assert!(families.is_empty(), "the real row still gathers");
    merge_into(parser.parse_text(part2), &mut families);
    assert_eq!(parser.reset(), Ok(()));

    assert_eq!(families.len(), 1);
    assert_eq!(families[0].metrics.len(), 1);
    let MetricValue::Histogram(histogram) = &families[0].metrics[0].value else {
        panic!("the family must hold the histogram");
    };
    assert_eq!(histogram.sample_count, Some(IntOrFloat::Int(1)));
    assert_eq!(histogram.sample_sum, Some(1.5));
    assert_eq!(histogram.buckets.len(), 1);

    let stats = parser.stats();
    assert_eq!(stats.skipped_lines, 1);
    let err = stats.last_error.as_ref().unwrap();
    assert_eq!(err.line, 3);
    assert!(
        matches!(err.kind, ErrorKind::ExpectedLeFloat { ref got } if got == "bla"),
        "unexpected error kind {err}"
    );
}

#[test]
fn parser_lenient_skipped_lines_should_keep_the_sample_group_gathering() {
    // A rejected line drops itself, the row keeps gathering: the negative
    // count cannot end the sample group, the hold waits for the real one.
    let part1 = b"# TYPE h histogram\n\
                  h_bucket{le=\"+Inf\"} 1\n\
                  h_count -5\n";
    let part2 = b"h_sum 1.5\n\
                  h_count 2\n";

    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_into(parser.parse_text(part1), &mut families);
    assert!(families.is_empty(), "the sample group is missing its sum");
    merge_into(parser.parse_text(part2), &mut families);
    assert_eq!(parser.reset(), Ok(()));

    assert_eq!(families.len(), 1);
    let MetricValue::Histogram(histogram) = &families[0].metrics[0].value else {
        panic!("the family must hold the histogram");
    };
    assert_eq!(histogram.sample_count, Some(IntOrFloat::Int(2)));
    assert_eq!(histogram.sample_sum, Some(1.5));
    assert_eq!(histogram.buckets.len(), 1);
    assert_eq!(parser.stats().skipped_lines, 1);
}

#[test]
fn parser_lenient_lex_error_should_drop_the_line_without_a_fragment() {
    // A lex error drops the whole line (lenient's line-level skip): no
    // fragment yields and nothing stays held.
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_into(
        parser.parse_text(b"# TYPE h histogram\nh_bucket{le=\"1\"} bad\n"),
        &mut families,
    );
    assert!(families.is_empty());
    assert_eq!(parser.reset(), Ok(()));

    let stats = parser.stats();
    assert_eq!(stats.skipped_lines, 1);
    assert_eq!(stats.skipped_families, 0);
    assert_eq!(stats.discarded_families, 0);
    assert_eq!(stats.metrics_total, 0);
}

#[test]
fn parser_lenient_skipped_last_row_keeps_the_half_group_discarded_at_reset() {
    // The count row skips and the group never turns whole: the held
    // family discards at reset — half data at the stream end yields
    // nothing rather than a fragment.
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_into(
        parser.parse_text(b"# TYPE h histogram\nh_bucket{le=\"1\"} 1\nh_count bad\n"),
        &mut families,
    );
    assert!(families.is_empty(), "the group is missing its count");
    assert_eq!(parser.reset(), Ok(()));

    let stats = parser.stats();
    assert_eq!(stats.lines_total, 3);
    assert_eq!(stats.metrics_total, 1);
    assert_eq!(stats.discarded_families, 1);
    assert_eq!(stats.skipped_lines, 1);
    assert_eq!(
        stats.last_error,
        Some(Error {
            line: 3,
            kind: ErrorKind::ExpectedFloatValue {
                got: "bad".to_owned(),
            },
        })
    );
    assert!(!parser.is_waiting());
}

#[test]
fn parser_text_strict_chunked_chain_equals_whole_reference() {
    // The mode instance travels with the stream, so the strict per-family
    // state machine sees the whole feed: the chunked parse must equal the
    // whole-slice reference exactly, per scheme.
    fn run<'a, M: Mode>(
        payload: &'a [u8],
        mode: M,
        whole: &[MetricFamily<'a>],
        whole_stats: &Stats,
        what: &str,
    ) {
        let chunks = split_lines(payload, 3);
        let mut parser = Parser::new(mode);
        let mut families = Vec::new();
        for chunk in &chunks {
            merge_into(parser.parse_text(chunk), &mut families);
        }
        assert_eq!(
            sorted_by_name(merge_instances_by_name(families)),
            sorted_by_name(whole.to_vec()),
            "{what}: chunked families differ from the whole-slice parse"
        );
        let stats = parser.stats();
        assert_eq!(stats.lines_total, whole_stats.lines_total, "{what}: lines");
        assert_eq!(
            stats.skipped_lines, whole_stats.skipped_lines,
            "{what}: skipped lines"
        );
        assert_eq!(
            stats.skipped_families, whole_stats.skipped_families,
            "{what}: skipped families"
        );
        assert_eq!(
            stats.last_error, whole_stats.last_error,
            "{what}: last_error"
        );
    }
    for payload in [CROSS_FAMILY, AGGREGATES, INTERLEAVED_AGGREGATES] {
        for (mode_name, mode) in modes() {
            let (whole, whole_stats) = parse(payload, mode);
            match mode {
                TestMode::Lenient => {} // covered by the lenient test above
                TestMode::StrictUtf8 => {
                    run(payload, StrictMode::utf8(), &whole, &whole_stats, mode_name)
                }
                TestMode::StrictLegacy => run(
                    payload,
                    StrictMode::legacy(),
                    &whole,
                    &whole_stats,
                    mode_name,
                ),
            }
        }
    }
}

#[test]
fn parser_text_metadata_chains_across_calls() {
    // The TYPE line lives in the first chunk, the histogram rows in the
    // second: the chained metadata memory must type the family.
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_into(parser.parse_text(b"# TYPE h histogram\n"), &mut families);
    merge_into(
        parser.parse_text(b"h_bucket{le=\"+Inf\"} 1\nh_sum 1\nh_count 1\n"),
        &mut families,
    );
    let merged = common::merge_instances_by_name(families);
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].name, "h");
    assert_eq!(merged[0].metric_type, MetricType::Histogram);
}

// =================== HELP metadata across a chunk boundary =================

#[test]
fn parser_text_help_after_samples_in_the_same_chunk_should_attach() {
    // A HELP line after the samples still finds the family pending
    // within the same chunk: the folding order costs nothing here.
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_into(parser.parse_text(b"m 1\n# HELP m hello\n"), &mut families);
    assert_eq!(families.len(), 1);
    assert_eq!(families[0].help.as_deref(), Some("hello"));
    assert_eq!(families[0].metrics.len(), 1);
}

#[test]
fn parser_text_help_before_samples_across_chunks_should_seed_the_resumed_family() {
    // HELP arrives in an earlier chunk than the samples: the metadata
    // memory seeds the family a later instance resumes.
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_into(parser.parse_text(b"# HELP m hello\n"), &mut families);
    assert!(families.is_empty(), "metadata-only families prune");
    merge_into(parser.parse_text(b"m 1\n"), &mut families);
    assert_eq!(families.len(), 1);
    assert_eq!(families[0].help.as_deref(), Some("hello"));
    assert_eq!(families[0].metrics.len(), 1);
}

#[test]
fn parser_text_help_across_a_boundary_should_attach_to_the_resuming_piece() {
    // The help-less first piece cannot be retrofitted, but the family
    // resuming after the HELP picks the text up from the memory.
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_into(parser.parse_text(b"m 1\n"), &mut families);
    merge_into(parser.parse_text(b"# HELP m hello\nm 2\n"), &mut families);
    assert_eq!(families.len(), 2);
    assert_eq!(families[0].help, None);
    assert_eq!(families[1].help.as_deref(), Some("hello"));
    assert_eq!(families[1].metrics.len(), 1);

    let (whole, _) = parse(b"m 1\n# HELP m hello\nm 2\n", TestMode::Lenient);
    assert_eq!(whole.len(), 1);
    assert_eq!(whole[0].help.as_deref(), Some("hello"));
    assert_eq!(whole[0].metrics.len(), 2);
}

#[test]
fn parser_text_help_across_a_boundary_should_reach_a_carried_aggregation_family() {
    // A family the boundary carried never yielded: the late HELP folds
    // straight onto the restored slot, and the piece is whole with it.
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_into(
        parser.parse_text(b"# TYPE s summary\ns{quantile=\"0.5\"} 1\n"),
        &mut families,
    );
    assert!(families.is_empty(), "the gathering family carries");
    merge_into(
        parser.parse_text(b"# HELP s hello\ns_count 1\ns_sum 1\n"),
        &mut families,
    );
    assert_eq!(families.len(), 1);
    assert_eq!(families[0].name, "s");
    assert_eq!(families[0].help.as_deref(), Some("hello"));
    assert_eq!(families[0].metric_type, MetricType::Summary);
    assert_eq!(families[0].metrics.len(), 1);
}

#[test]
fn parser_text_help_after_samples_across_a_boundary_should_leave_the_first_piece_help_less() {
    // The streaming limitation: the first piece yielded whole before the
    // HELP arrived and nothing it can attach to remains — the HELP only
    // lands in the metadata memory. The whole-slice read carries the
    // help; the chunked feed cannot retrofit the yielded piece.
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_into(parser.parse_text(b"# TYPE m counter\nm 1\n"), &mut families);
    merge_into(parser.parse_text(b"# HELP m hello\n"), &mut families);
    assert_eq!(families.len(), 1);
    assert_eq!(families[0].help, None);
    assert_eq!(families[0].metrics.len(), 1);

    let (whole, _) = parse(
        b"# TYPE m counter\nm 1\n# HELP m hello\n",
        TestMode::Lenient,
    );
    assert_eq!(whole.len(), 1);
    assert_eq!(whole[0].help.as_deref(), Some("hello"));
}

#[test]
fn parser_text_second_help_across_chunks_should_keep_the_first_silently() {
    // First wins across chunks through the seed: the resumed slot already
    // carries the first text, so the second HELP is a silent no-op. The
    // strict state machine errors instead — see the duplicate-HELP test.
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_into(parser.parse_text(b"# HELP m one\nm 1\n"), &mut families);
    merge_into(parser.parse_text(b"# HELP m two\nm 2\n"), &mut families);
    assert_eq!(families.len(), 2);
    assert_eq!(families[0].help.as_deref(), Some("one"));
    assert_eq!(families[1].help.as_deref(), Some("one"));
    assert_eq!(families[1].metrics.len(), 1);
}

#[test]
fn parser_text_stats_and_line_numbers_chain() {
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_into(parser.parse_text(b"c 1\nc 2\n"), &mut families);
    assert_eq!(parser.stats().lines_total, 2);
    merge_into(parser.parse_text(b"not_a_float\n"), &mut families);
    let stats = parser.stats();
    assert_eq!(stats.lines_total, 3);
    assert_eq!(stats.metrics_total, 2);
    assert_eq!(stats.skipped_lines, 1);
    assert_eq!(stats.last_error.as_ref().unwrap().line, 3);
    assert!(families.iter().all(|f| f.name == "c"));
}

#[test]
fn parser_early_drop_keeps_the_parser_usable() {
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    {
        let mut iter = parser.parse_text(b"c 1\nc 2\nc 3\n");
        families.push(iter.next().unwrap());
        // Drop mid-iteration: the staged state and the mode release back,
        // the rest of this chunk's families go with the discarded instance.
    }
    assert_eq!(families.len(), 1);
    assert_eq!(parser.stats().lines_total, 3);

    // The parser keeps working: a fresh chunk parses fresh — and the mode
    // instance survived the drop, so the stream chains on.
    merge_into(parser.parse_text(b"c 4\n"), &mut families);
    assert_eq!(parser.stats().lines_total, 4);
}

#[test]
fn parser_is_waiting_tracks_the_cut_line_and_reset_folds_it() {
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_into(parser.parse_text(b"c 1\nc 2"), &mut families);
    // The cut line waits for its continuation.
    assert!(parser.is_waiting());
    // reset ends the stream: the tail folds, reports the truncation,
    // nothing waits.
    assert_eq!(
        parser.reset(),
        Err(Error {
            line: 2,
            kind: ErrorKind::UnexpectedEof,
        })
    );
    assert!(!parser.is_waiting());

    // The next call is a fresh stream: line numbering and stats restart.
    merge_into(parser.parse_text(b"c 9\n"), &mut families);
    assert_eq!(families.last().unwrap().metrics.len(), 1);
    let stats = parser.stats();
    assert_eq!(stats.lines_total, 1);
    assert_eq!(stats.last_error, None);
}

// ==================== strict mode chains per stream ========================

#[test]
fn parser_strict_duplicate_help_across_calls_should_error_like_the_whole_read() {
    // The per-family dedup state chains through the mode member: a second
    // HELP for the same family in a later chunk errors exactly like the
    // whole-slice parse, at the same chained line number.
    let input: &[u8] = b"\n# HELP m one\n# HELP m two\n";
    for scheme in [StrictMode::legacy(), StrictMode::utf8()] {
        let mut parser = Parser::new(scheme);
        let mut families = Vec::new();
        merge_into(parser.parse_text(&input[..14]), &mut families);
        assert!(families.is_empty(), "metadata-only families prune");
        merge_into(parser.parse_text(&input[14..]), &mut families);
        let expected = Error {
            line: 3,
            kind: ErrorKind::SecondHelpLine {
                name: "m".to_owned(),
            },
        };
        assert_eq!(parser.stats().last_error, Some(expected));

        let (whole, whole_stats) = parse(input, TestMode::StrictLegacy);
        assert!(whole.is_empty());
        assert_eq!(whole_stats.last_error, parser.stats().last_error);
    }
}

#[test]
fn parser_reset_should_clear_the_mode_state_for_a_fresh_stream() {
    // A fresh stream may re-declare a family's TYPE without tripping the
    // strict per-family state machine the previous stream left behind.
    fn run<M: Mode>(mode: M, what: &str) {
        let mut parser = Parser::new(mode);
        let mut families = Vec::new();
        merge_into(parser.parse_text(b"# TYPE x counter\nx 1\n"), &mut families);
        assert_eq!(parser.reset(), Ok(()));
        merge_into(parser.parse_text(b"# TYPE x counter\nx 2\n"), &mut families);
        let stats = parser.stats();
        assert_eq!(
            stats.last_error, None,
            "{what}: the fresh stream re-declares TYPE cleanly"
        );
        assert_eq!(
            stats.lines_total, 2,
            "{what}: numbering restarts per stream"
        );
        assert_eq!(stats.metrics_total, 1, "{what}: metrics");
    }
    run(LenientMode, "lenient");
    run(StrictMode::legacy(), "strict-legacy");
    run(StrictMode::utf8(), "strict-utf8");
}

// =========================== reset as finish ===============================

#[test]
fn parser_reset_on_a_clean_end_should_report_ok() {
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_into(parser.parse_text(b"c 1\n"), &mut families);
    assert_eq!(families.len(), 1);
    assert_eq!(parser.reset(), Ok(()));
    let stats = parser.stats();
    assert_eq!(stats.lines_total, 1);
    assert_eq!(stats.metrics_total, 1);
    assert_eq!(stats.discarded_families, 0);
    assert!(!parser.is_waiting());
}

#[test]
fn parser_reset_should_fold_the_tail_lenient() {
    // The single-call whole read stages the tail: the plain family
    // flushed at the boundary yields whole, and the reset fold completes
    // the tail into its own half family — counted, never yielded.
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_into(parser.parse_text(b"c 1\nc 2"), &mut families);
    assert_eq!(families.len(), 1);
    assert_eq!(families[0].metrics.len(), 1);
    assert!(parser.is_waiting());
    assert_eq!(
        parser.reset(),
        Err(Error {
            line: 2,
            kind: ErrorKind::UnexpectedEof,
        })
    );
    let stats = parser.stats();
    assert_eq!(stats.lines_total, 2);
    assert_eq!(stats.metrics_total, 2);
    assert_eq!(stats.discarded_families, 1);
    assert_eq!(
        stats.last_error,
        Some(Error {
            line: 2,
            kind: ErrorKind::UnexpectedEof,
        })
    );
    assert!(!parser.is_waiting());
}

#[test]
fn parser_reset_should_fold_the_carried_aggregation_tail() {
    // The unterminated tail extends an open histogram: the carry holds
    // the family back from the drain, and the reset fold completes it
    // whole — counted, never yielded.
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_into(
        parser.parse_text(b"# TYPE h histogram\nh_bucket{le=\"+Inf\"} 1\nh_sum 1.5\nh_count 2"),
        &mut families,
    );
    assert!(
        families.is_empty(),
        "the carry holds the open histogram back"
    );
    assert!(parser.is_waiting());
    assert_eq!(
        parser.reset(),
        Err(Error {
            line: 4,
            kind: ErrorKind::UnexpectedEof,
        })
    );
    let stats = parser.stats();
    assert_eq!(stats.lines_total, 4);
    // The histogram's rows fold into one model Metric, tail row included.
    assert_eq!(stats.metrics_total, 1);
    assert_eq!(stats.discarded_families, 1);
    assert_eq!(
        stats.last_error,
        Some(Error {
            line: 4,
            kind: ErrorKind::UnexpectedEof,
        })
    );
    assert!(!parser.is_waiting());
}

#[test]
fn parser_reset_on_a_clean_end_should_discard_a_family_still_gathering_its_sample_group() {
    // The stream ends on a line boundary while the family still gathers:
    // rows never yield half a sample group, the family counts into
    // discarded_families. The batch TextParser::finish yields such
    // partials instead — the streaming hold exists so a later chunk can
    // still complete the group, and at the stream end nobody can.
    fn run<M: Mode>(mode: M, input: &[u8], what: &str) {
        let mut parser = Parser::new(mode);
        let mut families = Vec::new();
        merge_into(parser.parse_text(input), &mut families);
        assert!(families.is_empty(), "{what}: the incomplete family holds");
        assert_eq!(parser.reset(), Ok(()), "{what}");
        let stats = parser.stats();
        assert_eq!(stats.lines_total, 2, "{what}");
        assert_eq!(stats.metrics_total, 1, "{what}");
        assert_eq!(
            stats.discarded_families, 1,
            "{what}: the group never turned whole"
        );
        assert_eq!(stats.last_error, None, "{what}");
        assert!(!parser.is_waiting());
    }
    run(
        LenientMode,
        b"# TYPE h histogram\nh_bucket{le=\"+Inf\"} 1\n",
        "lenient histogram",
    );
    run(
        StrictMode::legacy(),
        b"# TYPE h histogram\nh_bucket{le=\"+Inf\"} 1\n",
        "strict-legacy histogram",
    );
    run(
        StrictMode::utf8(),
        b"# TYPE h histogram\nh_bucket{le=\"+Inf\"} 1\n",
        "strict-utf8 histogram",
    );
    run(
        LenientMode,
        b"# TYPE s summary\ns{quantile=\"0.5\"} 1\n",
        "lenient summary",
    );
    run(
        StrictMode::legacy(),
        b"# TYPE s summary\ns{quantile=\"0.5\"} 1\n",
        "strict-legacy summary",
    );
    run(
        StrictMode::utf8(),
        b"# TYPE s summary\ns{quantile=\"0.5\"} 1\n",
        "strict-utf8 summary",
    );
}

#[test]
fn parser_reset_on_a_truncated_end_should_discard_the_held_family_with_the_tail() {
    // The hold from the clean boundary rides the truncated tail: strict
    // reports the truncation, lenient tolerates it — either way the family
    // folds for the stats alone, never yielding.
    fn run<M: Mode>(mode: M, ended: Result<(), Error>, what: &str) {
        let mut parser = Parser::new(mode);
        let mut families = Vec::new();
        merge_into(
            parser.parse_text(b"# TYPE h histogram\nh_bucket{le=\"+Inf\"} 1\n"),
            &mut families,
        );
        merge_into(parser.parse_text(b"h_sum 1.5"), &mut families);
        assert!(families.is_empty(), "{what}");
        assert!(parser.is_waiting(), "{what}");
        assert_eq!(parser.reset(), ended, "{what}");
        assert_eq!(parser.stats().discarded_families, 1, "{what}");
    }
    run(
        StrictMode::legacy(),
        Err(Error {
            line: 3,
            kind: ErrorKind::UnexpectedEof,
        }),
        "strict",
    );
    run(
        LenientMode,
        Err(Error {
            line: 3,
            kind: ErrorKind::UnexpectedEof,
        }),
        "lenient",
    );
}

#[test]
fn parser_reset_strict_should_report_the_truncated_tail() {
    for scheme in [StrictMode::legacy(), StrictMode::utf8()] {
        let mut parser = Parser::new(scheme);
        let mut families = Vec::new();
        merge_into(parser.parse_text(b"c 1\nc 2"), &mut families);
        let ended = parser.reset();
        assert_eq!(
            ended,
            Err(Error {
                line: 2,
                kind: ErrorKind::UnexpectedEof,
            })
        );
        // The same error lands in the finalized stats.
        assert_eq!(parser.stats().last_error, ended.err());
        assert_eq!(parser.stats().lines_total, 2);
        assert_eq!(parser.stats().discarded_families, 1);
    }
}

#[test]
fn parser_reset_strict_truncation_should_match_the_go_scenario() {
    // Go scenario 0: the unterminated final line reports at its own line
    // number ("unexpected end of input stream").
    let input: &[u8] = b"\nbla 3.14\nblubber 42";
    let mut parser = Parser::new(StrictMode::legacy());
    let mut families = Vec::new();
    merge_into(parser.parse_text(input), &mut families);
    assert_eq!(
        parser.reset(),
        Err(Error {
            line: 3,
            kind: ErrorKind::UnexpectedEof,
        })
    );
    let (_, error) = parse_strict(input, Scheme::Legacy);
    assert_eq!(parser.stats().last_error, error);
}

#[test]
fn parser_reset_lenient_truncation_should_match_the_go_scenario() {
    // Go scenario 0 under lenient parsing: the unterminated final line
    // reports at its own line number, the same as strict — a cut-off
    // stream must stay detectable in every mode.
    let input: &[u8] = b"\nbla 3.14\nblubber 42";
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_into(parser.parse_text(input), &mut families);
    assert_eq!(
        parser.reset(),
        Err(Error {
            line: 3,
            kind: ErrorKind::UnexpectedEof,
        })
    );
    assert_eq!(
        parser.stats().last_error,
        Some(Error {
            line: 3,
            kind: ErrorKind::UnexpectedEof,
        })
    );
}

#[test]
fn parser_reset_should_accept_a_blank_partial_tail() {
    // A blank-only unterminated final line ends cleanly even under
    // StrictMode (a documented deviation from Go): the plain family
    // yielded at the boundary, the blank folds to nothing, and nothing
    // counts as discarded.
    fn run<M: Mode>(mode: M, what: &str) {
        let mut parser = Parser::new(mode);
        let mut families = Vec::new();
        merge_into(parser.parse_text(b"m 1\n "), &mut families);
        assert_eq!(families.len(), 1, "{what}: the plain family flushed");
        assert_eq!(parser.reset(), Ok(()), "{what}: blank partial ends cleanly");
        let stats = parser.stats();
        assert_eq!(stats.last_error, None, "{what}");
        assert_eq!(stats.lines_total, 2, "{what}");
        assert_eq!(stats.metrics_total, 1, "{what}");
        assert_eq!(stats.discarded_families, 0, "{what}");
    }
    run(LenientMode, "lenient");
    run(StrictMode::legacy(), "strict-legacy");
    run(StrictMode::utf8(), "strict-utf8");
}

#[test]
fn parser_reset_should_report_a_residual_line_of_its_own_error() {
    // The tail line's own failure owns the final error, mirroring
    // TextParser::finish — under both dispositions.
    fn run<M: Mode>(mode: M, what: &str) {
        let mut parser = Parser::new(mode);
        let mut families = Vec::new();
        merge_into(parser.parse_text(b"good 1\nbad 1 late"), &mut families);
        let error = ended_to_error(parser.reset(), what);
        assert_eq!(parser.stats().last_error, Some(error), "{what}");
    }
    run(LenientMode, "lenient");
    run(StrictMode::legacy(), "strict");
}

/// Unwraps a reset result with a `what`-tagged panic message.
fn ended_to_error(ended: Result<(), Error>, what: &str) -> Error {
    match ended {
        Ok(()) => panic!("{what}: expected an unclean-end error"),
        Err(error) => error,
    }
}

#[test]
fn parser_reset_should_count_only_the_tail_families_as_discarded() {
    // Chunk one ends on a clean boundary: its family yields whole. Chunk
    // two stages the tail; the reset fold completes it into a half
    // family — counted, never merged back.
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_into(parser.parse_text(b"c 1\nc 2\n"), &mut families);
    assert_eq!(families.len(), 1);
    assert_eq!(families[0].metrics.len(), 2);
    merge_into(parser.parse_text(b"c 3"), &mut families);
    assert_eq!(families.len(), 1, "the tail yields nothing mid-stream");
    assert_eq!(
        parser.reset(),
        Err(Error {
            line: 3,
            kind: ErrorKind::UnexpectedEof,
        })
    );
    let stats = parser.stats();
    assert_eq!(stats.lines_total, 3);
    assert_eq!(stats.metrics_total, 3);
    assert_eq!(stats.discarded_families, 1);
}

#[test]
fn parser_reset_should_be_idempotent() {
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_into(parser.parse_text(b"c 1\nc 2"), &mut families);
    assert_eq!(
        parser.reset(),
        Err(Error {
            line: 2,
            kind: ErrorKind::UnexpectedEof,
        })
    );
    let stats = parser.stats().clone();
    // A second reset finds nothing staged: the stats stay finalized.
    assert_eq!(parser.reset(), Ok(()));
    assert_eq!(parser.stats(), &stats);
    assert!(!parser.is_waiting());
}

#[test]
fn parser_reset_stats_should_clear_the_finalized_totals() {
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_into(parser.parse_text(b"c 1\nc 2\n"), &mut families);
    assert_eq!(parser.reset(), Ok(()));
    assert_ne!(parser.stats(), &Stats::default());

    parser.reset_stats();
    assert_eq!(parser.stats(), &Stats::default());
}

#[test]
fn parser_reset_stats_should_keep_the_chained_stream_state() {
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_into(parser.parse_text(b"c 1\nc 2"), &mut families);
    assert!(parser.is_waiting());
    parser.reset_stats();
    assert_eq!(parser.stats(), &Stats::default());

    assert_eq!(
        parser.reset(),
        Err(Error {
            line: 2,
            kind: ErrorKind::UnexpectedEof,
        })
    );
    let stats = parser.stats();
    // The staged line is stream state: it still folds at reset, counting
    // into the post-reset tally alone.
    assert_eq!(stats.lines_total, 1);
    assert_eq!(stats.metrics_total, 1);
    assert_eq!(stats.discarded_families, 1);
    assert!(!parser.is_waiting());
}

// ========================= reset_stats mid-stream ==========================

#[test]
fn parser_reset_stats_mid_stream_should_count_only_post_reset_input() {
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_into(parser.parse_text(b"a 1\nb 2\n"), &mut families);
    assert_eq!(parser.stats().lines_total, 2);
    parser.reset_stats();
    assert_eq!(parser.stats(), &Stats::default());
    // The zeroing sticks: the chained stage's own tally clears too, so
    // the old counts cannot resurrect at the next sync.
    merge_into(parser.parse_text(b"c 3\n"), &mut families);
    let stats = parser.stats();
    assert_eq!(stats.lines_total, 1, "counting restarts at the clear");
    assert_eq!(stats.metrics_total, 1);
    assert_eq!(stats.last_error, None);
    // The finalized totals count the post-reset input only.
    assert_eq!(parser.reset(), Ok(()));
    let stats = parser.stats();
    assert_eq!(stats.lines_total, 1);
    assert_eq!(stats.metrics_total, 1);
}

#[test]
fn parser_proto_reset_stats_mid_stream_should_count_only_post_reset_messages() {
    let input = proto_stream(&[("a", 1.0), ("b", 2.0)]);
    let cut = proto_stream(&[("a", 1.0)]).len();
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_view_into(parser.parse_proto(&input[..cut]), &mut families);
    assert_eq!(parser.stats().metrics_total, 1);
    parser.reset_stats();
    assert_eq!(parser.stats(), &Stats::default());
    merge_view_into(parser.parse_proto(&input[cut..]), &mut families);
    assert_eq!(families.len(), 2);
    let stats = parser.stats();
    assert_eq!(stats.metrics_total, 1, "counting restarts at the clear");
    assert_eq!(stats.last_error, None);
}

#[test]
fn parser_reset_stats_mid_stream_should_keep_the_staged_line_parsing_whole() {
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_into(parser.parse_text(b"a 1\ncou"), &mut families);
    assert!(parser.is_waiting());
    parser.reset_stats();
    assert_eq!(parser.stats(), &Stats::default());
    // The staged line is stream state, not stats: it completes and the
    // completed line counts into the post-reset tally alone.
    merge_into(parser.parse_text(b"nt 5\n"), &mut families);
    let stats = parser.stats();
    assert_eq!(stats.lines_total, 1);
    assert_eq!(stats.metrics_total, 1);
    assert_eq!(families.len(), 2);
    assert_eq!(families[1].name, "count");
    assert_eq!(families[1].metrics.len(), 1);
}

#[test]
fn parser_proto_reset_stats_mid_stream_should_keep_the_staged_message_parsing_whole() {
    let input = proto_stream(&[("a", 1.0), ("b", 2.0)]);
    let cut = input.len() - 4;
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_view_into(parser.parse_proto(&input[..cut]), &mut families);
    assert_eq!(families.len(), 1);
    assert!(parser.is_waiting());
    parser.reset_stats();
    assert_eq!(parser.stats(), &Stats::default());
    merge_view_into(parser.parse_proto(&input[cut..]), &mut families);
    assert_eq!(families.len(), 2);
    let stats = parser.stats();
    assert_eq!(
        stats.metrics_total, 1,
        "the completed staged message counts post-reset alone"
    );
    assert_eq!(stats.last_error, None);
}

#[test]
fn parser_reset_stats_mid_stream_should_drop_last_error() {
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_into(parser.parse_text(b"not_a_float\n"), &mut families);
    assert_ne!(parser.stats().last_error, None);
    parser.reset_stats();
    assert_eq!(parser.stats(), &Stats::default());
    // The remembered error is stats: it must not resurrect with the next
    // snapshot, and a clean line keeps the tally clean.
    merge_into(parser.parse_text(b"c 3\n"), &mut families);
    let stats = parser.stats();
    assert_eq!(stats.last_error, None);
    assert_eq!(stats.lines_total, 1);
    assert_eq!(stats.metrics_total, 1);
}

#[test]
fn parser_reset_stats_after_reset_should_restart_counting_for_a_fresh_stream() {
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_into(parser.parse_text(b"a 1\nb 2\n"), &mut families);
    assert_eq!(parser.reset(), Ok(()));
    assert_ne!(parser.stats(), &Stats::default());
    parser.reset_stats();
    assert_eq!(parser.stats(), &Stats::default());
    // A fresh stream counts from zero again.
    merge_into(parser.parse_text(b"c 3\n"), &mut families);
    let stats = parser.stats();
    assert_eq!(stats.lines_total, 1);
    assert_eq!(stats.metrics_total, 1);
    assert_eq!(stats.last_error, None);
}

// =============================== protobuf ==================================

/// One delimited counter-family message hand-encoded on the wire: family
/// field 1 name, field 4 one Metric whose field 3 wraps the Counter
/// message (field 1 fixed64 value). The TYPE field is omitted, so every
/// family decodes as COUNTER.
fn proto_stream(pairs: &[(&str, f64)]) -> Vec<u8> {
    fn uvarint(mut value: usize, out: &mut Vec<u8>) {
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                out.push(byte);
                return;
            }
            out.push(byte | 0x80);
        }
    }
    fn segment(field: u32, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(((field << 3) | 2) as u8);
        uvarint(payload.len(), &mut out);
        out.extend_from_slice(payload);
        out
    }
    let mut out = Vec::new();
    for (name, value) in pairs {
        let mut counter = Vec::new();
        counter.push(0x09); // field 1 (value), wire type 1 (fixed64)
        counter.extend_from_slice(&value.to_bits().to_le_bytes());
        let mut metric = segment(3, &counter); // field 3: the Counter message
        metric.extend_from_slice(&segment(1, &[])); // no labels
        let mut message = segment(1, name.as_bytes());
        message.extend_from_slice(&segment(4, &metric));
        uvarint(message.len(), &mut out);
        out.extend_from_slice(&message);
    }
    out
}

#[test]
fn parser_proto_chunked_matches_whole_and_completes_owned() {
    let input = proto_stream(&[("a", 1.0), ("b", 2.0)]);
    // The cut falls inside the last message, deterministically.
    let cut = input.len() - 4;

    // The whole-buffer reference.
    let (whole, _) = proto_reference(&input);
    assert_eq!(whole.len(), 2);
    assert!(whole.iter().all(|f| matches!(f.name(), Cow::Borrowed(_))));

    // The chunked feed cuts mid-message: the boundary-crossing family
    // completes through the owned path.
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    for family in parser.parse_proto(&input[..cut]) {
        families.push(family);
    }
    assert_eq!(families.len(), whole.len() - 1);
    assert!(parser.is_waiting());
    for family in parser.parse_proto(&input[cut..]) {
        families.push(family);
    }
    assert_eq!(owned(&families), owned(&whole));
    assert!(families[1].name() == "b");
    assert!(matches!(families[1].name(), Cow::Owned(_)));
    let metrics: Vec<_> = families[1].metrics().collect();
    assert_eq!(metrics[0].value(), MetricValue::Sample(2.0));
    assert_eq!(parser.stats().metrics_total, 2);
}

fn proto_reference<'a>(input: &'a [u8]) -> (Vec<ProtoFamily<'a>>, Stats) {
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    for family in parser.parse_proto(input) {
        families.push(family);
    }
    (families, parser.stats().clone())
}

#[test]
fn parser_reset_should_report_a_staged_proto_partial() {
    let input = proto_stream(&[("a", 1.0), ("b", 2.0)]);
    let cut = input.len() - 4;
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    for family in parser.parse_proto(&input[..cut]) {
        families.push(family);
    }
    assert_eq!(families.len(), 1);
    assert!(parser.is_waiting());
    // The staged partial message reports at the ordinal it would have
    // completed under.
    assert_eq!(
        parser.reset(),
        Err(Error {
            line: 2,
            kind: ErrorKind::InvalidProtoMessage {
                got: "truncated final message".to_owned(),
            },
        })
    );
    assert!(!parser.is_waiting());
}

#[test]
fn parser_proto_reset_stats_should_clear_the_finalized_totals() {
    let input = proto_stream(&[("a", 1.0)]);
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_view_into(parser.parse_proto(&input), &mut families);
    assert_eq!(parser.stats().metrics_total, 1);
    assert_eq!(parser.reset(), Ok(()));
    assert_ne!(parser.stats(), &Stats::default());

    parser.reset_stats();
    assert_eq!(parser.stats(), &Stats::default());
    assert!(!parser.is_waiting());
}

// ====================== formats and cut-line resume ========================

#[test]
fn parser_formats_are_independent() {
    // Alternating formats on one handle: each keeps its own chained state,
    // and stats() reflects whichever was driven last. Both formats drain
    // through the same view-trait helper.
    let proto_input = proto_stream(&[("a", 1.0)]);
    let mut parser = Parser::new(LenientMode);
    let mut families = Vec::new();
    merge_view_into(parser.parse_text(b"c 1\nc 2\n"), &mut families);
    assert_eq!(parser.stats().lines_total, 2);
    merge_view_into(parser.parse_proto(&proto_input), &mut families);
    assert_eq!(parser.stats().metrics_total, 1);
    merge_view_into(parser.parse_text(b"c 3\n"), &mut families);
    assert_eq!(parser.stats().lines_total, 3);
    assert!(!parser.is_waiting());
}

/// The cut-line resume shape on one facade handle, for every cut offset in
/// `cuts` (each lands mid-line): round one must stage the cut line
/// (`is_waiting`), round two completes it, and the reassembled output plus
/// the chained stats must equal the whole-slice reference. The mode
/// instance chains through the facade, so the strict schemes hold their
/// per-family rules exactly like the whole read. These scenarios never
/// split a model row across instances (aggregation families carry whole),
/// so `metrics_total` has a whole-slice reference.
fn assert_resume_matches_whole(input: &[u8], cuts: &[usize], mode: TestMode, what: &str) {
    for &cut in cuts {
        let (whole, whole_stats) = parse(input, mode);
        let (families, stats) = match mode {
            TestMode::Lenient => run_resume(input, cut, LenientMode, what),
            TestMode::StrictUtf8 => run_resume(input, cut, StrictMode::utf8(), what),
            TestMode::StrictLegacy => run_resume(input, cut, StrictMode::legacy(), what),
        };
        assert_eq!(
            sorted_by_name(merge_instances_by_name(families)),
            sorted_by_name(whole),
            "{what} cut {cut}: resumed families must reassemble the whole-slice output"
        );
        assert_eq!(
            stats.lines_total, whole_stats.lines_total,
            "{what} cut {cut}: line count"
        );
        assert_eq!(
            stats.metrics_total, whole_stats.metrics_total,
            "{what} cut {cut}: metric count"
        );
        assert_eq!(stats.last_error, None, "{what} cut {cut}: no error");
    }
}

/// One mid-line cut, two facade calls: the staged line waits in round one
/// and completes in round two.
fn run_resume<'a, M: Mode>(
    input: &'a [u8],
    cut: usize,
    mode: M,
    what: &str,
) -> (Vec<MetricFamily<'a>>, Stats) {
    let mut parser = Parser::new(mode);
    let mut families: Vec<MetricFamily<'a>> = parser.parse_text(&input[..cut]).collect();
    assert!(
        parser.is_waiting(),
        "{what} cut {cut}: the cut line must stage in the parser"
    );
    merge_into(parser.parse_text(&input[cut..]), &mut families);
    assert!(
        !parser.is_waiting(),
        "{what} cut {cut}: the completed stream must not stage"
    );
    (families, parser.stats().clone())
}

#[test]
fn metadata_keyword_cut_should_resume() {
    // The cut lands inside the "# TYPE" / "# HELP" keyword token itself.
    for (mode_name, mode) in modes() {
        let type_input: &[u8] = b"# TYPE http_requests_total counter\nhttp_requests_total 1\n";
        assert_resume_matches_whole(
            type_input,
            &[3], // "# T|YPE ..."
            mode,
            &format!("# T|YPE keyword [{mode_name}]"),
        );
        let help_input: &[u8] =
            b"# HELP http_requests_total Requests total.\nhttp_requests_total 1\n";
        assert_resume_matches_whole(
            help_input,
            &[5], // "# HEL|P ..."
            mode,
            &format!("# HEL|P keyword [{mode_name}]"),
        );
    }
}

#[test]
fn help_name_cut_should_resume() {
    let input: &[u8] = b"# HELP http_requests_total Requests total.\nhttp_requests_total 1\n";
    for (mode_name, mode) in modes() {
        assert_resume_matches_whole(
            input,
            &[16], // "# HELP http_requ|ests_total ..."
            mode,
            &format!("HELP family name [{mode_name}]"),
        );
    }
}

#[test]
fn help_text_cut_should_resume() {
    let input: &[u8] = b"# HELP http_requests_total Requests total.\nhttp_requests_total 1\n";
    for (mode_name, mode) in modes() {
        assert_resume_matches_whole(
            input,
            &[36], // "... Requests |total."
            mode,
            &format!("HELP text [{mode_name}]"),
        );
    }
}

#[test]
fn type_name_cut_should_resume() {
    let input: &[u8] = b"# TYPE http_requests_total counter\nhttp_requests_total 1\n";
    for (mode_name, mode) in modes() {
        assert_resume_matches_whole(
            input,
            &[16], // "# TYPE http_requ|ests_total ..."
            mode,
            &format!("TYPE family name [{mode_name}]"),
        );
    }
}

#[test]
fn sample_value_cut_should_resume() {
    let input: &[u8] = b"c_total 1027\n";
    for (mode_name, mode) in modes() {
        assert_resume_matches_whole(
            input,
            &[8], // "c_total |1027"
            mode,
            &format!("between name and value [{mode_name}]"),
        );
        assert_resume_matches_whole(
            input,
            &[10], // "c_total 10|27"
            mode,
            &format!("mid float [{mode_name}]"),
        );
    }
}

#[test]
fn exponent_value_cut_should_resume() {
    let input: &[u8] = b"test_counter 1.795188848e+09\n";
    for (mode_name, mode) in modes() {
        assert_resume_matches_whole(
            input,
            &[7, 8, 16, 20, 24],
            mode,
            &format!(
                "exponent float: mid name, mid mantissa (1.79518|8848e+09), e|+09 [{mode_name}]"
            ),
        );
    }
}

#[test]
fn label_value_cut_should_resume() {
    for (mode_name, mode) in modes() {
        let labeled: &[u8] = b"h_bucket{handler=\"/a\",le=\"+Inf\"} 1\n";
        assert_resume_matches_whole(
            labeled,
            &[19], // "h_bucket{handler=\"/|a\"..."
            mode,
            &format!("quoted label value [{mode_name}]"),
        );
        let escaped: &[u8] = b"m{msg=\"a\\nb\"} 1\n";
        assert_resume_matches_whole(
            escaped,
            &[9], // "m{msg=\"a\\|nb\"..."
            mode,
            &format!("escape sequence [{mode_name}]"),
        );
    }
}

#[test]
fn histogram_first_row_cut_should_resume() {
    // TYPE is complete but the first sample row is not: the typed family
    // shell exists while the cut line stages.
    let input: &[u8] = b"# TYPE h histogram\n\
         h_bucket{le=\"1\"} 1\n\
         h_bucket{le=\"+Inf\"} 2\n\
         h_sum 0.5\n\
         h_count 2\n";
    for (mode_name, mode) in modes() {
        assert_resume_matches_whole(
            input,
            &[36], // "h_bucket{le=\"1\"} |1"
            mode,
            &format!("first histogram row after TYPE [{mode_name}]"),
        );
    }
}

#[test]
fn summary_quantile_cut_should_resume() {
    let input: &[u8] = b"# TYPE s summary\n\
         s{quantile=\"0.5\"} 1.5\n\
         s_sum 100\n\
         s_count 10\n";
    for (mode_name, mode) in modes() {
        assert_resume_matches_whole(
            input,
            &[34, 35],
            mode,
            &format!("quantile row before value [{mode_name}]"),
        );
    }
}
