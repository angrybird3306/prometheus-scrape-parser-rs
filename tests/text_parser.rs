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

//! TextParser invariance: instances chained through `StageArea`, each
//! over one borrowed slice, must describe the same exposition as the
//! whole-slice parse. A clean line boundary carries nothing, so
//! line-aligned feeds merge back to that output exactly; a boundary cutting
//! a line in half carries the aggregation family it extends, which can move
//! a family's yield into its continuation instance — byte-level feeds
//! therefore compare order-insensitively and fold boundary-split rows back
//! together.

mod common;

use common::{
    SKIPPED_LABELED, TestMode, merge_by_name, merge_instances_by_name, mid_row_cut_histogram,
    modes, parse, payloads, scattered_histogram, sorted_by_name,
};
use prometheus_scrape_parser_rs::{
    ErrorKind, IntOrFloat, LenientMode, MetricFamily, MetricType, MetricValue, StageArea, Stats,
    StrictMode, TextParser,
};

/// Chains one TextParser instance per slice of a piece iterator,
/// draining around `finish` and reusing the released `StageArea`.
fn collect_pieces<'a, I>(pieces: I, mode: TestMode) -> (Vec<MetricFamily<'a>>, Stats)
where
    I: Iterator<Item = &'a [u8]>,
{
    match mode {
        TestMode::Lenient => collect_inner(pieces, LenientMode),
        TestMode::StrictUtf8 => collect_inner(pieces, StrictMode::utf8()),
        TestMode::StrictLegacy => collect_inner(pieces, StrictMode::legacy()),
    }
}

/// The generic core of [`collect_pieces`], parameterized by mode.
/// Only the last instance sees `finish` — a mid-stream `finish` would fold
/// the staged half-line as an EOF residual, which is the caller's job to
/// trigger exactly once, at the true end of the input.
fn collect_inner<'a, M, I>(pieces: I, mut mode: M) -> (Vec<MetricFamily<'a>>, Stats)
where
    I: Iterator<Item = &'a [u8]>,
    M: prometheus_scrape_parser_rs::Mode,
{
    let mut staging = StageArea::default();
    let mut families = Vec::new();
    let mut stats = Stats::default();
    let mut pieces = pieces.peekable();
    while let Some(piece) = pieces.next() {
        let is_last = pieces.peek().is_none();
        let mut parser = TextParser::new(std::mem::take(&mut staging), mode, piece);
        while let Some(family) = parser.next_family() {
            families.push(family);
        }
        if is_last {
            stats = parser.finish();
            while let Some(family) = parser.next_family() {
                families.push(family);
            }
        }
        let (next, next_mode) = parser.release();
        staging = next;
        mode = next_mode;
    }
    (families, stats)
}

/// Whole-input variant fed in `chunk`-sized slices.
fn collect_chunked<'a>(
    input: &'a [u8],
    chunk: usize,
    mode: TestMode,
) -> (Vec<MetricFamily<'a>>, Stats) {
    collect_pieces(input.chunks(chunk.max(1)), mode)
}

/// Line-group variant: each instance receives whole lines, so no line
/// ever stages across an instance boundary. An aggregation family still
/// gathering its sample group carries across; an unterminated tail
/// becomes the last instance's piece.
fn collect_by_lines<'a>(
    input: &'a [u8],
    lines_per_instance: usize,
    mode: TestMode,
) -> (Vec<MetricFamily<'a>>, Stats) {
    let mut pieces = Vec::new();
    let mut start = 0usize;
    let mut count = 0usize;
    for (i, &b) in input.iter().enumerate() {
        if b != b'\n' {
            continue;
        }
        count += 1;
        if count == lines_per_instance.max(1) {
            pieces.push(&input[start..=i]);
            start = i + 1;
            count = 0;
        }
    }
    if start < input.len() {
        pieces.push(&input[start..]);
    }
    collect_pieces(pieces.into_iter(), mode)
}

/// Stats equality at instance granularity: everything matches the whole-slice
/// run except `pruned_families`, which also counts metadata-only families
/// sealed at an instance boundary (they re-seed from the metadata memory
/// in their continuation instance).
fn assert_stats_matches_reference(got: &Stats, whole: &Stats, what: &str) {
    assert_eq!(got.lines_total, whole.lines_total, "{what}: line count");
    assert_eq!(
        got.skipped_lines, whole.skipped_lines,
        "{what}: skipped line count"
    );
    assert_eq!(
        got.skipped_families, whole.skipped_families,
        "{what}: family skip count"
    );
    assert_eq!(got.last_error, whole.last_error, "{what}: last_error");
}

#[test]
fn text_parser_single_instance_should_equal_whole_slice_exactly() {
    for (name, input) in payloads() {
        for (mode_name, mode) in modes() {
            let (whole, whole_stats) = parse(input, mode);
            let (families, stats) = collect_chunked(input, input.len().max(1), mode);
            assert_eq!(
                families, whole,
                "{mode_name} families differ for {name} in a single instance"
            );
            assert_stats_matches_reference(&stats, &whole_stats, &format!("{mode_name}/{name}"));
            if name != "truncated" {
                // The truncated tail re-folds at `finish`, so its
                // metadata-only prefix family prunes at the mid-parse
                // flush where the whole-slice prune happens only at the end.
                assert_eq!(
                    stats.pruned_families, whole_stats.pruned_families,
                    "{mode_name}/{name}: prune count differs"
                );
            }
        }
    }
}

#[test]
fn text_parser_line_per_line_should_merge_equal_whole_slice_exactly() {
    for (name, input) in payloads() {
        for (mode_name, mode) in modes() {
            let (whole, whole_stats) = parse(input, mode);
            // The flat yield order can diverge from whole-slice creation
            // order: carried families yield from their continuation
            // instance. Compare sorted.
            let whole = sorted_by_name(whole);
            for lines_per_instance in [1, 3] {
                let (families, stats) = collect_by_lines(input, lines_per_instance, mode);
                // Aggregation families a one-line instance leaves
                // gathering their sample group ride whole to the next
                // instance; interleaved rows still fragment and the
                // row-aware merge reassembles them. A lenient skip drops
                // the whole family in the whole-slice run, but the
                // zero-copy instances that yielded before the skip keep
                // their piece — the same exemption the differential
                // carry, so family equality is asserted on clean inputs.
                if stats.skipped_families == 0 {
                    assert_eq!(
                        sorted_by_name(merge_instances_by_name(families)),
                        whole.clone(),
                        "{mode_name} merged families differ for {name} at \
                         {lines_per_instance} lines per instance"
                    );
                }
                assert_stats_matches_reference(
                    &stats,
                    &whole_stats,
                    &format!("{mode_name}/{name}/{lines_per_instance}"),
                );
            }
        }
    }
}

#[test]
fn text_parser_byte_per_byte_should_merge_equal_whole_slice() {
    for (name, input) in payloads() {
        for (mode_name, mode) in modes() {
            let (whole, whole_stats) = parse(input, mode);
            let (families, stats) = collect_chunked(input, 1, mode);
            // Mid-line cuts carry the aggregation family they extend,
            // which yields from its continuation instance: the flat yield
            // order can diverge from whole-slice creation order. As everywhere,
            // a lenient skip exempts the family comparison — pieces that
            // yielded before the skip cannot be retracted.
            if stats.skipped_families == 0 {
                assert_eq!(
                    sorted_by_name(merge_instances_by_name(families)),
                    sorted_by_name(whole),
                    "{mode_name} merged families differ for {name} byte per byte"
                );
            }
            assert_stats_matches_reference(&stats, &whole_stats, &format!("{mode_name}/{name}/1"));
        }
    }
}

#[test]
fn text_parser_word_boundary_lengths_should_lex_exact() {
    // Lengths straddling the word and tail boundaries of the block scanners
    // (line split, label-value scan, token-to-whitespace): every input must
    // decode to the exact expected model, per mode.
    for len in [6, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65] {
        let expected = "v".repeat(len);
        let verbatim = format!("m{{l=\"{expected}\"}} 1\n");
        let escaped_value = format!("{}\nbb", "a".repeat(len - 1));
        let escaped = format!("m{{l=\"{}\\nbb\"}} 1\n", "a".repeat(len - 1));
        for (mode_name, mode) in modes() {
            let (families, stats) = parse(verbatim.as_bytes(), mode);
            assert_eq!(families.len(), 1, "{mode_name}/{len}: verbatim families");
            assert_eq!(families[0].name, "m", "{mode_name}/{len}: verbatim name");
            assert_eq!(
                families[0].metrics.len(),
                1,
                "{mode_name}/{len}: verbatim rows"
            );
            assert_eq!(families[0].metrics[0].labels.len(), 1);
            assert_eq!(families[0].metrics[0].labels[0].name, "l");
            assert_eq!(
                families[0].metrics[0].labels[0].value, expected,
                "{mode_name}/{len}: verbatim label value"
            );
            assert_eq!(stats.lines_total, 1);
            assert_eq!(stats.last_error, None);

            let (families, _) = parse(escaped.as_bytes(), mode);
            assert_eq!(
                families[0].metrics[0].labels[0].value, escaped_value,
                "{mode_name}/{len}: escaped label value"
            );
        }
    }
    for len in [6, 7, 8, 9, 15, 16, 17, 31, 32, 33] {
        let literal = format!("0.{}e+09", "1".repeat(len));
        let input = format!("m {literal}\n");
        let expected: f64 = literal.parse().unwrap();
        for (mode_name, mode) in modes() {
            let (families, stats) = parse(input.as_bytes(), mode);
            assert_eq!(
                families[0].metrics[0].value,
                MetricValue::Sample(expected),
                "{mode_name}/{len}: boundary float token"
            );
            assert_eq!(stats.last_error, None);
        }
    }
}

#[test]
fn text_parser_histogram_spanning_instances_should_reassemble_whole() {
    let input = mid_row_cut_histogram();
    for (mode_name, mode) in modes() {
        let (whole, whole_stats) = parse(&input, mode);
        let expected = sorted_by_name(whole);
        for chunk in [64, 997] {
            let (families, stats) = collect_chunked(&input, chunk, mode);
            assert_eq!(
                sorted_by_name(merge_instances_by_name(families)),
                expected,
                "{mode_name} merged families differ at chunk {chunk}"
            );
            assert_stats_matches_reference(&stats, &whole_stats, &format!("{mode_name}/{chunk}"));
        }
    }
}

#[test]
fn text_parser_scattered_histogram_should_merge_across_instances() {
    let input = scattered_histogram();
    for (mode_name, mode) in modes() {
        let (whole, whole_stats) = parse(&input, mode);
        let expected = sorted_by_name(whole);
        for chunk in [256, 2048] {
            let (families, stats) = collect_chunked(&input, chunk, mode);
            // The padding family interleaves plain lines between the
            // histogram rows: a boundary after a padding line flushes the
            // family, and its continuation rows fragment — the merge
            // folds them back together.
            assert_eq!(
                sorted_by_name(merge_instances_by_name(families)),
                expected,
                "{mode_name} merged families differ at chunk {chunk}"
            );
            assert_stats_matches_reference(&stats, &whole_stats, &format!("{mode_name}/{chunk}"));
        }
    }
}

#[test]
fn text_parser_metadata_should_replay_across_instances() {
    // The TYPE line lives in the first instance, the bucket lines two
    // instances later: the family must fold into a typed `h`, never into
    // an untyped `h_bucket` family.
    let input: &[u8] = b"# TYPE h histogram\np 1\nh_bucket{le=\"+Inf\"} 1\nh_sum 1\nh_count 1\n";
    for (mode_name, mode) in modes() {
        let (whole, _) = parse(input, mode);
        let (families, _) = collect_chunked(input, 11, mode);
        let merged = sorted_by_name(merge_instances_by_name(families));
        assert_eq!(merged, sorted_by_name(whole), "{mode_name} differs");
        assert!(
            merged.iter().all(|f| f.name.as_ref() != "h_bucket"),
            "bucket lines leaked into an untyped h_bucket family"
        );
        let h = merged.iter().find(|f| f.name.as_ref() == "h").unwrap();
        assert_eq!(h.metric_type, MetricType::Histogram);
    }
}

#[test]
fn text_parser_carried_prefix_should_join_borrowed_suffix() {
    // The cut falls inside the h_sum line: the open histogram family
    // carries its staged rows over as owned text and completes from lines
    // the second instance borrows, so nothing yields from the first
    // instance and every row ends up whole in one family.
    let input: &[u8] = b"# TYPE h histogram\n\
         h_bucket{handler=\"/a\",le=\"+Inf\"} 1\n\
         h_sum{handler=\"/a\"} 1.5\n\
         h_count{handler=\"/a\"} 1\n\
         h_bucket{handler=\"/b\",le=\"+Inf\"} 2\n";
    let mut staging = StageArea::default();

    let mut carried_through =
        TextParser::new(std::mem::take(&mut staging), LenientMode, &input[..68]);
    let mut families = Vec::new();
    while let Some(family) = carried_through.next_family() {
        families.push(family);
    }
    assert!(
        families.is_empty(),
        "the open histogram family must carry over, not yield"
    );
    let (staging, mode) = carried_through.release();

    let mut parser = TextParser::new(staging, mode, &input[68..]);
    while let Some(family) = parser.next_family() {
        families.push(family);
    }
    parser.finish();
    while let Some(family) = parser.next_family() {
        families.push(family);
    }
    let (whole, _) = parse(input, TestMode::Lenient);
    assert_eq!(
        sorted_by_name(merge_instances_by_name(families)),
        sorted_by_name(whole),
        "carried prefix and borrowed suffix must reassemble the whole-slice rows"
    );
}

#[test]
fn text_parser_clean_boundary_should_carry_an_incomplete_sample_group() {
    // The boundary falls on the newline after the bucket row: the row
    // still gathers its `_sum`/`_count`, the family rides the StageArea
    // whole, and the continuation instance completes it from borrowed
    // lines.
    let input: &[u8] = b"# TYPE h histogram\n\
         h_bucket{handler=\"/a\",le=\"+Inf\"} 1\n\
         h_sum{handler=\"/a\"} 1.5\n\
         h_count{handler=\"/a\"} 1\n";
    let mut staging = StageArea::default();

    let mut first = TextParser::new(std::mem::take(&mut staging), LenientMode, &input[..54]);
    let mut families = Vec::new();
    while let Some(family) = first.next_family() {
        families.push(family);
    }
    assert!(
        families.is_empty(),
        "the incomplete sample group must carry over, not yield"
    );
    let (staging, mode) = first.release();

    let mut parser = TextParser::new(staging, mode, &input[54..]);
    while let Some(family) = parser.next_family() {
        families.push(family);
    }
    parser.finish();
    while let Some(family) = parser.next_family() {
        families.push(family);
    }

    assert_eq!(families.len(), 1);
    let MetricValue::Histogram(histogram) = &families[0].metrics[0].value else {
        panic!("the family must hold the histogram");
    };
    assert_eq!(histogram.sample_count, Some(IntOrFloat::Int(1)));
    assert_eq!(histogram.sample_sum, Some(1.5));
    assert_eq!(histogram.buckets.len(), 1);
}

#[test]
fn text_parser_probe_names_the_family_from_the_cut_head() {
    // The cut falls inside h_bucket's label list right after `le=`: the
    // probe names the family from the line head (h_bucket resolves to the
    // typed h), so h carries even though the last folded line was plain g.
    // Under a full-prefix probe this cut fails to lex, the last_slot
    // fallback fires, nothing carries, and h yields from the first
    // instance — the head rule is what keeps re-probing a growing staged
    // line linear.
    let input: &[u8] = b"# TYPE h histogram\n\
         g 5\n\
         h_bucket{le=\"1\"} 1\n\
         h_bucket{le=\"+Inf\"} 2\n\
         h_sum 3\n\
         h_count 4\n";
    let mut staging = StageArea::default();

    let mut first = TextParser::new(std::mem::take(&mut staging), LenientMode, &input[..34]);
    let mut families = Vec::new();
    while let Some(family) = first.next_family() {
        families.push(family);
    }
    assert_eq!(
        families.len(),
        1,
        "g yields; the typed h must carry into the continuation instance"
    );
    assert_eq!(families[0].name, "g");
    let (staging, mode) = first.release();

    let mut parser = TextParser::new(staging, mode, &input[34..]);
    while let Some(family) = parser.next_family() {
        families.push(family);
    }
    let stats = parser.finish();
    while let Some(family) = parser.next_family() {
        families.push(family);
    }
    assert_eq!(stats.lines_total, 6);
    let (whole, _) = parse(input, TestMode::Lenient);
    assert_eq!(
        sorted_by_name(merge_instances_by_name(families)),
        sorted_by_name(whole),
        "the carried histogram must reassemble the whole-slice rows"
    );
}

#[test]
fn text_parser_plain_family_should_split_and_merge_by_name() {
    // A counter family cut across instances yields per-instance pieces
    // (no carry for plain types); merging by name reconstructs it, and
    // the early instance's families stay valid while later ones parse.
    let input: &[u8] = b"c 1\nc 2\nc 3\nc 4\n";
    let mut staging = StageArea::default();
    let mut parser = TextParser::new(std::mem::take(&mut staging), LenientMode, &input[..4]);
    let mut early = Vec::new();
    while let Some(family) = parser.next_family() {
        early.push(family);
    }
    let (staging, mode) = parser.release();
    assert_eq!(early.len(), 1, "the first instance yields its own piece");
    assert_eq!(early[0].metrics.len(), 1);

    let mut parser = TextParser::new(staging, mode, &input[4..]);
    let mut late = Vec::new();
    while let Some(family) = parser.next_family() {
        late.push(family);
    }
    let stats = parser.finish();
    while let Some(family) = parser.next_family() {
        late.push(family);
    }
    assert_eq!(stats.lines_total, 4);
    let merged = merge_by_name([early, late].concat());
    assert_eq!(merged.len(), 1, "both pieces are family c");
    assert_eq!(merged[0].metrics.len(), 4);
}

#[test]
fn text_parser_truncated_tail_should_error_at_finish_in_lenient() {
    let input: &[u8] = b"# TYPE t counter\nt 1";
    let (whole, whole_stats) = parse(input, TestMode::Lenient);
    let (families, stats) = collect_chunked(input, 7, TestMode::Lenient);
    assert_eq!(
        sorted_by_name(merge_instances_by_name(families)),
        sorted_by_name(whole),
        "the unterminated tail must fold at finish"
    );
    assert_eq!(stats.last_error, whole_stats.last_error);
    let err = whole_stats.last_error.as_ref().unwrap();
    assert_eq!(err.line, 2);
    assert!(matches!(err.kind, ErrorKind::UnexpectedEof));
    assert_eq!(stats.lines_total, whole_stats.lines_total);
}

#[test]
fn text_parser_strict_should_reject_partial_last_line_with_global_line_no() {
    // The truncated final line errors in strict mode, and the reported
    // line number counts from the stream's first instance, not the
    // instance that saw the tail.
    let input: &[u8] = b"# TYPE t counter\nt 1";
    let (_, whole_stats) = parse(input, TestMode::StrictUtf8);
    assert!(
        whole_stats.last_error.is_some(),
        "the whole-slice run must error first"
    );
    let pieces = [&input[..14], &input[14..]];
    let (_, stats) = collect_pieces(pieces.iter().copied(), TestMode::StrictUtf8);
    assert_eq!(
        stats.last_error, whole_stats.last_error,
        "the EOF error and its stream-global line number must match the whole-slice run"
    );
}

#[test]
fn text_parser_strict_residual_line_own_error_should_not_be_overridden_by_eof_error() {
    // A truncated final line that itself fails in strict mode reports its
    // own error, not the truncation one.
    let input: &[u8] = b"# TYPE h histogram\nh_count -5";
    let (whole, whole_stats) = parse(input, TestMode::StrictUtf8);
    let err = whole_stats.last_error.as_ref().unwrap();
    assert!(matches!(err.kind, ErrorKind::NegativeHistogramCount { .. }));
    assert_eq!(err.line, 2);
    assert_eq!(whole.len(), 1);

    let pieces = [&input[..14], &input[14..]];
    let (families, stats) = collect_pieces(pieces.iter().copied(), TestMode::StrictUtf8);
    assert_eq!(families, whole);
    assert_eq!(stats.last_error, whole_stats.last_error);
    assert_eq!(stats.lines_total, whole_stats.lines_total);
}

#[test]
fn text_parser_strict_abort_should_yield_staged_and_stay_inert() {
    for (mode_name, mode) in [
        ("strict-utf8", TestMode::StrictUtf8),
        ("strict-legacy", TestMode::StrictLegacy),
    ] {
        let (whole, whole_stats) = parse(SKIPPED_LABELED, mode);
        let (families, stats) = collect_chunked(SKIPPED_LABELED, 5, mode);
        assert_eq!(
            families, whole,
            "{mode_name} families differ across the abort"
        );
        assert_stats_matches_reference(&stats, &whole_stats, mode_name);
        assert_eq!(
            stats.pruned_families, whole_stats.pruned_families,
            "{mode_name}: prune count differs"
        );
    }
}
