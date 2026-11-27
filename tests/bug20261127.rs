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

//! Bug 20261127: the [`Parser`] facade must land what the chained
//! [`TextParser`] lands. A chunk that completes no line while an
//! aggregation family is in transit — a name prefix, a blank or a comment
//! head, an empty chunk — made the facade yield the family's pieces as
//! fragments and fold the last piece away at `reset`; merging the chain's
//! pieces rebuilds the whole row, merging the facade's cannot. Every
//! chunking of one exposition must land the same families.

mod common;

use common::{TestMode, merge_instances_by_name, modes, rows_whole};
use prometheus_scrape_parser_rs::{
    LenientMode, MetricFamily, Mode, Parser, StageArea, Stats, StrictMode, TextParser,
};

/// One histogram row spanning several lines, the shape a scrape serves.
const HISTOGRAM: &[u8] = b"\
# TYPE request_seconds histogram
request_seconds_bucket{le=\"0.5\"} 1
request_seconds_bucket{le=\"+Inf\"} 3
request_seconds_count 3
request_seconds_sum 1.5
";

/// The same row with a NaN `le` line folded into it.
const HISTOGRAM_NAN_LE: &[u8] = b"\
# TYPE request_seconds histogram
request_seconds_bucket{le=\"NaN\"} 1
request_seconds_bucket{le=\"+Inf\"} 3
request_seconds_count 3
request_seconds_sum 1.5
";

/// A blank line and a comment between the rows: a chunk ending inside
/// either completes no line too.
const HISTOGRAM_WITH_GAPS: &[u8] = b"\
# HELP request_seconds Request latency.
# TYPE request_seconds histogram
request_seconds_bucket{le=\"0.5\"} 1

# a comment
request_seconds_bucket{le=\"+Inf\"} 3
request_seconds_count 3
request_seconds_sum 1.5
";

/// One row per handler: the family carries several rows at once.
const HISTOGRAM_ROWS: &[u8] = b"\
# TYPE request_seconds histogram
request_seconds_bucket{handler=\"/a\",le=\"+Inf\"} 1
request_seconds_sum{handler=\"/a\"} 1.5
request_seconds_count{handler=\"/a\"} 1
request_seconds_bucket{handler=\"/b\",le=\"+Inf\"} 2
request_seconds_sum{handler=\"/b\"} 2.5
request_seconds_count{handler=\"/b\"} 2
";

/// A summary row, the other aggregation kind.
const SUMMARY: &[u8] = b"\
# TYPE request_seconds summary
request_seconds{quantile=\"0.5\"} 1
request_seconds{quantile=\"0.9\"} 2
request_seconds_count 2
request_seconds_sum 3
";

fn expositions() -> Vec<(&'static str, &'static [u8])> {
    vec![
        ("histogram", HISTOGRAM),
        ("histogram_nan_le", HISTOGRAM_NAN_LE),
        ("histogram_with_gaps", HISTOGRAM_WITH_GAPS),
        ("histogram_rows", HISTOGRAM_ROWS),
        ("summary", SUMMARY),
    ]
}

/// The facade the way a scraper drives it: one `parse_text` call per
/// chunk, `reset` ending the stream.
fn facade<'a, M: Mode>(pieces: &[&'a [u8]], mode: M) -> (Vec<MetricFamily<'a>>, Stats) {
    let mut parser = Parser::new(mode);
    let mut families = Vec::new();
    for &piece in pieces {
        families.extend(parser.parse_text(piece));
    }
    assert_eq!(parser.reset(), Ok(()), "the stream ends cleanly");
    (families, parser.stats().clone())
}

/// The chained [`TextParser`] instances, `finish` on the last one.
fn chain<'a, M: Mode>(pieces: &[&'a [u8]], mut mode: M) -> (Vec<MetricFamily<'a>>, Stats) {
    let mut staging = StageArea::default();
    let mut families = Vec::new();
    let mut stats = Stats::default();
    let mut pieces = pieces.iter().peekable();
    while let Some(&piece) = pieces.next() {
        let mut parser = TextParser::new(std::mem::take(&mut staging), mode, piece);
        while let Some(family) = parser.next_family() {
            families.push(family);
        }
        if pieces.peek().is_none() {
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

fn facade_in<'a>(pieces: &[&'a [u8]], mode: TestMode) -> (Vec<MetricFamily<'a>>, Stats) {
    match mode {
        TestMode::Lenient => facade(pieces, LenientMode),
        TestMode::StrictUtf8 => facade(pieces, StrictMode::utf8()),
        TestMode::StrictLegacy => facade(pieces, StrictMode::legacy()),
    }
}

fn chain_in<'a>(pieces: &[&'a [u8]], mode: TestMode) -> (Vec<MetricFamily<'a>>, Stats) {
    match mode {
        TestMode::Lenient => chain(pieces, LenientMode),
        TestMode::StrictUtf8 => chain(pieces, StrictMode::utf8()),
        TestMode::StrictLegacy => chain(pieces, StrictMode::legacy()),
    }
}

/// The facade lands what the chain lands: the same families once the
/// per-instance pieces merge by name, every aggregation row yielded
/// whole, the same rows counted, nothing folded away at `reset`.
fn assert_facade_matches_chain(pieces: &[&[u8]], mode: TestMode, what: &str) {
    let (facade_families, facade_stats) = facade_in(pieces, mode);
    let (chain_families, chain_stats) = chain_in(pieces, mode);

    assert_eq!(
        merge_instances_by_name(facade_families.clone()),
        merge_instances_by_name(chain_families),
        "{what}: the facade and the chain must land the same families"
    );
    assert!(
        rows_whole(&facade_families),
        "{what}: the facade yielded half a sample group"
    );
    assert_eq!(
        facade_stats.metrics_total, chain_stats.metrics_total,
        "{what}: the facade must count the rows the chain counts"
    );
    assert_eq!(
        facade_stats.discarded_families, 0,
        "{what}: reset folded a family away"
    );
}

/// Offsets of every line start of `input`.
fn line_starts(input: &[u8]) -> Vec<usize> {
    std::iter::once(0)
        .chain(
            (0..input.len())
                .filter(|&i| input[i] == b'\n')
                .map(|i| i + 1),
        )
        .collect()
}

// ======================== chunkings that already agree =====================

#[test]
fn parser_text_whole_buffer_should_match_the_chained_text_parser() {
    for (name, payload) in expositions() {
        for (mode_name, mode) in modes() {
            assert_facade_matches_chain(&[payload], mode, &format!("{mode_name}/{name}"));
        }
    }
}

#[test]
fn parser_text_every_two_chunk_split_should_match_the_chained_text_parser() {
    for (name, payload) in expositions() {
        for cut in 0..=payload.len() {
            for (mode_name, mode) in modes() {
                assert_facade_matches_chain(
                    &[&payload[..cut], &payload[cut..]],
                    mode,
                    &format!("{mode_name}/{name}/cut {cut}"),
                );
            }
        }
    }
}

// ======================== a chunk that completes no line ===================

#[test]
fn parser_text_line_per_chunk_should_match_the_chained_text_parser() {
    // A blank or comment line folds nothing: the family in transit must
    // ride through its chunk.
    for (name, payload) in expositions() {
        let pieces: Vec<&[u8]> = payload.split_inclusive(|&b| b == b'\n').collect();
        for (mode_name, mode) in modes() {
            assert_facade_matches_chain(&pieces, mode, &format!("{mode_name}/{name}"));
        }
    }
}

#[test]
fn parser_text_every_three_chunk_cut_on_a_line_start_should_match_the_chained_text_parser() {
    // The middle chunk starts a line and may stop anywhere inside it: a
    // name prefix, a blank, a comment head, or nothing at all.
    for (name, payload) in expositions() {
        for start in line_starts(payload) {
            let line_len = payload[start..]
                .iter()
                .position(|&b| b == b'\n')
                .map_or(payload.len() - start, |i| i + 1);
            for middle in 0..=line_len {
                for (mode_name, mode) in modes() {
                    assert_facade_matches_chain(
                        &[
                            &payload[..start],
                            &payload[start..start + middle],
                            &payload[start + middle..],
                        ],
                        mode,
                        &format!("{mode_name}/{name}/line {start}/middle {middle}"),
                    );
                }
            }
        }
    }
}

#[test]
fn parser_text_byte_per_byte_chunks_should_match_the_chained_text_parser() {
    for (name, payload) in expositions() {
        let pieces: Vec<&[u8]> = payload.chunks(1).collect();
        for (mode_name, mode) in modes() {
            assert_facade_matches_chain(&pieces, mode, &format!("{mode_name}/{name}"));
        }
    }
}

#[test]
fn parser_text_small_fixed_chunks_should_match_the_chained_text_parser() {
    // Sizes up to the longest line: small enough for a chunk to fall
    // entirely inside a line.
    for (name, payload) in expositions() {
        let longest = payload
            .split_inclusive(|&b| b == b'\n')
            .map(|line| line.len())
            .max()
            .unwrap_or(1);
        for size in 1..=longest {
            let pieces: Vec<&[u8]> = payload.chunks(size).collect();
            for (mode_name, mode) in modes() {
                assert_facade_matches_chain(
                    &pieces,
                    mode,
                    &format!("{mode_name}/{name}/{size} bytes"),
                );
            }
        }
    }
}
