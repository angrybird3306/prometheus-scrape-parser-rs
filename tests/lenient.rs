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

//! LenientMode smoke tests: Go error scenarios that lenient parsing
//! tolerates, plus the line-level skip disposition. Where a Go error case
//! has a direct counterpart, the test name carries its number like the
//! success scenarios do. The superset property test pins lenient output to
//! strict output wherever strict parsing succeeds.

mod common;

use common::scenarios::{ERROR_SCENARIO_0_INPUT, scenario_inputs};
use common::{TestMode, parse};
use prometheus_scrape_parser_rs::{
    Bucket, ErrorKind, HistogramValue, IntOrFloat, MetricType, MetricValue,
};

// Empty input yields no families.
#[test]
fn lenient_on_empty_input_should_pass() {
    let (got, _) = parse(b"", TestMode::Lenient);
    assert!(got.is_empty());
}

// The superset property: whenever a strict scheme parses the whole input
// cleanly, the lenient parser must agree with it exactly (families and
// stats). Inputs where strict aborts carry no obligation and are skipped.
#[test]
fn strict_parse_on_scenarios_should_match_lenient_when_strict_passes() {
    for (label, input) in scenario_inputs() {
        for mode in [TestMode::StrictUtf8, TestMode::StrictLegacy] {
            let (strict_families, strict_stats) = parse(input, mode);
            if strict_stats.last_error.is_some() {
                continue;
            }
            let (lenient_families, lenient_stats) = parse(input, TestMode::Lenient);
            assert_eq!(
                lenient_families, strict_families,
                "families differ on {label}"
            );
            assert_eq!(lenient_stats, strict_stats, "stats differ on {label}");
        }
    }
}

// Go error scenario "0: No new-line at end of input." fails strict parsing
// with "unexpected end of input stream"; lenient parsing records the same
// truncation instead of silently accepting a cut-off input.
#[test]
fn lenient_error_case_0_no_new_line_at_end_of_input_should_record_truncation_error() {
    let (got, stats) = parse(b"metric_a 1", TestMode::Lenient);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].name, "metric_a");
    assert_eq!(got[0].metrics[0].value, MetricValue::Sample(1.0));
    let err = stats.last_error.as_ref().unwrap();
    assert_eq!(err.line, 1);
    assert!(matches!(err.kind, ErrorKind::UnexpectedEof));
}

// The full scenario-0 input: the truncation reports at its own line number,
// after both complete families have folded.
#[test]
fn lenient_error_case_0_truncated_after_complete_family_should_record_truncation_error() {
    let (got, stats) = parse(ERROR_SCENARIO_0_INPUT.as_bytes(), TestMode::Lenient);
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].name, "bla");
    assert_eq!(got[0].metrics[0].value, MetricValue::Sample(3.14));
    assert_eq!(got[1].name, "blubber");
    assert_eq!(got[1].metrics[0].value, MetricValue::Sample(42.0));
    let err = stats.last_error.as_ref().unwrap();
    assert_eq!(err.line, 3);
    assert!(matches!(err.kind, ErrorKind::UnexpectedEof));
}

// A truncated TYPE+sample tail records the truncation without dropping the
// cleanly folded content.
#[test]
fn lenient_on_truncated_final_sample_should_record_truncation_error() {
    let (got, stats) = parse(b"# TYPE t counter\nt 1", TestMode::Lenient);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].name, "t");
    assert_eq!(got[0].metrics.len(), 1);
    assert_eq!(got[0].metrics[0].value, MetricValue::Sample(1.0));
    assert_eq!(stats.lines_total, 2);
    assert_eq!(stats.skipped_lines, 0);
    let err = stats.last_error.as_ref().unwrap();
    assert_eq!(err.line, 2);
    assert!(matches!(err.kind, ErrorKind::UnexpectedEof));
}

// A truncated comment tail is a non-blank partial line: it reports the
// truncation too.
#[test]
fn lenient_on_truncated_final_comment_should_record_truncation_error() {
    let (got, stats) = parse(b"m 1\n# c", TestMode::Lenient);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].name, "m");
    assert_eq!(got[0].metrics.len(), 1);
    let err = stats.last_error.as_ref().unwrap();
    assert_eq!(err.line, 2);
    assert!(matches!(err.kind, ErrorKind::UnexpectedEof));
}

// A blank-only unterminated tail ends cleanly in every mode: EOF at the
// start of a line is not an error (mirroring Go).
#[test]
fn blank_only_truncated_tail_should_end_cleanly_in_every_mode() {
    for input in [&b"m 1\n  "[..], &b"m 1\n\t"[..], &b"m 1\n\n"[..]] {
        for mode in [
            TestMode::Lenient,
            TestMode::StrictUtf8,
            TestMode::StrictLegacy,
        ] {
            let (_, stats) = parse(input, mode);
            assert!(stats.last_error.is_none(), "blank tail must not error");
        }
    }
}

// Fractional timestamps truncate to whole milliseconds.
#[test]
fn lenient_on_decimal_timestamp_should_pass() {
    let (got, _) = parse(b"m 1 1.5\n", TestMode::Lenient);
    assert_eq!(got[0].metrics[0].timestamp, Some(1));
    let (got, _) = parse(b"m 1 1500.9\n", TestMode::Lenient);
    assert_eq!(got[0].metrics[0].timestamp, Some(1500));
}

// A skipped line must not leak the labels it already staged into the next
// successful sample (found by the fuzz differential).
#[test]
fn lenient_on_skipped_labeled_line_should_not_leak_labels() {
    let (got, stats) = parse(
        b"broken{a=\"1\"} xx
broken{c=\"3\"} 1 2 3
good 1
good{b=\"2\"} 2
",
        TestMode::Lenient,
    );
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].name, "good");
    assert_eq!(got[0].metrics.len(), 2);
    assert!(got[0].metrics[0].labels.is_empty());
    assert_eq!(got[0].metrics[0].value, MetricValue::Sample(1.0));
    assert_eq!(got[0].metrics[1].labels.len(), 1);
    assert_eq!(got[0].metrics[1].labels[0].name, "b");
    assert_eq!(stats.lines_total, 4);
    assert_eq!(stats.skipped_lines, 2);
    assert_eq!(stats.skipped_families, 0);
}

// A line whose value does not parse drops itself; parsing resumes on the
// next line, and same-name lines merge into one family with the failure
// recorded in Stats.
#[test]
fn lenient_on_invalid_float_line_should_skip_family() {
    let (got, stats) = parse(
        b"good 1
bad xx
good 2
",
        TestMode::Lenient,
    );
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].name, "good");
    assert_eq!(got[0].metrics.len(), 2);
    assert_eq!(got[0].metrics[0].value, MetricValue::Sample(1.0));
    assert_eq!(got[0].metrics[1].value, MetricValue::Sample(2.0));
    assert_eq!(stats.lines_total, 3);
    assert_eq!(stats.skipped_lines, 1);
    assert_eq!(stats.skipped_families, 0);
    let err = stats.last_error.as_ref().unwrap();
    assert_eq!(err.line, 2);
    assert!(matches!(err.kind, ErrorKind::ExpectedFloatValue { .. }));
}

// An error line whose family already holds data drops only the line —
// the family keeps its good samples, and unrelated families survive.
#[test]
fn lenient_on_error_line_should_drop_only_its_own_family() {
    let (got, stats) = parse(
        b"a 1
b 2
b @@
c 3
",
        TestMode::Lenient,
    );
    assert_eq!(got.len(), 3);
    assert_eq!(got[0].name, "a");
    assert_eq!(got[0].metrics.len(), 1);
    assert_eq!(got[1].name, "b");
    assert_eq!(got[1].metrics.len(), 1);
    assert_eq!(got[2].name, "c");
    assert_eq!(got[2].metrics.len(), 1);
    assert_eq!(stats.skipped_lines, 1);
    assert_eq!(stats.skipped_families, 0);
    let err = stats.last_error.as_ref().unwrap();
    assert_eq!(err.line, 3);
}

// An error line whose metric name never resolves drops no family at all.
#[test]
fn lenient_on_error_line_with_unknown_name_should_drop_no_family() {
    let (got, stats) = parse(
        b"a 1
@@
c 3
",
        TestMode::Lenient,
    );
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].name, "a");
    assert_eq!(got[1].name, "c");
    assert_eq!(stats.skipped_lines, 1);
    assert_eq!(stats.skipped_families, 0);
    assert_eq!(stats.last_error.as_ref().unwrap().line, 2);
}

// A help-only family is pruned on flush, like Go's end-of-parse deletion of
// families without samples.
#[test]
fn lenient_on_help_only_family_should_prune_it() {
    let (got, stats) = parse(b"# HELP orphan some help\n", TestMode::Lenient);
    assert!(got.is_empty());
    assert_eq!(stats.pruned_families, 1);
}

// A negative histogram count is rejected in both modes: lenient drops just
// the offending line, keeps the family, and records the error.
#[test]
fn lenient_on_negative_histogram_count_should_drop_only_the_line() {
    let (got, stats) = parse(
        b"# TYPE h histogram
h_count -5
h_bucket{le=\"+Inf\"} 7
",
        TestMode::Lenient,
    );
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].name, "h");
    assert_eq!(got[0].metrics.len(), 1);
    assert_eq!(
        got[0].metrics[0].value,
        MetricValue::Histogram(HistogramValue {
            sample_count: None,
            sample_sum: None,
            buckets: vec![Bucket {
                upper_bound: f64::INFINITY,
                cumulative_count: IntOrFloat::Int(7),
            }],
        })
    );
    assert_eq!(stats.lines_total, 3);
    assert_eq!(stats.skipped_lines, 1);
    assert_eq!(stats.skipped_families, 0);
    let err = stats.last_error.as_ref().unwrap();
    assert_eq!(err.line, 2);
    assert!(matches!(err.kind, ErrorKind::NegativeHistogramCount { .. }));
}

// ==================== the disposition is line-level =========================
// Lenient rejects a bad line, not its family: a lex error in a line whose
// family already holds good samples must not take those samples down with
// it — the family, and its TYPE, survive.

#[test]
fn lenient_on_bad_value_should_drop_only_the_line_and_keep_the_family() {
    let (got, stats) = parse(b"# TYPE m gauge\nm 1\nm xx\nm 2\n", TestMode::Lenient);
    assert_eq!(stats.skipped_lines, 1);
    assert_eq!(stats.skipped_families, 0);
    let err = stats.last_error.as_ref().unwrap();
    assert_eq!(err.line, 3);
    assert!(
        matches!(err.kind, ErrorKind::ExpectedFloatValue { ref got } if got == "xx"),
        "unexpected error kind {err}"
    );
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].name, "m");
    assert_eq!(got[0].metric_type, MetricType::Gauge);
    assert_eq!(got[0].metrics.len(), 2);
    assert_eq!(got[0].metrics[0].value, MetricValue::Sample(1.0));
    assert_eq!(got[0].metrics[1].value, MetricValue::Sample(2.0));
}

#[test]
fn lenient_on_non_utf8_timestamp_should_drop_only_the_line_and_keep_the_family() {
    let (got, stats) = parse(b"m 1\nm 1 \xff\nm 2\n", TestMode::Lenient);
    assert_eq!(stats.skipped_lines, 1);
    assert_eq!(stats.skipped_families, 0);
    let err = stats.last_error.as_ref().unwrap();
    assert_eq!(err.line, 2);
    assert!(matches!(err.kind, ErrorKind::ExpectedTimestamp { .. }));
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].name, "m");
    assert_eq!(got[0].metrics.len(), 2);
    assert_eq!(got[0].metrics[0].value, MetricValue::Sample(1.0));
    assert_eq!(got[0].metrics[1].value, MetricValue::Sample(2.0));
}

#[test]
fn lenient_on_spurious_after_timestamp_should_drop_only_the_line_and_keep_the_family() {
    let (got, stats) = parse(b"m 1 5\nm 1 5  \n", TestMode::Lenient);
    assert_eq!(stats.skipped_lines, 1);
    assert_eq!(stats.skipped_families, 0);
    let err = stats.last_error.as_ref().unwrap();
    assert_eq!(err.line, 2);
    assert!(matches!(err.kind, ErrorKind::SpuriousAfterTimestamp { .. }));
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].name, "m");
    assert_eq!(got[0].metrics.len(), 1);
    assert_eq!(got[0].metrics[0].value, MetricValue::Sample(1.0));
    assert_eq!(got[0].metrics[0].timestamp, Some(5));
}
