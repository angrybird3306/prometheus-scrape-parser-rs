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
//! tolerates, plus the SkipFamily disposition. Where a Go error case has a
//! direct counterpart, the test name carries its number like the success
//! scenarios do. The superset property test pins lenient output to strict
//! output wherever strict parsing succeeds.

mod common;

use common::scenarios::scenario_inputs;
use common::{TestMode, parse};
use prometheus_scrape_parser_rs::{Bucket, ErrorKind, HistogramValue, IntOrFloat, MetricValue};

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
// with "unexpected end of input stream"; LenientMode accepts the truncated
// last line.
#[test]
fn lenient_error_case_0_no_new_line_at_end_of_input_should_pass() {
    let (got, _) = parse(b"metric_a 1", TestMode::Lenient);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].name, "metric_a");
    assert_eq!(got[0].metrics[0].value, MetricValue::Sample(1.0));
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
    assert_eq!(stats.skipped_families, 2);
}

// SkipFamily disposition: the erroring line drops the family it belongs to
// (the `bad` family does not exist yet, so nothing is dropped), parsing
// resumes on the next line, and same-name lines merge into one family with
// the failure recorded in Stats.
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
    assert_eq!(stats.skipped_families, 1);
    let err = stats.last_error.as_ref().unwrap();
    assert_eq!(err.line, 2);
    assert!(matches!(err.kind, ErrorKind::ExpectedFloatValue { .. }));
}

// An error line whose family already holds data drops exactly that family;
// unrelated families survive.
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
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].name, "a");
    assert_eq!(got[0].metrics.len(), 1);
    assert_eq!(got[1].name, "c");
    assert_eq!(got[1].metrics.len(), 1);
    assert_eq!(stats.skipped_lines, 1);
    assert_eq!(stats.skipped_families, 1);
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
