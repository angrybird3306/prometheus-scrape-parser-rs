//! Scenarios ported from prometheus/common `expfmt/text_parse_test.go`.
//! Test names carry the Go table's scenario number and description so cases
//! can be cross-referenced one-to-one.
//!
//! Success scenarios run under `StrictMode::utf8()` only, mirroring Go's
//! `testTextParse` (which only uses the UTF8 parser). Error scenarios run
//! each input under both schemes, mirroring `testTextParseError`'s
//! errUTF8/errLegacy double run.
//!
//! Family order in expected vectors is creation order (the order each name
//! is first mentioned), which is deterministic; Go compares its map output
//! by name so its array order is arbitrary.

use prometheus_scrape_parser_rs::{
    Bucket, ErrorKind, HistogramValue, IntOrFloat, Label, Metric, MetricFamily, MetricType,
    MetricValue, Quantile, Scheme, SummaryValue,
};
use std::borrow::Cow;

mod common;

use common::scenarios::{
    ERROR_SCENARIO_0_INPUT, ERROR_SCENARIO_1_INPUT, ERROR_SCENARIO_2_INPUT, ERROR_SCENARIO_3_INPUT,
    ERROR_SCENARIO_4_INPUT, ERROR_SCENARIO_5_INPUT, ERROR_SCENARIO_6_INPUT, ERROR_SCENARIO_7_INPUT,
    ERROR_SCENARIO_8_INPUT, ERROR_SCENARIO_9_INPUT, ERROR_SCENARIO_10_INPUT,
    ERROR_SCENARIO_11_INPUT, ERROR_SCENARIO_12_INPUT, ERROR_SCENARIO_13_INPUT,
    ERROR_SCENARIO_14_INPUT, ERROR_SCENARIO_15_INPUT, ERROR_SCENARIO_16_INPUT,
    ERROR_SCENARIO_17_INPUT, ERROR_SCENARIO_18_INPUT, ERROR_SCENARIO_19_INPUT,
    ERROR_SCENARIO_20_INPUT, ERROR_SCENARIO_21_INPUT, ERROR_SCENARIO_22_INPUT,
    ERROR_SCENARIO_23_INPUT, ERROR_SCENARIO_24_INPUT, ERROR_SCENARIO_25_INPUT,
    ERROR_SCENARIO_26_INPUT, ERROR_SCENARIO_27_INPUT, ERROR_SCENARIO_28_INPUT,
    ERROR_SCENARIO_29_INPUT, ERROR_SCENARIO_30_INPUT, ERROR_SCENARIO_31_INPUT,
    ERROR_SCENARIO_32_INPUT, ERROR_SCENARIO_33_INPUT, ERROR_SCENARIO_34_INPUT,
    ERROR_SCENARIO_35_INPUT, ERROR_SCENARIO_36_INPUT, ERROR_SCENARIO_37_INPUT,
    ERROR_SCENARIO_38_INPUT, ERROR_SCENARIO_39_INPUT, ERROR_SCENARIO_40_INPUT,
    ERROR_SCENARIO_41_INPUT, ERROR_SCENARIO_42_INPUT, ERROR_SCENARIO_43_INPUT,
    ERROR_SCENARIO_44_INPUT, ERROR_SCENARIO_45_INPUT, ERROR_SCENARIO_46_INPUT,
    ERROR_SCENARIO_47_INPUT, SCENARIO_0_INPUT, SCENARIO_1_INPUT, SCENARIO_2_INPUT,
    SCENARIO_3_INPUT, SCENARIO_4_INPUT, SCENARIO_5_INPUT, SCENARIO_6_INPUT, SCENARIO_7_INPUT,
    SCENARIO_8_INPUT, SCENARIO_9_INPUT, SCENARIO_10_INPUT, SCENARIO_11_INPUT, SCENARIO_12_INPUT,
    SCENARIO_13_INPUT, SCENARIO_14_INPUT, SCENARIO_15_INPUT, SCENARIO_16_INPUT, SCENARIO_17_INPUT,
    STRICT_ABORT_PARTIAL_INPUT, SUMMARY_FRACTIONAL_COUNT_INPUT, SUMMARY_INFINITE_QUANTILE_INPUT,
    SUMMARY_NAN_QUANTILE_INPUT, SUMMARY_NEGATIVE_COUNT_INPUT, SUMMARY_PERMUTED_LABELS_INPUT,
};

use common::{TestMode, parse, parse_strict};

fn family(
    name: &'static str,
    metric_type: MetricType,
    help: Option<&'static str>,
    metrics: Vec<Metric<'static>>,
) -> MetricFamily<'static> {
    MetricFamily {
        name: Cow::from(name),
        help: help.map(Cow::from),
        metric_type,
        metrics,
    }
}

fn to_labels(labels: Vec<(&'static str, &'static str)>) -> Vec<Label<'static>> {
    labels
        .into_iter()
        .map(|(name, value)| Label {
            name: Cow::from(name),
            value: Cow::from(value),
        })
        .collect()
}

fn metric(
    labels: Vec<(&'static str, &'static str)>,
    value: f64,
    timestamp: Option<i64>,
) -> Metric<'static> {
    Metric {
        labels: to_labels(labels),
        timestamp,
        value: MetricValue::Sample(value),
    }
}

fn summary_metric(
    labels: Vec<(&'static str, &'static str)>,
    timestamp: Option<i64>,
    sample_count: Option<u64>,
    sample_sum: Option<f64>,
    quantiles: Vec<(f64, f64)>,
) -> Metric<'static> {
    Metric {
        labels: to_labels(labels),
        timestamp,
        value: MetricValue::Summary(SummaryValue {
            sample_count,
            sample_sum,
            quantiles: quantiles
                .into_iter()
                .map(|(quantile, value)| Quantile { quantile, value })
                .collect(),
        }),
    }
}

#[allow(clippy::type_complexity)]
fn histogram_metric(
    labels: Vec<(&'static str, &'static str)>,
    timestamp: Option<i64>,
    sample_count: Option<IntOrFloat>,
    sample_sum: Option<f64>,
    buckets: Vec<(f64, IntOrFloat)>,
) -> Metric<'static> {
    Metric {
        labels: to_labels(labels),
        timestamp,
        value: MetricValue::Histogram(HistogramValue {
            sample_count,
            sample_sum,
            buckets: buckets
                .into_iter()
                .map(|(upper_bound, cumulative_count)| Bucket {
                    upper_bound,
                    cumulative_count,
                })
                .collect(),
        }),
    }
}

fn assert_scenario_passes(input: &str, want: Vec<MetricFamily<'static>>) {
    let (got, err) = parse_strict(input.as_bytes(), Scheme::Utf8);
    assert_eq!(err, None, "unexpected parse error");
    assert_eq!(got, want);
}

// "0: Empty lines as input." (text_parse_test.go lines 50-56)
#[test]
fn text_parse_0_empty_lines_as_input_should_pass() {
    let input = SCENARIO_0_INPUT;
    assert_scenario_passes(input, vec![]);
}

// "1: Minimal case."
#[test]
fn text_parse_1_minimal_case_should_pass() {
    let input = SCENARIO_1_INPUT;
    let (got, stats) = parse(input.as_bytes(), TestMode::StrictUtf8);
    let want = vec![
        family(
            "minimal_metric",
            MetricType::Untyped,
            None,
            vec![metric(vec![], 1.234, None)],
        ),
        family(
            "another_metric",
            MetricType::Untyped,
            None,
            vec![metric(vec![], -3e3, Some(103948))],
        ),
        family(
            "no_labels",
            MetricType::Untyped,
            None,
            vec![metric(vec![], 3.0, None)],
        ),
    ];
    assert_eq!(got, want);
    // The trailing "# HELP line ..." comment opens a help-only family, which
    // Go deletes at end of parse as well.
    assert_eq!(stats.pruned_families, 1);
}

// "2: Counters & gauges, docstrings, various whitespace, escape sequences."
//
#[test]
fn text_parse_2_counters_gauges_docstrings_various_whitespace_escape_sequences_should_pass() {
    let input = SCENARIO_2_INPUT;
    let want = vec![
        family(
            "name",
            MetricType::Counter,
            Some("two-line\n doc  str\\ing"),
            vec![
                metric(
                    vec![("labelname", "val1"), ("basename", "basevalue")],
                    f64::NAN,
                    None,
                ),
                metric(
                    vec![("labelname", "val2"), ("basename", "base\"v\\al\nue")],
                    0.23,
                    Some(1234567890),
                ),
            ],
        ),
        family(
            "name2",
            MetricType::Gauge,
            Some("doc str\"ing 2"),
            vec![
                metric(
                    vec![("labelname", "val2"), ("basename", "basevalue2")],
                    f64::INFINITY,
                    Some(54321),
                ),
                metric(vec![("labelname", "val1")], f64::NEG_INFINITY, None),
            ],
        ),
    ];
    assert_scenario_passes(input, want);
}

// "3: The evil summary, mixed with other types and funny comments."
#[test]
fn text_parse_3_the_evil_summary_mixed_with_other_types_and_funny_comments_should_pass() {
    let input = SCENARIO_3_INPUT;
    // Creation order: my_summary (TYPE line 2), decoy, fake_sum,
    // another_summary. Go's array order (fake_sum, decoy, ...) is map
    // literal order only.
    let want = vec![
        family(
            "my_summary",
            MetricType::Summary,
            None,
            vec![
                summary_metric(
                    vec![("n1", "val1")],
                    Some(2),
                    Some(42),
                    Some(4711.0),
                    vec![(0.5, 110.0), (0.9, 140.0)],
                ),
                summary_metric(
                    vec![("n2", "val2"), ("n1", "val1")],
                    Some(5),
                    Some(5),
                    None,
                    vec![(-12.34, f64::NAN)],
                ),
                summary_metric(vec![("n1", "val2")], Some(15), None, Some(8.0), vec![]),
                summary_metric(vec![("n1", "val3")], None, None, None, vec![(0.2, 4711.0)]),
            ],
        ),
        family(
            "decoy",
            MetricType::Untyped,
            None,
            vec![metric(vec![], -1.0, Some(-2))],
        ),
        family(
            "fake_sum",
            MetricType::Untyped,
            None,
            vec![metric(vec![("n1", "val1")], 2001.0, None)],
        ),
        family(
            "another_summary",
            MetricType::Summary,
            None,
            vec![summary_metric(
                vec![("n2", "val2"), ("n1", "val1")],
                None,
                Some(20),
                None,
                vec![(0.3, -1.2)],
            )],
        ),
    ];
    assert_scenario_passes(input, want);
}

// "4: The histogram."
#[test]
fn text_parse_4_the_histogram_should_pass() {
    let input = SCENARIO_4_INPUT;
    let want = vec![family(
        "request_duration_microseconds",
        MetricType::Histogram,
        Some("The response latency."),
        vec![histogram_metric(
            vec![],
            None,
            Some(IntOrFloat::Int(2693)),
            Some(1756047.3),
            vec![
                (100.0, IntOrFloat::Int(123)),
                (120.0, IntOrFloat::Int(412)),
                (144.0, IntOrFloat::Int(592)),
                (172.8, IntOrFloat::Int(1524)),
                (f64::INFINITY, IntOrFloat::Int(2693)),
            ],
        )],
    )];
    assert_scenario_passes(input, want);
}

// "5: Quoted metric name and quoted label name with dots."
#[test]
fn text_parse_5_quoted_metric_name_and_quoted_label_name_with_dots_should_pass() {
    let input = SCENARIO_5_INPUT;
    let want = vec![family(
        "my.noncompliant.metric",
        MetricType::Counter,
        Some("help text"),
        vec![metric(vec![("label.name", "value")], 1.0, None)],
    )];
    assert_scenario_passes(input, want);
}

// "6: Metric family with dots in name."
#[test]
fn text_parse_6_metric_family_with_dots_in_name_should_pass() {
    let input = SCENARIO_6_INPUT;
    let want = vec![family(
        "name.with.dots",
        MetricType::Counter,
        Some("boring help"),
        vec![
            metric(
                vec![("labelname", "val1"), ("basename", "basevalue")],
                42.0,
                None,
            ),
            metric(
                vec![("labelname", "val2"), ("basename", "basevalue")],
                0.23,
                Some(1234567890),
            ),
        ],
    )];
    assert_scenario_passes(input, want);
}

// "7: Metric family with dots in name, no labels."
#[test]
fn text_parse_7_metric_family_with_dots_in_name_no_labels_should_pass() {
    let input = SCENARIO_7_INPUT;
    let want = vec![family(
        "name.with.dots",
        MetricType::Counter,
        Some("boring help"),
        vec![
            metric(vec![], 42.0, None),
            metric(vec![], 0.23, Some(1234567890)),
        ],
    )];
    assert_scenario_passes(input, want);
}

// "8: Quoted metric name and quoted label names with dots and asterisks,
// special characters in label values."
#[test]
fn text_parse_8_quoted_metric_name_and_quoted_label_names_with_dots_and_asterisks_special_characters_in_label_values_should_pass()
 {
    let input = SCENARIO_8_INPUT;
    let want = vec![family(
        "gauge.name",
        MetricType::Gauge,
        Some("gauge\ndoc\nstr\"ing"),
        vec![
            metric(
                vec![
                    ("name.1", "val with\nnew line"),
                    ("name*2", "val with \\backslash and \"quotes\""),
                ],
                f64::INFINITY,
                None,
            ),
            metric(vec![("name.1", "Björn"), ("name*2", "佖佥")], 3.14e42, None),
        ],
    )];
    assert_scenario_passes(input, want);
}

// "9: Various escaped special characters in metric and label names."
//
#[test]
fn text_parse_9_various_escaped_special_characters_in_metric_and_label_names_should_pass() {
    let input = SCENARIO_9_INPUT;
    let want = vec![family(
        "my\"noncompliant\nmetric\\",
        MetricType::Counter,
        Some("help text"),
        vec![metric(vec![("label\"name\n", "value")], 1.0, None)],
    )];
    assert_scenario_passes(input, want);
}

// "10: Quoted metric name, not the first element in the label set."
//
#[test]
fn text_parse_10_quoted_metric_name_not_the_first_element_in_the_label_set_should_pass() {
    let input = SCENARIO_10_INPUT;
    let want = vec![family(
        "my.noncompliant.metric",
        MetricType::Counter,
        Some("help text"),
        vec![metric(vec![("labelname", "value")], 1.0, None)],
    )];
    assert_scenario_passes(input, want);
}

// "11: Multiple minimal metrics with quoted metric names."
#[test]
fn text_parse_11_multiple_minimal_metrics_with_quoted_metric_names_should_pass() {
    let input = SCENARIO_11_INPUT;
    let want = vec![
        family(
            "name.1",
            MetricType::Untyped,
            None,
            vec![metric(vec![], 1.0, None)],
        ),
        family(
            "name.2",
            MetricType::Untyped,
            None,
            vec![metric(vec![], 1.0, None)],
        ),
        family(
            "name.3",
            MetricType::Untyped,
            None,
            vec![metric(vec![], 1.0, None)],
        ),
    ];
    assert_scenario_passes(input, want);
}

// "12: A full float histogram."
#[test]
fn text_parse_12_a_full_float_histogram_should_pass() {
    let input = SCENARIO_12_INPUT;
    let want = vec![family(
        "request_duration_microseconds",
        MetricType::Histogram,
        Some("The response latency."),
        vec![histogram_metric(
            vec![],
            None,
            Some(IntOrFloat::Float(2693.9)),
            Some(1756047.3),
            vec![
                (100.0, IntOrFloat::Float(123.5)),
                (120.0, IntOrFloat::Float(412.6)),
                (144.0, IntOrFloat::Float(592.7)),
                (172.8, IntOrFloat::Float(1524.8)),
                (f64::INFINITY, IntOrFloat::Float(2693.9)),
            ],
        )],
    )];
    assert_scenario_passes(input, want);
}

// "13: A float histogram where only the count is really a float."
//
#[test]
fn text_parse_13_a_float_histogram_where_only_the_count_is_really_a_float_should_pass() {
    let input = SCENARIO_13_INPUT;
    // The float count normalizes the whole histogram to float counters.
    let want = vec![family(
        "request_duration_microseconds",
        MetricType::Histogram,
        Some("The response latency."),
        vec![histogram_metric(
            vec![],
            None,
            Some(IntOrFloat::Float(2693.9)),
            Some(1756047.3),
            vec![
                (100.0, IntOrFloat::Float(123.0)),
                (120.0, IntOrFloat::Float(412.0)),
                (144.0, IntOrFloat::Float(592.0)),
                (172.8, IntOrFloat::Float(1524.0)),
            ],
        )],
    )];
    assert_scenario_passes(input, want);
}

// "14: A float histogram where only one of the buckets is really a float."
//
#[test]
fn text_parse_14_a_float_histogram_where_only_one_of_the_buckets_is_really_a_float_should_pass() {
    let input = SCENARIO_14_INPUT;
    let want = vec![family(
        "request_duration_microseconds",
        MetricType::Histogram,
        Some("The response latency."),
        vec![histogram_metric(
            vec![],
            None,
            Some(IntOrFloat::Float(2693.0)),
            Some(1756047.3),
            vec![
                (100.0, IntOrFloat::Float(123.0)),
                (120.0, IntOrFloat::Float(412.0)),
                (144.0, IntOrFloat::Float(592.0)),
                (172.8, IntOrFloat::Float(1524.8)),
                (f64::INFINITY, IntOrFloat::Float(2693.0)),
            ],
        )],
    )];
    assert_scenario_passes(input, want);
}

// "15: A gauge histogram."
#[test]
fn text_parse_15_a_gauge_histogram_should_pass() {
    let input = SCENARIO_15_INPUT;
    let want = vec![family(
        "request_duration_microseconds",
        MetricType::GaugeHistogram,
        Some("The response latency."),
        vec![histogram_metric(
            vec![],
            None,
            Some(IntOrFloat::Int(2693)),
            Some(1756047.3),
            vec![
                (100.0, IntOrFloat::Int(123)),
                (120.0, IntOrFloat::Int(412)),
                (144.0, IntOrFloat::Int(592)),
                (172.8, IntOrFloat::Int(1524)),
                (f64::INFINITY, IntOrFloat::Int(2693)),
            ],
        )],
    )];
    assert_scenario_passes(input, want);
}

// "16: A gauge histogram with alternative spelling."
#[test]
fn text_parse_16_a_gauge_histogram_with_alternative_spelling_should_pass() {
    let input = SCENARIO_16_INPUT;
    let want = vec![family(
        "request_duration_microseconds",
        MetricType::GaugeHistogram,
        Some("The response latency."),
        vec![histogram_metric(
            vec![],
            None,
            Some(IntOrFloat::Int(2693)),
            Some(1756047.3),
            vec![
                (100.0, IntOrFloat::Int(123)),
                (120.0, IntOrFloat::Int(412)),
                (144.0, IntOrFloat::Int(592)),
                (172.8, IntOrFloat::Int(1524)),
                (f64::INFINITY, IntOrFloat::Int(2693)),
            ],
        )],
    )];
    assert_scenario_passes(input, want);
}

// "17: A float gauge histogram where only one of the buckets is really a
// float and with alternative spelling."
#[test]
fn text_parse_17_a_float_gauge_histogram_where_only_one_of_the_buckets_is_really_a_float_and_with_alternative_spelling_should_pass()
 {
    let input = SCENARIO_17_INPUT;
    let want = vec![family(
        "request_duration_microseconds",
        MetricType::GaugeHistogram,
        Some("The response latency."),
        vec![histogram_metric(
            vec![],
            None,
            Some(IntOrFloat::Float(2693.0)),
            Some(1756047.3),
            vec![
                (100.0, IntOrFloat::Float(123.0)),
                (120.0, IntOrFloat::Float(412.0)),
                (144.0, IntOrFloat::Float(592.0)),
                (172.8, IntOrFloat::Float(1524.8)),
                (f64::INFINITY, IntOrFloat::Float(2693.0)),
            ],
        )],
    )];
    assert_scenario_passes(input, want);
}

// ============================ error scenarios ==============================
// Ported from testTextParseError . Each input runs under
// both schemes; expectations are prefix matches on the error's Display, like
// Go's strings.Index check. An empty expectation asserts a successful parse.

fn assert_error_scenario(input: &str, err_utf8: &str, err_legacy: &str) {
    let expected_legacy = if err_legacy.is_empty() {
        err_utf8
    } else {
        err_legacy
    };
    for (scheme, expected) in [(Scheme::Utf8, err_utf8), (Scheme::Legacy, expected_legacy)] {
        let (_, err) = parse_strict(input.as_bytes(), scheme);
        if expected.is_empty() {
            assert_eq!(err, None, "expected no error under {scheme:?}");
        } else {
            let err = err.unwrap_or_else(|| {
                panic!("expected error starting with {expected:?}, got none under {scheme:?}")
            });
            assert!(
                err.to_string().starts_with(expected),
                "expected error starting with {expected:?}, got {err} under {scheme:?}",
            );
        }
    }
}

// "0: No new-line at end of input."
#[test]
fn text_parse_error_0_no_new_line_at_end_of_input_should_fail_with_error_unexpected_end_of_input_stream()
 {
    let input = ERROR_SCENARIO_0_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 3: unexpected end of input stream",
        "",
    );
}

// "1: Invalid escape sequence in label value."
#[test]
fn text_parse_error_1_invalid_escape_sequence_in_label_value_should_fail_with_error_invalid_escape_sequence()
 {
    let input = ERROR_SCENARIO_1_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: invalid escape sequence",
        "",
    );
}

// "2: Newline in label value."
#[test]
fn text_parse_error_2_newline_in_label_value_should_fail_with_error_label_value_contains_unescaped_new_line()
 {
    let input = ERROR_SCENARIO_2_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 2: label value \"new\" contains unescaped new-line",
        "",
    );
}

#[test]
fn text_parse_error_3_invalid_label_name_for_metric_should_fail_with_error_invalid_label_name_for_metric()
 {
    let input = ERROR_SCENARIO_3_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: invalid label name for metric",
        "",
    );
}

#[test]
fn text_parse_error_4_reserved_label_name_should_fail_with_error_label_name_reserved() {
    let input = ERROR_SCENARIO_4_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: label name \"__name__\" is reserved",
        "",
    );
}

#[test]
fn text_parse_error_5_expected_eq_after_label_name_should_fail_with_error_expected_eq_after_label_name()
 {
    let input = ERROR_SCENARIO_5_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: expected '=' after label name",
        "",
    );
}

#[test]
fn text_parse_error_6_expected_quote_at_start_of_label_value_should_fail_with_error_expected_quote_at_start_of_label_value()
 {
    let input = ERROR_SCENARIO_6_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: expected '\"' at start of label value",
        "",
    );
}

#[test]
fn text_parse_error_7_expected_float_as_quantile_value_should_fail_with_error_expected_float_as_value_for_quantile_label()
 {
    let input = ERROR_SCENARIO_7_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 3: expected float as value for 'quantile' label",
        "",
    );
}

#[test]
fn text_parse_error_8_unexpected_end_of_label_value_should_fail_with_error_unexpected_end_of_label_value()
 {
    let input = ERROR_SCENARIO_8_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: unexpected end of label value",
        "",
    );
}

#[test]
fn text_parse_error_9_expected_integer_as_timestamp_should_fail_with_error_expected_integer_as_timestamp()
 {
    let input = ERROR_SCENARIO_9_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: expected integer as timestamp",
        "",
    );
}

#[test]
fn text_parse_error_10_spurious_string_after_timestamp_should_fail_with_error_spurious_string_after_timestamp()
 {
    let input = ERROR_SCENARIO_10_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: spurious string after timestamp",
        "",
    );
}

#[test]
fn text_parse_error_11_expected_float_as_value_should_fail_with_error_expected_float_as_value() {
    let input = ERROR_SCENARIO_11_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: expected float as value",
        "",
    );
}

#[test]
fn text_parse_error_12_second_help_line_should_fail_with_error_second_help_line() {
    let input = ERROR_SCENARIO_12_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 3: second HELP line for metric name",
        "",
    );
}

#[test]
fn text_parse_error_13_second_type_line_should_fail_with_error_second_type_line() {
    let input = ERROR_SCENARIO_13_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 3: second TYPE line for metric name \"metric\", or TYPE reported after samples",
        "",
    );
}

#[test]
fn text_parse_error_14_type_reported_after_samples_should_fail_with_error_second_type_line() {
    let input = ERROR_SCENARIO_14_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 3: second TYPE line for metric name \"metric\", or TYPE reported after samples",
        "",
    );
}

#[test]
fn text_parse_error_15_unknown_metric_type_should_fail_with_error_unknown_metric_type() {
    let input = ERROR_SCENARIO_15_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 2: unknown metric type",
        "",
    );
}

#[test]
fn text_parse_error_16_invalid_metric_name_in_comment_should_fail_with_error_invalid_metric_name_in_comment()
 {
    let input = ERROR_SCENARIO_16_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 2: invalid metric name in comment",
        "",
    );
}

#[test]
fn text_parse_error_17_invalid_metric_name_at_sign_should_fail_with_error_invalid_metric_name() {
    let input = ERROR_SCENARIO_17_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: invalid metric name",
        "",
    );
}

#[test]
fn text_parse_error_18_missing_metric_name_should_fail_with_error_invalid_metric_name() {
    let input = ERROR_SCENARIO_18_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: invalid metric name",
        "",
    );
}

#[test]
fn text_parse_error_19_expected_float_as_le_value_should_fail_with_error_expected_float_as_value_for_le_label()
 {
    let input = ERROR_SCENARIO_19_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 3: expected float as value for 'le' label",
        "",
    );
}

// "20: Invalid UTF-8 in label value."
#[test]
fn text_parse_error_20_invalid_utf8_in_label_value_should_fail_with_error_invalid_label_value() {
    let input = ERROR_SCENARIO_20_INPUT;
    let expected = "text format parsing error in line 1: invalid label value \"\\xbd\"";
    let (_, err) = parse_strict(input, Scheme::Utf8);
    assert!(err.unwrap().to_string().starts_with(expected));
    let (_, err) = parse_strict(input, Scheme::Legacy);
    assert!(err.unwrap().to_string().starts_with(expected));
}

// "21: Go 1.13 sometimes allows underscores in numbers."
#[test]
fn text_parse_error_21_go_1_13_allows_underscores_in_numbers_should_fail_with_error_expected_float_as_value()
 {
    let input = ERROR_SCENARIO_21_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: expected float as value",
        "",
    );
}

// "22: Go 1.13 supports hex floating point."
#[test]
fn text_parse_error_22_go_1_13_hex_floating_point_should_fail_with_error_expected_float_as_value() {
    let input = ERROR_SCENARIO_22_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: expected float as value",
        "",
    );
}

// "23: Check for various other literals variants, just in case."
#[test]
fn text_parse_error_23_other_literal_variants_should_fail_with_error_expected_float_as_value() {
    let input = ERROR_SCENARIO_23_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: expected float as value",
        "",
    );
}

#[test]
fn text_parse_error_24_literal_0b1_uppercase_should_fail_with_error_expected_float_as_value() {
    let input = ERROR_SCENARIO_24_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: expected float as value",
        "",
    );
}

#[test]
fn text_parse_error_25_literal_0o1_uppercase_should_fail_with_error_expected_float_as_value() {
    let input = ERROR_SCENARIO_25_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: expected float as value",
        "",
    );
}

#[test]
fn text_parse_error_26_literal_0x1_uppercase_should_fail_with_error_expected_float_as_value() {
    let input = ERROR_SCENARIO_26_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: expected float as value",
        "",
    );
}

#[test]
fn text_parse_error_27_literal_0x1_should_fail_with_error_expected_float_as_value() {
    let input = ERROR_SCENARIO_27_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: expected float as value",
        "",
    );
}

#[test]
fn text_parse_error_28_literal_0b1_should_fail_with_error_expected_float_as_value() {
    let input = ERROR_SCENARIO_28_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: expected float as value",
        "",
    );
}

#[test]
fn text_parse_error_29_literal_0o1_should_fail_with_error_expected_float_as_value() {
    let input = ERROR_SCENARIO_29_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: expected float as value",
        "",
    );
}

#[test]
fn text_parse_error_30_literal_0x1_repeat_should_fail_with_error_expected_float_as_value() {
    let input = ERROR_SCENARIO_30_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: expected float as value",
        "",
    );
}

#[test]
fn text_parse_error_31_literal_0x1_repeat_2_should_fail_with_error_expected_float_as_value() {
    let input = ERROR_SCENARIO_31_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: expected float as value",
        "",
    );
}

// "32: Check histogram label."
#[test]
fn text_parse_error_32_histogram_label_hex_should_fail_with_error_expected_float_as_value_for_le_label()
 {
    let input = ERROR_SCENARIO_32_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 3: expected float as value for 'le' label",
        "",
    );
}

// "33: Check quantile label."
#[test]
fn text_parse_error_33_quantile_label_hex_should_fail_with_error_expected_float_as_value_for_quantile_label()
 {
    let input = ERROR_SCENARIO_33_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 3: expected float as value for 'quantile' label",
        "",
    );
}

// "34: Check duplicate label."
#[test]
fn text_parse_error_34_duplicate_label_should_fail_with_error_duplicate_label_names() {
    let input = ERROR_SCENARIO_34_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: duplicate label names for metric",
        "",
    );
}

// "35: Multiple quoted metric names."
#[test]
fn text_parse_error_35_multiple_quoted_metric_names_should_fail_with_error_multiple_metric_names() {
    let input = ERROR_SCENARIO_35_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: multiple metric names",
        "text format parsing error in line 1: invalid metric name \"one.name\"",
    );
}

// "36: Invalid escape sequence in quoted metric name."
#[test]
fn text_parse_error_36_invalid_escape_sequence_in_quoted_metric_name_should_fail_with_error_invalid_escape_sequence()
 {
    let input = ERROR_SCENARIO_36_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: invalid escape sequence",
        "",
    );
}

// "37: Unexpected end of quoted metric name."
#[test]
fn text_parse_error_37_unexpected_end_of_quoted_metric_name_should_fail_with_error_unexpected_end_of_metric_name()
 {
    let input = ERROR_SCENARIO_37_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: unexpected end of metric name",
        "",
    );
}

// "38: Invalid escape sequence in quoted metric name."
#[test]
fn text_parse_error_38_invalid_escape_sequence_on_type_line_should_fail_with_error_invalid_escape_sequence()
 {
    let input = ERROR_SCENARIO_38_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 2: invalid escape sequence",
        "",
    );
}

// "39: Newline in quoted metric name."
#[test]
fn text_parse_error_39_newline_in_quoted_metric_name_should_fail_with_error_metric_name_contains_unescaped_new_line()
 {
    let input = ERROR_SCENARIO_39_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 2: metric name \"metric\" contains unescaped new-line",
        "",
    );
}

// "40: Newline in quoted label name."
#[test]
fn text_parse_error_40_newline_in_quoted_label_name_should_fail_with_error_label_name_contains_unescaped_new_line()
 {
    let input = ERROR_SCENARIO_40_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 2: label name \"new\" contains unescaped new-line",
        "text format parsing error in line 2: invalid metric name \"metric.name\"",
    );
}

// "41: Dotted metric name fails legacy validation."
#[test]
fn text_parse_error_41_dotted_metric_name_fails_legacy_validation_should_fail_with_error_invalid_metric_name()
 {
    let input = ERROR_SCENARIO_41_INPUT;
    assert_error_scenario(
        input,
        "",
        "text format parsing error in line 1: invalid metric name \"metric.name\"",
    );
}

// "42: Dotted label name fails legacy validation."
#[test]
fn text_parse_error_42_dotted_label_name_fails_legacy_validation_should_fail_with_error_invalid_label_name()
 {
    let input = ERROR_SCENARIO_42_INPUT;
    assert_error_scenario(
        input,
        "",
        "text format parsing error in line 1: invalid label name \"dotted.label\"",
    );
}

// "43: Histogram with negative count."
#[test]
fn text_parse_error_43_histogram_with_negative_count_should_fail_with_error_negative_count_for_histogram()
 {
    let input = ERROR_SCENARIO_43_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 10: negative count for histogram \"request_duration_microseconds\"",
        "",
    );
}

// "44: Histogram with negative bucket."
#[test]
fn text_parse_error_44_histogram_with_negative_bucket_should_fail_with_error_negative_bucket_population()
 {
    let input = ERROR_SCENARIO_44_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 5: negative bucket population for histogram \"request_duration_microseconds\"",
        "",
    );
}

// "45: Histogram with negative float count."
#[test]
fn text_parse_error_45_histogram_with_negative_float_count_should_fail_with_error_negative_count_for_histogram()
 {
    let input = ERROR_SCENARIO_45_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 10: negative count for histogram \"request_duration_microseconds\"",
        "",
    );
}

// "46: Histogram with negative float bucket."
#[test]
fn text_parse_error_46_histogram_with_negative_float_bucket_should_fail_with_error_negative_bucket_population()
 {
    let input = ERROR_SCENARIO_46_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 5: negative bucket population for histogram \"request_duration_microseconds\"",
        "",
    );
}

// "47: Empty braces with no metric name (regression test for a nil pointer
// dereference in startLabelName)."
#[test]
fn text_parse_error_47_empty_braces_with_no_metric_name_should_fail_with_error_invalid_metric_name()
{
    let input = ERROR_SCENARIO_47_INPUT;
    assert_error_scenario(
        input,
        "text format parsing error in line 1: invalid metric name",
        "",
    );
}

// ========================== aggregation corners ============================
// Behaviors verified against the Go parser by probe (they are not Go-table
// scenarios) and locked here so they cannot regress.

// quantile="NaN": Go's !IsNaN branch drops the quantile and the value, and
// an empty Summary row still exists.
#[test]
fn strict_summary_on_nan_quantile_should_pass_with_quantile_and_value_dropped() {
    let input = SUMMARY_NAN_QUANTILE_INPUT;
    let want = vec![family(
        "s",
        MetricType::Summary,
        None,
        vec![summary_metric(vec![], None, None, None, vec![])],
    )];
    let (got, err) = parse_strict(input.as_bytes(), Scheme::Utf8);
    assert_eq!(err, None);
    assert_eq!(got, want);
}

// A negative summary count errors in both modes; Go wraps it through
// uint64(value) instead (probed behavior, overridden by user decision).
#[test]
fn strict_summary_on_negative_count_should_fail_with_error_negative_count_for_summary() {
    let input = SUMMARY_NEGATIVE_COUNT_INPUT;
    let (got, err) = parse_strict(input.as_bytes(), Scheme::Utf8);
    let err = err.expect("expected abort error");
    assert_eq!(err.line, 2);
    assert!(matches!(err.kind, ErrorKind::NegativeSummaryCount { .. }));
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].name, "s");
    assert_eq!(got[0].metrics.len(), 1);
}

// quantile="+Inf" is not NaN, so Go keeps it.
#[test]
fn strict_summary_on_infinite_quantile_should_pass() {
    let input = SUMMARY_INFINITE_QUANTILE_INPUT;
    let want = vec![family(
        "s",
        MetricType::Summary,
        None,
        vec![summary_metric(
            vec![],
            None,
            None,
            None,
            vec![(f64::INFINITY, 7.0)],
        )],
    )];
    let (got, err) = parse_strict(input.as_bytes(), Scheme::Utf8);
    assert_eq!(err, None);
    assert_eq!(got, want);
}

// A fractional summary count truncates like Go's uint64(value).
#[test]
fn strict_summary_on_fractional_count_should_pass_with_truncation() {
    let input = SUMMARY_FRACTIONAL_COUNT_INPUT;
    let want = vec![family(
        "s",
        MetricType::Summary,
        None,
        vec![summary_metric(vec![], None, Some(42), None, vec![])],
    )];
    let (got, err) = parse_strict(input.as_bytes(), Scheme::Utf8);
    assert_eq!(err, None);
    assert_eq!(got, want);
}

// On abort, the families accumulated before the error are still returned
// next to it, and parsing stops at the error line (probe-verified against
// Go: the histogram row from the first bucket survives, the line after the
// error is not folded).
#[test]
fn strict_abort_should_return_partial_families_and_stop_parsing() {
    let input = STRICT_ABORT_PARTIAL_INPUT;
    let (got, err) = parse_strict(input.as_bytes(), Scheme::Utf8);
    let err = err.expect("expected abort error");
    assert_eq!(err.line, 3);
    assert!(matches!(err.kind, ErrorKind::NegativeHistogramCount { .. }));
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].name, "h");
    assert_eq!(got[0].metrics.len(), 1);
    assert_eq!(
        got[0].metrics[0].value,
        MetricValue::Histogram(HistogramValue {
            sample_count: None,
            sample_sum: None,
            buckets: vec![Bucket {
                upper_bound: 1.0,
                cumulative_count: IntOrFloat::Int(10),
            }],
        })
    );
}

// Label order carries no identity: quantile lines whose non-quantile labels
// are written in different orders fold into one summary row, so the second
// line must hit the row the first line created.
#[test]
fn strict_summary_on_permuted_label_order_should_pass_with_one_row() {
    let input = SUMMARY_PERMUTED_LABELS_INPUT;
    assert_scenario_passes(
        input,
        vec![family(
            "s",
            MetricType::Summary,
            None,
            vec![summary_metric(
                vec![("a", "1"), ("b", "2")],
                None,
                None,
                None,
                vec![(0.5, 1.0), (0.9, 2.0)],
            )],
        )],
    );
}
