//! Scenario input shared by property tests. The payloads are the
//! same literals the scenario table in tests/scenarios.rs asserts

pub const SCENARIO_0_INPUT: &str = r#"

"#;

pub const SCENARIO_1_INPUT: &str = r#"
minimal_metric 1.234
another_metric -3e3 103948
# Even that:
no_labels{} 3
# HELP line for non-existing metric will be ignored.
"#;

pub const SCENARIO_2_INPUT: &str = r#"
# A normal comment.
#
# TYPE name counter
name{labelname="val1",basename="basevalue"} NaN
name {labelname="val2",basename="base\"v\\al\nue"} 0.23 1234567890
# HELP name two-line\n doc  str\\ing

 # HELP  name2  	doc str"ing 2
  #    TYPE    name2 gauge
name2{labelname="val2"	,basename   =   "basevalue2"		} +Inf 54321
name2{ labelname = "val1" , }-Inf
"#;

pub const SCENARIO_3_INPUT: &str = r#"
# TYPE my_summary summary
my_summary{n1="val1",quantile="0.5"} 110
decoy -1 -2
my_summary{n1="val1",quantile="0.9"} 140 1
my_summary_count{n1="val1"} 42
# Latest timestamp wins in case of a summary.
my_summary_sum{n1="val1"} 4711 2
fake_sum{n1="val1"} 2001
# TYPE another_summary summary
another_summary_count{n2="val2",n1="val1"} 20
my_summary_count{n2="val2",n1="val1"} 5 5
another_summary{n1="val1",n2="val2",quantile=".3"} -1.2
my_summary_sum{n1="val2"} 08 15
my_summary{n1="val3", quantile="0.2"} 4711
  my_summary{n1="val1",n2="val2",quantile="-12.34",} NaN
# some
# funny comments
# HELP
# HELP
# HELP my_summary
# HELP my_summary
"#;

pub const SCENARIO_4_INPUT: &str = r#"
# HELP request_duration_microseconds The response latency.
# TYPE request_duration_microseconds histogram
request_duration_microseconds_bucket{le="100"} 123
request_duration_microseconds_bucket{le="120"} 412
request_duration_microseconds_bucket{le="144"} 592
request_duration_microseconds_bucket{le="172.8"} 1524
request_duration_microseconds_bucket{le="+Inf"} 2693
request_duration_microseconds_sum 1.7560473e+06
request_duration_microseconds_count 2693
"#;

pub const SCENARIO_5_INPUT: &str = r#"
# HELP "my.noncompliant.metric" help text
# TYPE "my.noncompliant.metric" counter
{"my.noncompliant.metric","label.name"="value"} 1
"#;

pub const SCENARIO_6_INPUT: &str = r#"
# HELP "name.with.dots" boring help
# TYPE "name.with.dots" counter
{"name.with.dots",labelname="val1",basename="basevalue"} 42.0
{"name.with.dots",labelname="val2",basename="basevalue"} 0.23 1234567890
"#;

pub const SCENARIO_7_INPUT: &str = r#"
				# HELP "name.with.dots" boring help
				# TYPE "name.with.dots" counter
				{"name.with.dots"} 42.0
				{"name.with.dots"} 0.23 1234567890
				"#;

pub const SCENARIO_8_INPUT: &str = r#"# HELP "gauge.name" gauge\ndoc\nstr\"ing
# TYPE "gauge.name" gauge
{"gauge.name","name.1"="val with\nnew line","name*2"="val with \\backslash and \"quotes\""} +Inf
{"gauge.name","name.1"="Björn","name*2"="佖佥"} 3.14e+42
"#;

pub const SCENARIO_9_INPUT: &str = r#"
# HELP "my\"noncompliant\nmetric\\" help text
# TYPE "my\"noncompliant\nmetric\\" counter
{"my\"noncompliant\nmetric\\","label\"name\n"="value"} 1
"#;

pub const SCENARIO_10_INPUT: &str = r#"
# HELP "my.noncompliant.metric" help text
# TYPE "my.noncompliant.metric" counter
{labelname="value", "my.noncompliant.metric"} 1
"#;

pub const SCENARIO_11_INPUT: &str = r#"
{"name.1"} 1
{"name.2"} 1
{"name.3"} 1
"#;

pub const SCENARIO_12_INPUT: &str = r#"
# HELP request_duration_microseconds The response latency.
# TYPE request_duration_microseconds histogram
request_duration_microseconds_bucket{le="100"} 123.5
request_duration_microseconds_bucket{le="120"} 412.6
request_duration_microseconds_bucket{le="144"} 592.7
request_duration_microseconds_bucket{le="172.8"} 1524.8
request_duration_microseconds_bucket{le="+Inf"} 2693.9
request_duration_microseconds_sum 1.7560473e+06
request_duration_microseconds_count 2693.9
"#;

pub const SCENARIO_13_INPUT: &str = r#"
# HELP request_duration_microseconds The response latency.
# TYPE request_duration_microseconds histogram
request_duration_microseconds_bucket{le="100"} 123
request_duration_microseconds_bucket{le="120"} 412
request_duration_microseconds_bucket{le="144"} 592
request_duration_microseconds_bucket{le="172.8"} 1524
request_duration_microseconds_sum 1.7560473e+06
request_duration_microseconds_count 2693.9
"#;

pub const SCENARIO_14_INPUT: &str = r#"
# HELP request_duration_microseconds The response latency.
# TYPE request_duration_microseconds histogram
request_duration_microseconds_bucket{le="100"} 123
request_duration_microseconds_bucket{le="120"} 412
request_duration_microseconds_bucket{le="144"} 592
request_duration_microseconds_bucket{le="172.8"} 1524.8
request_duration_microseconds_bucket{le="+Inf"} 2693
request_duration_microseconds_sum 1.7560473e+06
request_duration_microseconds_count 2693
"#;

pub const SCENARIO_15_INPUT: &str = r#"
# HELP request_duration_microseconds The response latency.
# TYPE request_duration_microseconds gaugehistogram
request_duration_microseconds_bucket{le="100"} 123
request_duration_microseconds_bucket{le="120"} 412
request_duration_microseconds_bucket{le="144"} 592
request_duration_microseconds_bucket{le="172.8"} 1524
request_duration_microseconds_bucket{le="+Inf"} 2693
request_duration_microseconds_sum 1.7560473e+06
request_duration_microseconds_count 2693
"#;

pub const SCENARIO_16_INPUT: &str = r#"
# HELP request_duration_microseconds The response latency.
# TYPE request_duration_microseconds gauge_histogram
request_duration_microseconds_bucket{le="100"} 123
request_duration_microseconds_bucket{le="120"} 412
request_duration_microseconds_bucket{le="144"} 592
request_duration_microseconds_bucket{le="172.8"} 1524
request_duration_microseconds_bucket{le="+Inf"} 2693
request_duration_microseconds_sum 1.7560473e+06
request_duration_microseconds_count 2693
"#;

pub const SCENARIO_17_INPUT: &str = r#"
# HELP request_duration_microseconds The response latency.
# TYPE request_duration_microseconds gauge_histogram
request_duration_microseconds_bucket{le="100"} 123
request_duration_microseconds_bucket{le="120"} 412
request_duration_microseconds_bucket{le="144"} 592
request_duration_microseconds_bucket{le="172.8"} 1524.8
request_duration_microseconds_bucket{le="+Inf"} 2693
request_duration_microseconds_sum 1.7560473e+06
request_duration_microseconds_count 2693
"#;

pub const ERROR_SCENARIO_0_INPUT: &str = r#"
bla 3.14
blubber 42"#;

pub const ERROR_SCENARIO_1_INPUT: &str = r#"metric{label="\t"} 3.14"#;

pub const ERROR_SCENARIO_2_INPUT: &str = r#"
metric{label="new
line"} 3.14
"#;

pub const ERROR_SCENARIO_3_INPUT: &str = r#"metric{@="bla"} 3.14"#;

pub const ERROR_SCENARIO_4_INPUT: &str = r#"metric{__name__="bla"} 3.14"#;

pub const ERROR_SCENARIO_5_INPUT: &str = r#"metric{label+="bla"} 3.14"#;

pub const ERROR_SCENARIO_6_INPUT: &str = r#"metric{label=bla} 3.14"#;

pub const ERROR_SCENARIO_7_INPUT: &str = r#"
# TYPE metric summary
metric{quantile="bla"} 3.14
"#;

pub const ERROR_SCENARIO_8_INPUT: &str = r#"metric{label="bla"+} 3.14"#;

pub const ERROR_SCENARIO_9_INPUT: &str = r#"metric{label="bla"} 3.14 2.72
"#;

pub const ERROR_SCENARIO_10_INPUT: &str = r#"metric{label="bla"} 3.14 2 3
"#;

pub const ERROR_SCENARIO_11_INPUT: &str = r#"metric{label="bla"} blubb
"#;

pub const ERROR_SCENARIO_12_INPUT: &str = r#"
# HELP metric one
# HELP metric two
"#;

pub const ERROR_SCENARIO_13_INPUT: &str = r#"
# TYPE metric counter
# TYPE metric untyped
"#;

pub const ERROR_SCENARIO_14_INPUT: &str = r#"
metric 4.12
# TYPE metric counter
"#;

pub const ERROR_SCENARIO_15_INPUT: &str = r#"
# TYPE metric bla
"#;

pub const ERROR_SCENARIO_16_INPUT: &str = r#"
# TYPE met-ric
"#;

pub const ERROR_SCENARIO_17_INPUT: &str = r#"@invalidmetric{label="bla"} 3.14 2"#;

pub const ERROR_SCENARIO_18_INPUT: &str = r#"{label="bla"} 3.14 2"#;

pub const ERROR_SCENARIO_19_INPUT: &str = r#"
# TYPE metric histogram
metric_bucket{le="bla"} 3.14
"#;

pub const ERROR_SCENARIO_20_INPUT: &[u8] = b"metric{l=\"\xbd\"} 3.14\n";

pub const ERROR_SCENARIO_21_INPUT: &str = "foo 1_2\n";

pub const ERROR_SCENARIO_22_INPUT: &str = "foo 0x1p-3\n";

pub const ERROR_SCENARIO_23_INPUT: &str = "foo 0x1P-3\n";

pub const ERROR_SCENARIO_24_INPUT: &str = "foo 0B1\n";

pub const ERROR_SCENARIO_25_INPUT: &str = "foo 0O1\n";

pub const ERROR_SCENARIO_26_INPUT: &str = "foo 0X1\n";

pub const ERROR_SCENARIO_27_INPUT: &str = "foo 0x1\n";

pub const ERROR_SCENARIO_28_INPUT: &str = "foo 0b1\n";

pub const ERROR_SCENARIO_29_INPUT: &str = "foo 0o1\n";

pub const ERROR_SCENARIO_30_INPUT: &str = "foo 0x1\n";

pub const ERROR_SCENARIO_31_INPUT: &str = "foo 0x1\n";

pub const ERROR_SCENARIO_32_INPUT: &str = r#"
# TYPE metric histogram
metric_bucket{le="0x1p-3"} 3.14
"#;

pub const ERROR_SCENARIO_33_INPUT: &str = r#"
# TYPE metric summary
metric{quantile="0x1p-3"} 3.14
"#;

pub const ERROR_SCENARIO_34_INPUT: &str = r#"metric{label="bla",label="bla"} 3.14"#;

pub const ERROR_SCENARIO_35_INPUT: &str = r#"{"one.name","another.name"} 3.14"#;

pub const ERROR_SCENARIO_36_INPUT: &str = r#"{"a\xc5z",label="bla"} 3.14"#;

pub const ERROR_SCENARIO_37_INPUT: &str = r#"{"metric.name".label="bla"} 3.14"#;

pub const ERROR_SCENARIO_38_INPUT: &str = r#"
# TYPE "metric.name\t" counter
{"metric.name\t",label="bla"} 3.14
"#;

pub const ERROR_SCENARIO_39_INPUT: &str = r#"
# TYPE "metric
name" counter
{"metric
name",label="bla"} 3.14
"#;

pub const ERROR_SCENARIO_40_INPUT: &str = r#"
{"metric.name","new
line"="bla"} 3.14
"#;

pub const ERROR_SCENARIO_41_INPUT: &str = r#"{"metric.name",foo="bla"} 3.14
"#;

pub const ERROR_SCENARIO_42_INPUT: &str = r#"metric_name{"foo"="bar", "dotted.label"="bla"} 3.14
"#;

pub const ERROR_SCENARIO_43_INPUT: &str = r#"
# HELP request_duration_microseconds The response latency.
# TYPE request_duration_microseconds histogram
request_duration_microseconds_bucket{le="100"} 123
request_duration_microseconds_bucket{le="120"} 412
request_duration_microseconds_bucket{le="144"} 592
request_duration_microseconds_bucket{le="172.8"} 1524
request_duration_microseconds_bucket{le="+Inf"} 2693
request_duration_microseconds_sum 1.7560473e+06
request_duration_microseconds_count -2693
"#;

pub const ERROR_SCENARIO_44_INPUT: &str = r#"
# HELP request_duration_microseconds The response latency.
# TYPE request_duration_microseconds histogram
request_duration_microseconds_bucket{le="100"} 123
request_duration_microseconds_bucket{le="120"} -412
request_duration_microseconds_bucket{le="144"} 592
request_duration_microseconds_bucket{le="172.8"} 1524
request_duration_microseconds_bucket{le="+Inf"} 2693
request_duration_microseconds_sum 1.7560473e+06
request_duration_microseconds_count 2693
"#;

pub const ERROR_SCENARIO_45_INPUT: &str = r#"
# HELP request_duration_microseconds The response latency.
# TYPE request_duration_microseconds histogram
request_duration_microseconds_bucket{le="100"} 123
request_duration_microseconds_bucket{le="120"} 412
request_duration_microseconds_bucket{le="144"} 592
request_duration_microseconds_bucket{le="172.8"} 1524
request_duration_microseconds_bucket{le="+Inf"} 2693
request_duration_microseconds_sum 1.7560473e+06
request_duration_microseconds_count -2693.123
"#;

pub const ERROR_SCENARIO_46_INPUT: &str = r#"
# HELP request_duration_microseconds The response latency.
# TYPE request_duration_microseconds histogram
request_duration_microseconds_bucket{le="100"} 123
request_duration_microseconds_bucket{le="120"} -412.456
request_duration_microseconds_bucket{le="144"} 592
request_duration_microseconds_bucket{le="172.8"} 1524
request_duration_microseconds_bucket{le="+Inf"} 2693
request_duration_microseconds_sum 1.7560473e+06
request_duration_microseconds_count 2693
"#;

pub const ERROR_SCENARIO_47_INPUT: &str = r#"{} 3.14"#;

pub const SUMMARY_NAN_QUANTILE_INPUT: &str = r#"# TYPE s summary
s{quantile="NaN"} 42
"#;

pub const SUMMARY_NEGATIVE_COUNT_INPUT: &str = r#"# TYPE s summary
s_count -5
"#;

pub const SUMMARY_INFINITE_QUANTILE_INPUT: &str = r#"# TYPE s summary
s{quantile="+Inf"} 7
"#;

pub const SUMMARY_FRACTIONAL_COUNT_INPUT: &str = r#"# TYPE s summary
s_count 42.9
"#;

pub const STRICT_ABORT_PARTIAL_INPUT: &str = r#"# TYPE h histogram
h_bucket{le="1"} 10
h_count -5
h_bucket{le="+Inf"} 20
"#;

pub const SUMMARY_PERMUTED_LABELS_INPUT: &str = r#"# TYPE s summary
s{a="1",b="2",quantile="0.5"} 1
s{b="2",a="1",quantile="0.9"} 2
"#;

/// Every scenario payload with a short label for assertion messages.
pub fn scenario_inputs() -> Vec<(&'static str, &'static [u8])> {
    vec![
        ("scenario_0", SCENARIO_0_INPUT.as_bytes()),
        ("scenario_1", SCENARIO_1_INPUT.as_bytes()),
        ("scenario_2", SCENARIO_2_INPUT.as_bytes()),
        ("scenario_3", SCENARIO_3_INPUT.as_bytes()),
        ("scenario_4", SCENARIO_4_INPUT.as_bytes()),
        ("scenario_5", SCENARIO_5_INPUT.as_bytes()),
        ("scenario_6", SCENARIO_6_INPUT.as_bytes()),
        ("scenario_7", SCENARIO_7_INPUT.as_bytes()),
        ("scenario_8", SCENARIO_8_INPUT.as_bytes()),
        ("scenario_9", SCENARIO_9_INPUT.as_bytes()),
        ("scenario_10", SCENARIO_10_INPUT.as_bytes()),
        ("scenario_11", SCENARIO_11_INPUT.as_bytes()),
        ("scenario_12", SCENARIO_12_INPUT.as_bytes()),
        ("scenario_13", SCENARIO_13_INPUT.as_bytes()),
        ("scenario_14", SCENARIO_14_INPUT.as_bytes()),
        ("scenario_15", SCENARIO_15_INPUT.as_bytes()),
        ("scenario_16", SCENARIO_16_INPUT.as_bytes()),
        ("scenario_17", SCENARIO_17_INPUT.as_bytes()),
        ("error_scenario_0", ERROR_SCENARIO_0_INPUT.as_bytes()),
        ("error_scenario_1", ERROR_SCENARIO_1_INPUT.as_bytes()),
        ("error_scenario_2", ERROR_SCENARIO_2_INPUT.as_bytes()),
        ("error_scenario_3", ERROR_SCENARIO_3_INPUT.as_bytes()),
        ("error_scenario_4", ERROR_SCENARIO_4_INPUT.as_bytes()),
        ("error_scenario_5", ERROR_SCENARIO_5_INPUT.as_bytes()),
        ("error_scenario_6", ERROR_SCENARIO_6_INPUT.as_bytes()),
        ("error_scenario_7", ERROR_SCENARIO_7_INPUT.as_bytes()),
        ("error_scenario_8", ERROR_SCENARIO_8_INPUT.as_bytes()),
        ("error_scenario_9", ERROR_SCENARIO_9_INPUT.as_bytes()),
        ("error_scenario_10", ERROR_SCENARIO_10_INPUT.as_bytes()),
        ("error_scenario_11", ERROR_SCENARIO_11_INPUT.as_bytes()),
        ("error_scenario_12", ERROR_SCENARIO_12_INPUT.as_bytes()),
        ("error_scenario_13", ERROR_SCENARIO_13_INPUT.as_bytes()),
        ("error_scenario_14", ERROR_SCENARIO_14_INPUT.as_bytes()),
        ("error_scenario_15", ERROR_SCENARIO_15_INPUT.as_bytes()),
        ("error_scenario_16", ERROR_SCENARIO_16_INPUT.as_bytes()),
        ("error_scenario_17", ERROR_SCENARIO_17_INPUT.as_bytes()),
        ("error_scenario_18", ERROR_SCENARIO_18_INPUT.as_bytes()),
        ("error_scenario_19", ERROR_SCENARIO_19_INPUT.as_bytes()),
        ("error_scenario_20", ERROR_SCENARIO_20_INPUT),
        ("error_scenario_21", ERROR_SCENARIO_21_INPUT.as_bytes()),
        ("error_scenario_22", ERROR_SCENARIO_22_INPUT.as_bytes()),
        ("error_scenario_23", ERROR_SCENARIO_23_INPUT.as_bytes()),
        ("error_scenario_24", ERROR_SCENARIO_24_INPUT.as_bytes()),
        ("error_scenario_25", ERROR_SCENARIO_25_INPUT.as_bytes()),
        ("error_scenario_26", ERROR_SCENARIO_26_INPUT.as_bytes()),
        ("error_scenario_27", ERROR_SCENARIO_27_INPUT.as_bytes()),
        ("error_scenario_28", ERROR_SCENARIO_28_INPUT.as_bytes()),
        ("error_scenario_29", ERROR_SCENARIO_29_INPUT.as_bytes()),
        ("error_scenario_30", ERROR_SCENARIO_30_INPUT.as_bytes()),
        ("error_scenario_31", ERROR_SCENARIO_31_INPUT.as_bytes()),
        ("error_scenario_32", ERROR_SCENARIO_32_INPUT.as_bytes()),
        ("error_scenario_33", ERROR_SCENARIO_33_INPUT.as_bytes()),
        ("error_scenario_34", ERROR_SCENARIO_34_INPUT.as_bytes()),
        ("error_scenario_35", ERROR_SCENARIO_35_INPUT.as_bytes()),
        ("error_scenario_36", ERROR_SCENARIO_36_INPUT.as_bytes()),
        ("error_scenario_37", ERROR_SCENARIO_37_INPUT.as_bytes()),
        ("error_scenario_38", ERROR_SCENARIO_38_INPUT.as_bytes()),
        ("error_scenario_39", ERROR_SCENARIO_39_INPUT.as_bytes()),
        ("error_scenario_40", ERROR_SCENARIO_40_INPUT.as_bytes()),
        ("error_scenario_41", ERROR_SCENARIO_41_INPUT.as_bytes()),
        ("error_scenario_42", ERROR_SCENARIO_42_INPUT.as_bytes()),
        ("error_scenario_43", ERROR_SCENARIO_43_INPUT.as_bytes()),
        ("error_scenario_44", ERROR_SCENARIO_44_INPUT.as_bytes()),
        ("error_scenario_45", ERROR_SCENARIO_45_INPUT.as_bytes()),
        ("error_scenario_46", ERROR_SCENARIO_46_INPUT.as_bytes()),
        ("error_scenario_47", ERROR_SCENARIO_47_INPUT.as_bytes()),
        (
            "summary_nan_quantile",
            SUMMARY_NAN_QUANTILE_INPUT.as_bytes(),
        ),
        (
            "summary_negative_count",
            SUMMARY_NEGATIVE_COUNT_INPUT.as_bytes(),
        ),
        (
            "summary_infinite_quantile",
            SUMMARY_INFINITE_QUANTILE_INPUT.as_bytes(),
        ),
        (
            "summary_fractional_count",
            SUMMARY_FRACTIONAL_COUNT_INPUT.as_bytes(),
        ),
        (
            "strict_abort_partial",
            STRICT_ABORT_PARTIAL_INPUT.as_bytes(),
        ),
        (
            "summary_permuted_labels",
            SUMMARY_PERMUTED_LABELS_INPUT.as_bytes(),
        ),
    ]
}
