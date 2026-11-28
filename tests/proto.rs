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

//! Protobuf exposition parsing: fixtures are built with the generated dto
//! types and encoded through their own delimited writer, then parsed with
//! [`ProtoBufParser`] and asserted through the shared view traits.

mod common;

use buffa::{Message, MessageField, UnknownFields};
use prometheus_scrape_parser_rs::proto::io::prometheus::client as dto;
use prometheus_scrape_parser_rs::{
    Bucket, Error, ErrorKind, HistogramValue, IntOrFloat, LabelView, MetricFamily,
    MetricFamilyView, MetricType, MetricValue, MetricView, ProtoBufParser, ProtoFamily, Quantile,
    Stats, SummaryValue,
};
use std::borrow::Cow;

/// Encodes families as one delimited stream, the wire shape a scrape
/// serves.
fn encode(families: &[dto::MetricFamily]) -> Vec<u8> {
    let mut out = Vec::new();
    for family in families {
        family.encode_length_delimited(&mut out);
    }
    out
}

fn label(name: &str, value: &str) -> dto::LabelPair {
    dto::LabelPair {
        name: name.to_owned(),
        value: value.to_owned(),
        __buffa_unknown_fields: UnknownFields::new(),
    }
}

fn family(
    name: &str,
    help: &str,
    metric_type: dto::MetricType,
    metrics: Vec<dto::Metric>,
) -> dto::MetricFamily {
    dto::MetricFamily {
        name: name.to_owned(),
        help: help.to_owned(),
        r#type: metric_type.into(),
        metric: metrics,
        unit: String::new(),
        __buffa_unknown_fields: UnknownFields::new(),
    }
}

/// One dto Metric with every value field unset; `set` fills exactly the
/// field the family's type names.
fn metric_with(
    labels: Vec<dto::LabelPair>,
    timestamp_ms: i64,
    set: impl FnOnce(&mut dto::Metric),
) -> dto::Metric {
    let mut metric = dto::Metric {
        label: labels,
        gauge: MessageField::none(),
        counter: MessageField::none(),
        summary: MessageField::none(),
        untyped: MessageField::none(),
        histogram: MessageField::none(),
        timestamp_ms,
        __buffa_unknown_fields: UnknownFields::new(),
    };
    set(&mut metric);
    metric
}

fn counter(labels: Vec<dto::LabelPair>, value: f64, timestamp_ms: i64) -> dto::Metric {
    metric_with(labels, timestamp_ms, |metric| {
        metric.counter = MessageField::some(dto::Counter {
            value,
            exemplar: MessageField::none(),
            start_timestamp: MessageField::none(),
            __buffa_unknown_fields: UnknownFields::new(),
        });
    })
}

fn gauge(labels: Vec<dto::LabelPair>, value: f64, timestamp_ms: i64) -> dto::Metric {
    metric_with(labels, timestamp_ms, |metric| {
        metric.gauge = MessageField::some(dto::Gauge {
            value,
            __buffa_unknown_fields: UnknownFields::new(),
        });
    })
}

fn untyped(labels: Vec<dto::LabelPair>, value: f64, timestamp_ms: i64) -> dto::Metric {
    metric_with(labels, timestamp_ms, |metric| {
        metric.untyped = MessageField::some(dto::Untyped {
            value,
            __buffa_unknown_fields: UnknownFields::new(),
        });
    })
}

fn summary(
    labels: Vec<dto::LabelPair>,
    timestamp_ms: i64,
    count: u64,
    sum: f64,
    quantiles: Vec<dto::Quantile>,
) -> dto::Metric {
    metric_with(labels, timestamp_ms, |metric| {
        metric.summary = MessageField::some(dto::Summary {
            sample_count: count,
            sample_sum: sum,
            quantile: quantiles,
            start_timestamp: MessageField::none(),
            __buffa_unknown_fields: UnknownFields::new(),
        });
    })
}

fn quantile(quantile: f64, value: f64) -> dto::Quantile {
    dto::Quantile {
        quantile,
        value,
        __buffa_unknown_fields: UnknownFields::new(),
    }
}

fn bucket(count: u64, count_float: f64, upper_bound: f64) -> dto::Bucket {
    dto::Bucket {
        cumulative_count: count,
        cumulative_count_float: count_float,
        upper_bound,
        exemplar: MessageField::none(),
        __buffa_unknown_fields: UnknownFields::new(),
    }
}

#[allow(clippy::too_many_arguments)]
fn histogram(
    labels: Vec<dto::LabelPair>,
    timestamp_ms: i64,
    count: u64,
    count_float: f64,
    sum: f64,
    buckets: Vec<dto::Bucket>,
) -> dto::Metric {
    metric_with(labels, timestamp_ms, |metric| {
        metric.histogram = MessageField::some(dto::Histogram {
            sample_count: count,
            sample_count_float: count_float,
            sample_sum: sum,
            bucket: buckets,
            start_timestamp: MessageField::none(),
            schema: 0,
            zero_threshold: 0.0,
            zero_count: 0,
            zero_count_float: 0.0,
            negative_span: Vec::new(),
            negative_delta: Vec::new(),
            negative_count: Vec::new(),
            positive_span: Vec::new(),
            positive_delta: Vec::new(),
            positive_count: Vec::new(),
            exemplars: Vec::new(),
            __buffa_unknown_fields: UnknownFields::new(),
        });
    })
}

/// Drains one whole-buffer parse, returning the families plus final stats.
fn parse<'a>(input: &'a [u8]) -> (Vec<ProtoFamily<'a>>, Stats) {
    parse_chunks(&[input])
}

/// Feeds the pieces as successive chunks of one stream, concatenating the
/// yields — the chunked invariance the facade and examples rely on.
fn parse_chunks<'a>(chunks: &[&'a [u8]]) -> (Vec<ProtoFamily<'a>>, Stats) {
    let mut parser = ProtoBufParser::new();
    let mut families = Vec::new();
    for chunk in chunks {
        for family in parser.parse(chunk) {
            families.push(family);
        }
    }
    (families, parser.stats().clone())
}

/// Materializes families through the view trait for whole-content
/// comparison; a chunked feed completes its boundary-crossing message as
/// the `Owned` variant, so the views themselves never compare equal to
/// the all-wire whole-buffer yield. Borrow-ness is asserted separately
/// where it matters.
fn owned(families: &[ProtoFamily<'_>]) -> Vec<MetricFamily<'static>> {
    common::owned(families)
}

/// Splits `input` at assorted byte offsets — mid-prefix, mid-varint, and
/// mid-message — into chunk pieces.
fn assorted_chunks(input: &[u8]) -> Vec<&[u8]> {
    let mut cuts = vec![1usize, 2, 3, 7, 11, input.len() / 2, input.len() - 2];
    cuts.sort_unstable();
    cuts.dedup();
    cuts.retain(|cut| *cut > 0 && *cut < input.len());
    let mut chunks = Vec::new();
    let mut last = 0;
    for cut in cuts {
        if cut > last {
            chunks.push(&input[last..cut]);
            last = cut;
        }
    }
    chunks.push(&input[last..]);
    chunks
}

#[test]
fn proto_counter_family_borrows_names_and_yields_metrics() {
    let input = encode(&[family(
        "http_requests_total",
        "Requests total.",
        dto::MetricType::COUNTER,
        vec![
            counter(vec![label("method", "get")], 1027.0, 1234),
            counter(vec![label("method", "post")], 3.0, 0),
        ],
    )]);
    let (families, stats) = parse(&input);
    assert_eq!(families.len(), 1);
    assert!(matches!(families[0].name(), Cow::Borrowed(_)));
    assert_eq!(families[0].name(), "http_requests_total");
    assert_eq!(families[0].help().as_deref(), Some("Requests total."));
    assert_eq!(families[0].metric_type(), MetricType::Counter);
    assert_eq!(families[0].metric_count(), 2);
    let metrics: Vec<_> = families[0].metrics().collect();
    assert_eq!(
        metrics[0].labels().collect::<Vec<_>>(),
        vec![LabelView {
            name: Cow::Borrowed("method"),
            value: Cow::Borrowed("get"),
        }]
    );
    assert_eq!(metrics[0].timestamp(), Some(1234));
    assert_eq!(metrics[0].value(), MetricValue::Sample(1027.0));
    assert_eq!(metrics[1].timestamp(), None);
    assert_eq!(stats.metrics_total, 2);
    assert_eq!(stats.last_error, None);
}

#[test]
fn proto_chunked_feed_matches_whole_buffer() {
    // Arbitrary chunk cuts land mid-prefix, mid-varint, and mid-message;
    // the staged reassembly must reproduce the whole-buffer yield.
    let input = encode(&[
        family(
            "c",
            "With help.",
            dto::MetricType::COUNTER,
            vec![counter(vec![label("k", "v")], 1.0, 5)],
        ),
        family(
            "h",
            "",
            dto::MetricType::HISTOGRAM,
            vec![histogram(
                Vec::new(),
                0,
                2,
                0.0,
                3.25,
                vec![bucket(1, 0.0, 1.0), bucket(2, 0.0, f64::INFINITY)],
            )],
        ),
    ]);
    let (whole, whole_stats) = parse(&input);
    let (families, stats) = parse_chunks(&assorted_chunks(&input));
    assert_eq!(owned(&families), owned(&whole));
    assert_eq!(stats, whole_stats);
}

#[test]
fn proto_cross_chunk_message_completes_owned() {
    // A message the chunk boundary cut in half completes from the second
    // chunk through the owned path: its strings carry `Cow::Owned`.
    let input = encode(&[family(
        "c",
        "",
        dto::MetricType::COUNTER,
        vec![counter(Vec::new(), 1.0, 0)],
    )]);
    let cut = input.len() - 10;
    let mut parser = ProtoBufParser::new();
    let mut families = Vec::new();
    for family in parser.parse(&input[..cut]) {
        families.push(family);
    }
    assert!(families.is_empty());
    assert!(parser.is_waiting());
    assert_eq!(parser.stats(), &Stats::default());
    for family in parser.parse(&input[cut..]) {
        families.push(family);
    }
    assert_eq!(families.len(), 1);
    assert!(matches!(families[0].name(), Cow::Owned(_)));
    assert_eq!(families[0].name(), "c");
    let metrics: Vec<_> = families[0].metrics().collect();
    assert_eq!(metrics[0].value(), MetricValue::Sample(1.0));
    assert!(!parser.is_waiting());
    assert_eq!(parser.stats().metrics_total, 1);
    assert_eq!(parser.stats().last_error, None);
}

#[test]
fn proto_stats_chain_across_chunks() {
    let input = encode(&[
        family(
            "a",
            "",
            dto::MetricType::COUNTER,
            vec![counter(Vec::new(), 1.0, 0)],
        ),
        family(
            "b",
            "",
            dto::MetricType::GAUGE,
            vec![gauge(Vec::new(), 2.0, 0)],
        ),
    ]);
    let mid = input.len() / 2;
    let mut parser = ProtoBufParser::new();
    let mut yielded = 0;
    for family in parser.parse(&input[..mid]) {
        yielded += family.metric_count();
    }
    for family in parser.parse(&input[mid..]) {
        yielded += family.metric_count();
    }
    assert_eq!(yielded, 2);
    assert_eq!(parser.stats().metrics_total, 2);
    assert_eq!(parser.stats().last_error, None);
}

#[test]
fn proto_reset_discards_staged_and_stats() {
    let input = encode(&[family(
        "c",
        "",
        dto::MetricType::COUNTER,
        vec![counter(Vec::new(), 1.0, 0)],
    )]);
    let cut = input.len() - 3;
    let mut parser = ProtoBufParser::new();
    for family in parser.parse(&input[..cut]) {
        drop(family);
    }
    assert!(parser.is_waiting());
    // The staged partial reports as the truncation it now turns out to
    // be, at the ordinal it would have completed under.
    assert_eq!(
        parser.reset(),
        Some(Error {
            line: 1,
            kind: ErrorKind::InvalidProtoMessage {
                got: "truncated final message".to_owned(),
            },
        })
    );
    assert!(!parser.is_waiting());
    assert_eq!(parser.stats(), &Stats::default());

    // After the reset the same bytes parse as a fresh stream.
    let mut families = Vec::new();
    for family in parser.parse(&input) {
        families.push(family);
    }
    assert_eq!(families.len(), 1);
    assert_eq!(parser.stats().metrics_total, 1);
}

#[test]
fn proto_gauge_and_untyped_families_keep_sample_values() {
    let input = encode(&[
        family(
            "temperature",
            "",
            dto::MetricType::GAUGE,
            vec![gauge(Vec::new(), 21.5, 0)],
        ),
        family(
            "size",
            "",
            dto::MetricType::UNTYPED,
            vec![untyped(Vec::new(), 7.0, 0)],
        ),
    ]);
    let (families, _) = parse(&input);
    assert_eq!(families.len(), 2);
    assert_eq!(families[0].metric_type(), MetricType::Gauge);
    let first: Vec<_> = families[0].metrics().collect();
    assert_eq!(first[0].value(), MetricValue::Sample(21.5));
    assert_eq!(families[1].metric_type(), MetricType::Untyped);
    let second: Vec<_> = families[1].metrics().collect();
    assert_eq!(second[0].value(), MetricValue::Sample(7.0));
}

#[test]
fn proto_summary_carries_quantiles_and_counts() {
    let input = encode(&[family(
        "latency",
        "",
        dto::MetricType::SUMMARY,
        vec![summary(
            Vec::new(),
            0,
            10,
            100.5,
            vec![quantile(0.5, 1.5), quantile(0.9, 2.5)],
        )],
    )]);
    let (families, _) = parse(&input);
    assert_eq!(families.len(), 1);
    assert_eq!(families[0].metric_type(), MetricType::Summary);
    let metrics: Vec<_> = families[0].metrics().collect();
    assert_eq!(
        metrics[0].value(),
        MetricValue::Summary(SummaryValue {
            sample_count: Some(10),
            sample_sum: Some(100.5),
            quantiles: vec![
                Quantile {
                    quantile: 0.5,
                    value: 1.5
                },
                Quantile {
                    quantile: 0.9,
                    value: 2.5
                },
            ],
        })
    );
}

#[test]
fn proto_histogram_keeps_int_counts() {
    let input = encode(&[family(
        "size",
        "",
        dto::MetricType::HISTOGRAM,
        vec![histogram(
            Vec::new(),
            0,
            2,
            0.0,
            3.25,
            vec![bucket(1, 0.0, 1.0), bucket(2, 0.0, f64::INFINITY)],
        )],
    )]);
    let (families, _) = parse(&input);
    let metrics: Vec<_> = families[0].metrics().collect();
    assert_eq!(
        metrics[0].value(),
        MetricValue::Histogram(HistogramValue {
            sample_count: Some(IntOrFloat::Int(2)),
            sample_sum: Some(3.25),
            buckets: vec![
                Bucket {
                    upper_bound: 1.0,
                    cumulative_count: IntOrFloat::Int(1),
                },
                Bucket {
                    upper_bound: f64::INFINITY,
                    cumulative_count: IntOrFloat::Int(2),
                },
            ],
        })
    );
}

#[test]
fn proto_histogram_float_twin_overrides_int_counts() {
    // The dto's float twin fields override the int fields when positive;
    // one float bucket must not switch the int-only buckets.
    let input = encode(&[family(
        "size",
        "",
        dto::MetricType::HISTOGRAM,
        vec![histogram(
            Vec::new(),
            0,
            0,
            2.5,
            3.25,
            vec![bucket(0, 1.5, 1.0), bucket(2, 0.0, f64::INFINITY)],
        )],
    )]);
    let (families, _) = parse(&input);
    let metrics: Vec<_> = families[0].metrics().collect();
    assert_eq!(
        metrics[0].value(),
        MetricValue::Histogram(HistogramValue {
            sample_count: Some(IntOrFloat::Float(2.5)),
            sample_sum: Some(3.25),
            buckets: vec![
                Bucket {
                    upper_bound: 1.0,
                    cumulative_count: IntOrFloat::Float(1.5),
                },
                Bucket {
                    upper_bound: f64::INFINITY,
                    cumulative_count: IntOrFloat::Int(2),
                },
            ],
        })
    );
}

#[test]
fn proto_gauge_histogram_projects_the_histogram_field() {
    // The classic dto has no gauge-histogram message; the family's type
    // selects the histogram field and the model type.
    let input = encode(&[family(
        "rps",
        "",
        dto::MetricType::GAUGE_HISTOGRAM,
        vec![histogram(
            Vec::new(),
            0,
            1,
            0.0,
            1.0,
            vec![bucket(1, 0.0, f64::INFINITY)],
        )],
    )]);
    let (families, _) = parse(&input);
    assert_eq!(families.len(), 1);
    assert_eq!(families[0].metric_type(), MetricType::GaugeHistogram);
    assert_eq!(families[0].metric_count(), 1);
}

#[test]
fn proto_families_yield_in_input_order() {
    let input = encode(&[
        family(
            "c",
            "",
            dto::MetricType::COUNTER,
            vec![counter(Vec::new(), 1.0, 0)],
        ),
        family(
            "a",
            "",
            dto::MetricType::GAUGE,
            vec![gauge(Vec::new(), 2.0, 0)],
        ),
        family(
            "m",
            "",
            dto::MetricType::UNTYPED,
            vec![untyped(Vec::new(), 3.0, 0)],
        ),
    ]);
    let (families, _) = parse(&input);
    let names: Vec<_> = families.iter().map(|f| f.name()).collect();
    assert_eq!(names, ["c", "a", "m"]);
}

#[test]
fn proto_same_name_families_yield_separately() {
    // Two messages naming one family yield as two families: the proto
    // format folds nothing across messages.
    let input = encode(&[
        family(
            "c",
            "",
            dto::MetricType::COUNTER,
            vec![counter(Vec::new(), 1.0, 0)],
        ),
        family(
            "c",
            "",
            dto::MetricType::COUNTER,
            vec![counter(Vec::new(), 2.0, 0)],
        ),
    ]);
    let (families, stats) = parse(&input);
    assert_eq!(families.len(), 2);
    let first: Vec<_> = families[0].metrics().collect();
    assert_eq!(first[0].value(), MetricValue::Sample(1.0));
    let second: Vec<_> = families[1].metrics().collect();
    assert_eq!(second[0].value(), MetricValue::Sample(2.0));
    assert_eq!(stats.metrics_total, 2);
}

#[test]
fn proto_empty_input_yields_nothing() {
    let (families, stats) = parse(b"");
    assert!(families.is_empty());
    assert_eq!(stats, Stats::default());
}

#[test]
fn proto_missing_type_decodes_as_counter() {
    // proto3 implicit presence: an unset enum field reads back as its
    // default, COUNTER.
    let input = encode(&[family(
        "c",
        "",
        dto::MetricType::COUNTER,
        vec![counter(Vec::new(), 1.0, 0)],
    )]);
    let (families, _) = parse(&input);
    assert_eq!(families[0].metric_type(), MetricType::Counter);
}

#[test]
fn proto_missing_help_and_zero_timestamp_report_none() {
    let input = encode(&[family(
        "c",
        "",
        dto::MetricType::COUNTER,
        vec![counter(Vec::new(), 1.0, 0)],
    )]);
    let (families, _) = parse(&input);
    assert_eq!(families[0].help(), None);
    let metrics: Vec<_> = families[0].metrics().collect();
    assert_eq!(metrics[0].timestamp(), None);
}

#[test]
fn proto_unknown_enum_type_drops_family_and_continues() {
    let good = family(
        "c",
        "",
        dto::MetricType::COUNTER,
        vec![counter(Vec::new(), 1.0, 0)],
    );
    let mut unknown = family("x", "", dto::MetricType::COUNTER, Vec::new());
    unknown.r#type = 99i32.into();
    let input = encode(&[unknown, good]);
    let (families, stats) = parse(&input);
    assert_eq!(families.len(), 1);
    assert_eq!(families[0].name(), "c");
    assert_eq!(stats.skipped_families, 1);
    assert_eq!(stats.metrics_total, 1);
    let error = stats.last_error.unwrap();
    assert_eq!(error.kind, ErrorKind::UnknownProtoMetricType { got: 99 },);
    assert_eq!(error.line, 1);
    assert_eq!(
        error.to_string(),
        "protobuf exposition parsing error in message 1: unknown metric type 99"
    );
}

#[test]
fn proto_malformed_length_prefix_reports_error() {
    let input = [0xFFu8; 10];
    let (families, stats) = parse(&input);
    assert!(families.is_empty());
    assert_eq!(stats.skipped_families, 1);
    assert_eq!(
        stats.last_error.unwrap().kind,
        ErrorKind::InvalidProtoMessage {
            got: "malformed length prefix".to_owned(),
        }
    );
}

#[test]
fn proto_malformed_prefix_should_stop_the_stream_until_reset() {
    // A malformed length prefix desyncs the framing: the rest of the
    // chunk drops and the parser stops — the next chunk is a fresh
    // stream only after reset, not a fresh message stream.
    let good = encode(&[family(
        "c",
        "",
        dto::MetricType::COUNTER,
        vec![counter(Vec::new(), 1.0, 0)],
    )]);
    let mut chunk1 = good.clone();
    chunk1.extend_from_slice(&[0xFFu8; 10]);
    let mut parser = ProtoBufParser::new();
    let mut families = Vec::new();
    for family in parser.parse(&chunk1) {
        families.push(family);
    }
    assert_eq!(families.len(), 1);
    assert_eq!(families[0].name(), "c");
    assert_eq!(parser.stats().skipped_families, 1);
    assert_eq!(
        parser.stats().last_error.as_ref().unwrap().kind,
        ErrorKind::InvalidProtoMessage {
            got: "malformed length prefix".to_owned(),
        }
    );
    assert!(!parser.is_waiting());

    // The desynced stream does not resume: the next chunk parses nothing
    // and the recorded error stays.
    let second = encode(&[family(
        "d",
        "",
        dto::MetricType::COUNTER,
        vec![counter(Vec::new(), 2.0, 0)],
    )]);
    assert!(parser.parse(&second).next().is_none());
    assert!(!parser.is_waiting());
    assert_eq!(parser.stats().skipped_families, 1);
    assert_eq!(parser.stats().metrics_total, 1);
    assert_eq!(
        parser.stats().last_error.as_ref().unwrap().kind,
        ErrorKind::InvalidProtoMessage {
            got: "malformed length prefix".to_owned(),
        }
    );

    // reset starts a fresh stream.
    assert_eq!(parser.reset(), None);
    for family in parser.parse(&second) {
        families.push(family);
    }
    assert_eq!(families.len(), 2);
    assert_eq!(families[1].name(), "d");
    assert_eq!(parser.stats().metrics_total, 1);
    assert_eq!(parser.stats().last_error, None);
}

#[test]
fn proto_malformed_staged_prefix_should_stop_the_stream_until_reset() {
    // The malformed prefix itself assembled across the boundary: the
    // staged continuation completes it, the stop still follows, and the
    // valid message behind it must not parse as a fresh message stream.
    let good = encode(&[family(
        "c",
        "",
        dto::MetricType::COUNTER,
        vec![counter(Vec::new(), 1.0, 0)],
    )]);
    let mut parser = ProtoBufParser::new();
    assert!(parser.parse(&[0x80u8; 5]).next().is_none());
    assert!(parser.is_waiting());

    let mut chunk2 = vec![0x80u8; 5];
    chunk2.extend_from_slice(&good);
    assert!(parser.parse(&chunk2).next().is_none());
    assert!(!parser.is_waiting());
    assert_eq!(parser.stats().skipped_families, 1);
    assert_eq!(
        parser.stats().last_error.as_ref().unwrap().kind,
        ErrorKind::InvalidProtoMessage {
            got: "malformed length prefix".to_owned(),
        }
    );
    // The rest of the completing chunk was discarded with the malformed
    // prefix — and a further chunk must not resume the desynced stream
    // as a fresh message stream either.
    assert!(parser.parse(&good).next().is_none());

    assert_eq!(parser.reset(), None);
    let families: Vec<_> = parser.parse(&good).collect();
    assert_eq!(families.len(), 1);
    assert_eq!(families[0].name(), "c");
}

#[test]
fn proto_truncated_prefix_should_complete_on_the_second_parse() {
    // A valid prefix split across the boundary assembles byte by byte:
    // the first chunk holds only the prefix's first varint byte, the
    // second completes it and the message whole. The malformed twin of
    // this shape stops the parser instead.
    let long = "x".repeat(120);
    let input = encode(&[family(
        "c",
        "",
        dto::MetricType::COUNTER,
        vec![counter(vec![label("k", &long)], 1.0, 0)],
    )]);
    assert!(
        input.len() >= 128,
        "the test needs a two-byte length prefix"
    );
    let mut parser = ProtoBufParser::new();
    assert!(parser.parse(&input[..1]).next().is_none());
    assert!(parser.is_waiting());
    assert_eq!(parser.stats(), &Stats::default());

    let families: Vec<_> = parser.parse(&input[1..]).collect();
    assert_eq!(families.len(), 1);
    assert_eq!(families[0].name(), "c");
    assert!(matches!(families[0], ProtoFamily::Owned(_)));
    let metrics: Vec<_> = families[0].metrics().collect();
    assert_eq!(metrics.len(), 1);
    assert!(!parser.is_waiting());
    assert_eq!(parser.stats().metrics_total, 1);
    assert_eq!(parser.stats().last_error, None);
}

#[test]
fn proto_stop_should_survive_reset_stats_and_block_until_reset() {
    // Clearing the stats is not a stream event: the stop is framing
    // state and keeps blocking until reset.
    let mut parser = ProtoBufParser::new();
    assert!(parser.parse(&[0xFFu8; 10]).next().is_none());
    parser.reset_stats();
    assert_eq!(parser.stats(), &Stats::default());
    let good = encode(&[family(
        "c",
        "",
        dto::MetricType::COUNTER,
        vec![counter(Vec::new(), 1.0, 0)],
    )]);
    assert!(parser.parse(&good).next().is_none());
    assert_eq!(parser.reset(), None);
    let families: Vec<_> = parser.parse(&good).collect();
    assert_eq!(families.len(), 1);
    assert_eq!(families[0].name(), "c");
}

#[test]
fn proto_corrupt_body_skips_message_and_continues() {
    // Field 1 (name) tagged with the fixed64 wire type: a wire-type
    // mismatch the decoder rejects, while the length prefix keeps the
    // stream aligned for the next message.
    let mut corrupt = vec![9u8, 0, 0, 0, 0, 0, 0, 0, 0];
    let good = encode(&[family(
        "c",
        "",
        dto::MetricType::COUNTER,
        vec![counter(Vec::new(), 1.0, 0)],
    )]);
    let mut input = vec![(corrupt.len() as u64) as u8];
    input.append(&mut corrupt);
    input.extend_from_slice(&good);
    let (families, stats) = parse(&input);
    assert_eq!(families.len(), 1);
    assert_eq!(families[0].name(), "c");
    assert_eq!(stats.skipped_families, 1);
    assert_eq!(
        stats.last_error.unwrap().kind,
        ErrorKind::InvalidProtoMessage {
            got: "wire type mismatch on field 1: expected 2, got 1".to_owned()
        }
    );
}

#[test]
fn proto_chunked_corrupt_body_should_keep_the_stream_aligned() {
    // A decode error is not a framing failure: mid-message cuts and all,
    // the corrupt message drops and the stream stays aligned for the
    // next one. Only a malformed length prefix stops the parser.
    let mut corrupt = vec![9u8, 0, 0, 0, 0, 0, 0, 0, 0];
    let good = encode(&[family(
        "c",
        "",
        dto::MetricType::COUNTER,
        vec![counter(Vec::new(), 1.0, 0)],
    )]);
    let mut input = vec![(corrupt.len() as u64) as u8];
    input.append(&mut corrupt);
    input.extend_from_slice(&good);
    let (families, stats) = parse_chunks(&assorted_chunks(&input));
    assert_eq!(families.len(), 1);
    assert_eq!(families[0].name(), "c");
    assert_eq!(stats.skipped_families, 1);
    assert!(matches!(
        stats.last_error.unwrap().kind,
        ErrorKind::InvalidProtoMessage { .. }
    ));
}

#[test]
fn proto_missing_value_drops_metric_only() {
    let input = encode(&[family(
        "c",
        "",
        dto::MetricType::COUNTER,
        vec![
            metric_with(Vec::new(), 0, |_| {}),
            counter(Vec::new(), 1.0, 0),
        ],
    )]);
    let (families, stats) = parse(&input);
    assert_eq!(families.len(), 1);
    assert_eq!(families[0].metric_count(), 1);
    assert_eq!(stats.metrics_total, 1);
    assert_eq!(stats.skipped_metrics, 1);
    assert_eq!(stats.skipped_families, 0);
    let error = stats.last_error.unwrap();
    assert_eq!(
        error.kind,
        ErrorKind::MissingMetricValue {
            family: "c".to_owned()
        }
    );
    assert_eq!(error.line, 1);
}

#[test]
fn proto_all_metrics_dropped_prunes_family() {
    let input = encode(&[family(
        "c",
        "",
        dto::MetricType::COUNTER,
        vec![metric_with(Vec::new(), 0, |_| {})],
    )]);
    let (families, stats) = parse(&input);
    assert!(families.is_empty());
    assert_eq!(stats.pruned_families, 1);
    assert_eq!(stats.skipped_metrics, 1);
}
