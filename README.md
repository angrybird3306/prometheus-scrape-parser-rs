# prometheus-scrape-parser-rs

A parser for the two Prometheus exposition formats — classic text (0.0.4)
and delimited protobuf — ported from `expfmt` in Go's `prometheus/common`.
It parses streaming, chunked input with a single stateful facade, yields
zero-copy metric families that borrow the input buffer, and offers
lenient/strict per-line validation modes.

## Install

```toml
[dependencies]
prometheus-scrape-parser-rs = "0.1"
```

## Usage

```rust
use prometheus_scrape_parser_rs::{LenientMode, MetricFamilyView, MetricView, Parser};

let mut parser = Parser::new();
let payload = b"# TYPE http_requests_total counter\nhttp_requests_total{code=\"200\"} 1027\n";

for family in parser.parse_text(payload, LenientMode) {
    println!("{} ({:?})", family.name(), family.metric_type());
    for metric in family.metrics() {
        for label in metric.labels() {
            println!("  {}={}", label.name, label.value);
        }
        println!("  value: {:?}", metric.value());
    }
}

println!("{:?}", parser.stats());
```

## License

Licensed under the [Apache License 2.0](LICENSE).
