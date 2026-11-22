//! Standalone example: scrape one Prometheus text exposition endpoint
//! with reqwest and parse the whole body with the unified `Parser`.

use prometheus_scrape_parser_rs::{LenientMode, MetricFamilyView, MetricView, Parser};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://localhost:9090/metrics".to_string());

    let body = reqwest::get(&url)
        .await?
        .error_for_status()?
        .bytes()
        .await?;

    let mut parser = Parser::new();
    for family in parser.parse_text(&body, LenientMode) {
        println!("{} ({:?})", family.name(), family.metric_type());
        if let Some(help) = family.help() {
            println!("  # {help}");
        }
        for metric in family.metrics() {
            for label in metric.labels() {
                println!("  {}={}", label.name, label.value);
            }
            println!("  value: {:?}", metric.value());
        }
    }

    println!("{:?}", parser.stats());
    Ok(())
}
