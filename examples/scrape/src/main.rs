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

    let mut parser = Parser::new(LenientMode);
    for family in parser.parse_text(&body) {
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

    // End of stream: folds the tail, finalizes the stats below.
    parser.reset()?;
    println!("{:?}", parser.stats());
    Ok(())
}
