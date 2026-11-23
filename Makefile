# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The prometheus-scrape-parser-rs Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

.PHONY: lint test generate

# Auto-fix what can be fixed, then verify strictly: formatting must be
# canonical and clippy warnings are errors.
lint:
	cargo clippy --fix --allow-dirty --all-targets
	cargo fmt --all
	cargo fmt --all -- --check
	cargo clippy --all-targets -- -D warnings
	git status

test:
	cargo test --workspace

generate:
	buf generate buf.build/prometheus/prometheus --path io/prometheus/client/metrics.proto
