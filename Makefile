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
