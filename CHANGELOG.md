# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
Fixed:
- fix: a chunk that completes no line split the aggregation family carried across it into fragments and dropped the last one at reset
- fix: parsing histogram & summary should match with name pattern strictly
- fix: histogram count f64 overflow can be cast back
- fix: make sure summary count parse like golang expfmt
- fix: reject spurious blank in strict mode
- fix: validate every le & quantile label
- fix: skip histogram & summary lines with illegal le/quantile label
- fix: skip illegal lines in lenient mode
- fix: fail on truncated final line in lenient mode
- fix: yield complete family before staging truncated line
- fix: yield complete family for interleaved histogram & summary
- fix: reset_stats should clear internal states as well
- fix: parse segarated protobuf inputs

Added:
- feat: more accurate error messages

## [0.2.1] - 2026-11-26
Fixed:
- fix: a summary/histogram family split across chunks is parsed as one

## [0.2.0] - 2026-11-26
Added:
- feat: new method of Parser to reset stats

## [0.1.1] - 2026-11-23
Fixed:
- fix: Parser facade supports chunked messages

## [0.1.0] - 2026-11-23
Added:
- Opensource this project.
