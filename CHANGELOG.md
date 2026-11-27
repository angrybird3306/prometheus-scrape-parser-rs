# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
Fixed:
- fix: a chunk that completes no line split the aggregation family carried across it into fragments and dropped the last one at reset

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
