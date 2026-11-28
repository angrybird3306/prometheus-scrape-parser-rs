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

//! The [`Parser`] facade: one stateful parser drives both text and proto
//! formats of metrics payloads, chaining each format's streaming state
//! across calls and ending the stream with [`Parser::reset`].

use crate::error::Error;
use crate::model::MetricFamily;
use crate::parse_proto::{ProtoBufParser, ProtoFamily};
use crate::parse_text::{StageArea, TextParser};
use crate::{Mode, Stats, StrictMode};

pub struct Parser<M: Mode = StrictMode> {
    /// The text format's staged state, `None` while a `parse_text`
    /// iteration holds it — the same in-flight invariant as `mode`.
    stage: Option<StageArea>,
    /// The mode instance travels with the stream, so its validation
    /// state — `StrictMode`'s per-family rules — chains across calls;
    /// `None` while a `parse_text` iteration holds it.
    mode: Option<M>,
    proto_parser: ProtoBufParser,
    stats: Stats,
}

impl<M: Mode + Default> Default for Parser<M> {
    fn default() -> Self {
        Self::new(M::default())
    }
}

impl<M: Mode> Parser<M> {
    pub fn new(mode: M) -> Self {
        Self {
            stage: None,
            mode: Some(mode),
            proto_parser: ProtoBufParser::new(),
            stats: Stats::default(),
        }
    }

    /// Feeds one chunk of a text exposition stream and yields its
    /// families; a line the chunk boundary cut in half stages and
    /// completes with the next call, and an aggregation family the chunk
    /// leaves still gathering its sample group stays held with the stage
    /// instead of yielding a fragment. The mode instance travels with the
    /// stream — its validation state chains across calls, so
    /// `StrictMode`'s per-family rules hold exactly over a chunked feed.
    /// End the stream with [`reset`](Self::reset). Families borrow `buf`.
    /// Dropping the iterator before it runs dry discards the families the
    /// chunk still holds; the stats and the staged state survive.
    pub fn parse_text<'a>(&mut self, buf: &'a [u8]) -> impl Iterator<Item = MetricFamily<'a>> {
        let stage = self.stage.take().unwrap_or_default();
        let mode = self
            .mode
            .take()
            .expect("mode written back when TextIter drops");
        TextIter {
            parent: self,
            inner: Some(TextParser::new(stage, mode, buf)),
        }
    }

    /// Feeds one chunk of a protobuf exposition stream and yields the
    /// families of the messages it completes; a message the chunk boundary
    /// cut in half stages and completes with the next call. Families
    /// borrow `buf`, except a message completed across the boundary, which
    /// carries owned strings.
    pub fn parse_proto<'a>(&mut self, buf: &'a [u8]) -> impl Iterator<Item = ProtoFamily<'a>> {
        let Parser {
            proto_parser: proto,
            stats,
            ..
        } = self;
        proto.parse_into(buf, stats)
    }

    /// Whether a staged partial — a text cut line or a proto partial
    /// message — is still waiting for its continuation bytes.
    pub fn is_waiting(&self) -> bool {
        self.stage.as_ref().is_some_and(StageArea::has_staged_line)
            || self.proto_parser.is_waiting()
    }

    /// Ends the stream: the staged text tail folds as the unterminated
    /// final line, its families counting into
    /// [`Stats::discarded_families`] without yielding — so does an
    /// aggregation family the stream ends on while it still gathers its
    /// sample group (rows never yield half a sample group); the chained
    /// state — the mode's validation state included — clears for a fresh
    /// stream, capacities kept, [`stats`](Self::stats) keeping the final
    /// totals.
    ///
    /// `Err` reports an unclean end: a truncated tail, a tail erroring
    /// under the fold (its own error then owns the end), or a staged proto
    /// partial message. The finalized totals can be cleared with
    /// [`reset_stats`](Self::reset_stats).
    pub fn reset(&mut self) -> Result<(), Error> {
        let mut out = Ok(());

        if self
            .stage
            .as_ref()
            .is_some_and(|stage| stage.has_staged_line() || stage.has_carried_family())
        {
            let stage = self.stage.take().unwrap();
            let mode = self
                .mode
                .take()
                .expect("mode written back when TextIter drops");
            let mut parser = TextParser::new(stage, mode, b"");
            let before = parser.stats().last_error.clone();
            let mut stats = parser.finish();
            while parser.next_family().is_some() {
                stats.discarded_families += 1;
            }
            if stats.last_error != before {
                out = Err(stats.last_error.clone().expect("error set by the fold"));
            }
            let (stage, mode) = parser.release();
            self.stage = Some(stage);
            self.mode = Some(mode);
            self.stats = stats;
        }

        if let Some(stage) = &mut self.stage {
            stage.reset();
        }
        let proto_ended = self.proto_parser.reset();
        if out.is_ok()
            && let Some(error) = proto_ended
        {
            out = Err(error);
        }
        if let Some(mode) = &mut self.mode {
            mode.reset();
        }
        out
    }

    /// Clears the stats.
    pub fn reset_stats(&mut self) {
        self.stats = Stats::default();
        if let Some(stage) = &mut self.stage {
            stage.reset_stats();
        }
        self.proto_parser.reset_stats();
    }

    /// Read current stats.
    pub fn stats(&self) -> &Stats {
        &self.stats
    }
}

struct TextIter<'a, 'p, M: Mode> {
    parent: &'p mut Parser<M>,
    inner: Option<TextParser<'a, M>>,
}

impl<'a, M: Mode> Iterator for TextIter<'a, '_, M> {
    type Item = MetricFamily<'a>;

    fn next(&mut self) -> Option<MetricFamily<'a>> {
        let parser = self.inner.as_mut()?;
        parser.next_family()
    }
}

impl<'a, M: Mode> TextIter<'a, '_, M> {
    /// Hands the staged state, the mode and the running stats back to
    /// the parser.
    fn write_back(&mut self) {
        if let Some(parser) = self.inner.take() {
            self.parent.stats = parser.stats().clone();
            let (stage, mode) = parser.release();
            self.parent.stage = Some(stage);
            self.parent.mode = Some(mode);
        }
    }
}

impl<M: Mode> Drop for TextIter<'_, '_, M> {
    fn drop(&mut self) {
        self.write_back();
    }
}
