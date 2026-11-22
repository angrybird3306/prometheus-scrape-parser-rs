//! The [`Parser`] facade: one stateful parser can parse both text and proto
//! formats of metrics payloads.

use crate::Stats;
use crate::mode::Mode;
use crate::model::MetricFamily;
use crate::parse_proto::{ProtoBufParser, ProtoFamily};
use crate::parse_text::{StageArea, TextParser};

pub struct Parser {
    stage: StageArea,
    proto: ProtoBufParser,
    last_stats: Stats,
}

impl Default for Parser {
    fn default() -> Self {
        Self::new()
    }
}

impl Parser {
    pub fn new() -> Self {
        Self {
            stage: StageArea::default(),
            proto: ProtoBufParser::new(),
            last_stats: Stats::default(),
        }
    }

    /// Feeds one chunk of a text exposition stream and yields its
    /// families; a line the chunk boundary cut in half stages and
    /// completes with the next call. Families borrow `buf`.
    pub fn parse_text<'a, M: Mode>(
        &mut self,
        buf: &'a [u8],
        mode: M,
    ) -> impl Iterator<Item = MetricFamily<'a>> {
        let stage = std::mem::take(&mut self.stage);
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
            proto, last_stats, ..
        } = self;
        proto.parse_into(buf, last_stats)
    }

    /// Whether a staged partial — a text cut line or a proto partial
    /// message — is still waiting for its continuation bytes. At a true
    /// end of stream this being `true` means the final partial never
    /// completed; [`reset`](Self::reset) discards it.
    pub fn is_waiting(&self) -> bool {
        self.stage.has_staged_line() || self.proto.is_waiting()
    }

    /// Discards the whole chained state — the staged partials, the carried
    /// families, the metadata memory, the stats and line numbering — and
    /// returns to the fresh-[`new`](Self::new) shape; staged buffer
    /// capacities are kept. The next parse call starts a fresh stream.
    pub fn reset(&mut self) {
        self.stage.reset();
        self.proto.reset();
        self.last_stats = Stats::default();
    }

    /// The running totals of the format driven last: written back each
    /// time an iteration ends (or is dropped), chained across calls of
    /// that format.
    pub fn stats(&self) -> &Stats {
        &self.last_stats
    }
}

struct TextIter<'a, 'p, M: Mode> {
    parent: &'p mut Parser,
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
    /// Hands the staged state and the running stats back to the parser.
    fn write_back(&mut self) {
        if let Some(parser) = self.inner.take() {
            self.parent.last_stats = parser.stats().clone();
            let (stage, _) = parser.release();
            self.parent.stage = stage;
        }
    }
}

impl<M: Mode> Drop for TextIter<'_, '_, M> {
    fn drop(&mut self) {
        self.write_back();
    }
}
