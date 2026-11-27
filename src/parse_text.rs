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

//! Parser for the classic text exposition format: incremental and
//! zero-copy over caller-held slices, chaining staged state across chunk
//! boundaries through [`StageArea`]; see the README.

use std::collections::HashSet;

use crate::Stats;
use crate::assemble::{Assembler, KnownMemory};
use crate::error::{Disposition, Error, ErrorKind};
use crate::lexer::{LineView, Stage, StageBorrow, StageOwned, lex_line, probe_staged_family};
use crate::mode::{Mode, Scheme};
use crate::model::{Label, MetricFamily};
use crate::search::find_byte;

fn record_skip(
    stats: &mut Stats,
    asm: &mut Assembler<'_>,
    line: u64,
    kind: ErrorKind,
    family: Option<String>,
) {
    stats.skipped_lines += 1;
    stats.last_error = Some(Error { line, kind });
    if let Some(raw) = family {
        let resolved = asm.family_name(&raw);
        asm.drop_family(resolved);
        stats.skipped_families += 1;
    }
}

/// Names the family the staged cut line folds into, without folding it.
/// Only the head is lexed — the name token cannot change as the line
/// extends — so re-probing a growing staged line stays linear.
fn pending_fold_family(
    line_bytes: &[u8],
    eol_is_newline: bool,
    scheme: Option<Scheme>,
    asm: &Assembler<'_>,
) -> Option<String> {
    let mut scratch = Vec::new();
    let name = probe_staged_family(
        line_bytes,
        eol_is_newline,
        scheme,
        &mut scratch,
        StageBorrow,
    )?;
    Some(asm.family_name(&name.decoded).to_string())
}

/// The owned state a [`TextParser`] carries across instance boundaries;
/// move it from one instance's [`TextParser::release`] into the next
/// [`TextParser::new`] to parse a stream of separately borrowed chunks.
#[derive(Default)]
pub struct StageArea {
    line: Vec<u8>,
    carried: Option<MetricFamily<'static>>,
    known: KnownMemory,
    created: HashSet<String>,
    typed: HashSet<String>,
    stats: Stats,
    line_no: u64,
    stopped: bool,
}

impl StageArea {
    pub fn has_staged_line(&self) -> bool {
        !self.line.is_empty()
    }

    /// Whether an instance ended holding a family back for the next one.
    pub(crate) fn has_carried_family(&self) -> bool {
        self.carried.is_some()
    }

    /// Clears all chained state, keeping buffer capacities.
    pub(crate) fn reset(&mut self) {
        self.line.clear();
        self.carried = None;
        self.known.clear();
        self.created.clear();
        self.typed.clear();
        self.stats = Stats::default();
        self.line_no = 0;
        self.stopped = false;
    }
}

/// Parses one borrowed slice; the yielded [`MetricFamily`] borrows the
/// input, a line the slice cuts in half stages in [`StageArea`] until its
/// newline arrives, and an aggregation family the slice leaves still
/// gathering its sample group carries with the stage.
pub struct TextParser<'a, M: Mode> {
    mode: M,
    input_buf: &'a [u8],
    pos: usize,
    staging: StageArea,
    assembler: Assembler<'a>,
    labels: Vec<Label<'a>>,
    scratch: Vec<u8>,
    line_no: u64,
    last_blank: bool,
    saw_residual: bool,
    finished: bool,
    stopped: bool,
    stats: Stats,
}

impl<'a, M: Mode> TextParser<'a, M> {
    pub fn new(mut staging: StageArea, mode: M, input: &'a [u8]) -> Self {
        let stats = std::mem::take(&mut staging.stats);
        let known = std::mem::take(&mut staging.known);
        let created = std::mem::take(&mut staging.created);
        let typed = std::mem::take(&mut staging.typed);
        let line_no = staging.line_no;
        let stopped = staging.stopped;
        let mut assembler = Assembler::default();
        assembler.restore_owned_memory(known, created, typed);
        TextParser {
            mode,
            input_buf: input,
            pos: 0,
            staging,
            assembler,
            labels: Vec::new(),
            scratch: Vec::new(),
            line_no,
            last_blank: true,
            saw_residual: false,
            finished: false,
            stopped,
            stats,
        }
    }

    /// Yields the next family in creation order, folding the whole slice
    /// on the first call; a family carried in from the previous instance
    /// yields first, the carried-out one only from the next instance or
    /// [`finish`](Self::finish).
    pub fn next_family(&mut self) -> Option<MetricFamily<'a>> {
        if !self.finished && !self.stopped {
            self.run();
            self.finished = true;
        }
        self.assembler.take_next()
    }

    /// Marks the true end of the input: the staged line (if any) folds as
    /// the unterminated final line and every family, the carried one
    /// included, yields through `next_family`. Returns the final stats.
    pub fn finish(&mut self) -> Stats {
        if !self.finished && !self.stopped {
            self.run();
            self.finished = true;
        }
        if !self.stopped {
            self.restore_carried();
            if !self.staging.line.is_empty() {
                let staged = std::mem::take(&mut self.staging.line);
                self.saw_residual = true;
                // A residual line that itself errors (strict abort included)
                // owns the final error; only a cleanly folded partial last
                // line reports the truncation.
                if self.fold_line(&staged, false, StageOwned)
                    && !self.last_blank
                    && self.mode.rejects_partial_last_line()
                {
                    self.stats.last_error = Some(Error {
                        line: self.line_no,
                        kind: ErrorKind::UnexpectedEof,
                    });
                }
            }
            self.stats.pruned_families += self.assembler.flush_ready();
            self.finished = true;
        }
        self.stats.clone()
    }

    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    /// Ends the instance mid-exposition, handing the staged state and the
    /// mode to the next [`TextParser`]. Yielded families must be consumed
    /// first — they borrow this instance's input.
    pub fn release(mut self) -> (StageArea, M) {
        self.staging.stats = std::mem::take(&mut self.stats);
        (self.staging.known, self.staging.created, self.staging.typed) =
            self.assembler.release_memory();
        self.staging.line_no = self.line_no;
        self.staging.stopped = self.stopped;
        (self.staging, self.mode)
    }

    /// Folds every complete line of the slice; the newlineless suffix
    /// stages for the next instance.
    fn run(&mut self) {
        if !self.staging.line.is_empty() {
            let rest = &self.input_buf[self.pos..];
            match find_byte(b'\n', rest) {
                Some(i) => {
                    self.staging.line.extend_from_slice(&rest[..i]);
                    self.pos += i + 1;
                    let staged = std::mem::take(&mut self.staging.line);
                    if !self.fold_line(&staged, true, StageOwned) {
                        self.flush_end(false);
                        return;
                    }
                }
                None => {
                    self.staging.line.extend_from_slice(rest);
                    self.pos = self.input_buf.len();
                    self.flush_end(!self.stopped);
                    return;
                }
            }
        }
        let input = self.input_buf;
        while self.pos < input.len() && !self.stopped {
            match find_byte(b'\n', &input[self.pos..]) {
                Some(i) => {
                    let line = &input[self.pos..self.pos + i];
                    self.pos += i + 1;
                    if !self.fold_line(line, true, StageBorrow) {
                        break;
                    }
                }
                None => break,
            }
        }
        if !self.stopped && self.pos < input.len() {
            self.staging.line.extend_from_slice(&input[self.pos..]);
            self.pos = input.len();
        }
        self.flush_end(!self.stopped);
    }

    /// Hands the family the previous instance carried in to the assembler,
    /// once. The first line that can touch a family finds it in place; an
    /// instance folding nothing, or only blank and comment lines, leaves it
    /// in the stage untouched.
    fn restore_carried(&mut self) {
        if let Some(carried) = self.staging.carried.take() {
            self.assembler.restore_carried(carried);
        }
    }

    /// Feeds one line through the fold pipeline, borrowed or owned by the
    /// stage. Returns false on a strict abort: folding stops, staged
    /// families still yield.
    fn fold_line<'s, S: Stage<'s, 'a>>(
        &mut self,
        line: &'s [u8],
        eol_is_newline: bool,
        stage: S,
    ) -> bool {
        let strict = self.mode.disposition() == Disposition::Abort;
        let scheme = self.mode.name_scheme();
        self.line_no += 1;
        self.stats.lines_total += 1;
        self.last_blank = false;
        let lexed = lex_line(
            line,
            eol_is_newline,
            scheme,
            &mut self.labels,
            &mut self.scratch,
            stage,
        );
        // A blank or comment line touches no family: an instance folding
        // only those leaves the carried family where it is.
        if !matches!(lexed, Ok(LineView::Blank | LineView::Comment)) {
            self.restore_carried();
        }
        let mut view = match lexed {
            Ok(v) => v,
            Err(e) => {
                if strict {
                    self.stats.last_error = Some(Error {
                        line: self.line_no,
                        kind: e.kind,
                    });
                    self.stopped = true;
                    return false;
                }
                self.labels.clear();
                record_skip(
                    &mut self.stats,
                    &mut self.assembler,
                    self.line_no,
                    e.kind,
                    e.family,
                );
                return true;
            }
        };
        self.last_blank = matches!(view, LineView::Blank);
        if let Err(e) = self.mode.check(&mut view, self.line_no) {
            if strict {
                self.stats.last_error = Some(e);
                self.stopped = true;
                return false;
            }
            if let LineView::Sample(s) = view {
                self.labels = s.labels;
                self.labels.clear();
            }
            self.stats.skipped_lines += 1;
            self.stats.last_error = Some(e);
            return true;
        }
        match view {
            LineView::Blank | LineView::Comment => {}
            LineView::Help { name, text } => self.assembler.fold_help(name, text),
            LineView::Type { name, metric_type } => self.assembler.fold_type(name, metric_type),
            LineView::Sample(mut s) => {
                let folded = self.assembler.fold_sample(&mut s, strict);
                self.stats.metrics_total += self.assembler.take_created_metrics();
                if let Err(kind) = folded {
                    if strict {
                        self.stats.last_error = Some(Error {
                            line: self.line_no,
                            kind,
                        });
                        self.stopped = true;
                        return false;
                    }
                    // Recycle the leftover labels buffer for the next line.
                    self.labels = std::mem::take(&mut s.labels);
                    self.labels.clear();
                    record_skip(
                        &mut self.stats,
                        &mut self.assembler,
                        self.line_no,
                        kind,
                        None,
                    );
                }
            }
        }
        true
    }

    /// Ends the instance: an aggregation family the input may still extend
    /// carries over as owned text. A staged cut line names its family by
    /// the probe, falling back to the last folded family (a blank or
    /// interleaved plain line may sit between an aggregation row's lines);
    /// a clean boundary holds the last folded family only while it still
    /// gathers a sample group.
    fn flush_end(&mut self, carry_allowed: bool) {
        if carry_allowed {
            let scheme = self.mode.name_scheme();
            let slot = if self.staging.line.is_empty() {
                self.assembler
                    .last_slot()
                    .filter(|&slot| self.assembler.gathers_sample_group(slot))
            } else {
                pending_fold_family(&self.staging.line, false, scheme, &self.assembler)
                    .and_then(|name| self.assembler.aggregation_slot_of(&name))
                    .or_else(|| self.assembler.last_slot())
            };
            if let Some(slot) = slot {
                self.staging.carried = self.assembler.take_aggregation_slot_owned(slot);
            }
        }
        self.stats.pruned_families += self.assembler.flush_ready();
    }
}
