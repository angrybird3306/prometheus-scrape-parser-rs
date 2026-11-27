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

use crate::error::ErrorKind;
use crate::mode::Scheme;
use crate::model::{Label, MetricType};
use crate::search::{find_byte, find_byte2};
use std::borrow::Cow;
use std::marker::PhantomData;

/// Where lexed text comes from: verbatim slices of the input line (the
/// zero-copy fast path), or detached owned text for the staged lines a feed
/// boundary assembled.
pub(crate) trait Stage<'a, 'l> {
    fn place(&self, text: &'a str) -> Cow<'l, str>;
}

/// Borrowed staging: decoded text borrows the input line verbatim.
#[derive(Clone, Copy)]
pub(crate) struct StageBorrow;

impl<'a> Stage<'a, 'a> for StageBorrow {
    fn place(&self, text: &'a str) -> Cow<'a, str> {
        Cow::Borrowed(text)
    }
}

/// Owned staging: decoded text detaches from the input entirely. The
/// zero-copy parser folds its feed-straddling lines (assembled in an
/// internal buffer no family may reference) through this stage.
pub(crate) struct StageOwned;

impl<'a, 'l> Stage<'a, 'l> for StageOwned {
    fn place(&self, text: &'a str) -> Cow<'l, str> {
        Cow::Owned(text.to_owned())
    }
}

/// A lexed token: the escape-decoded text plus the first byte of the first
/// escape sequence Go rejects (`\t` and friends). LenientMode keeps unknown
/// escapes verbatim; StrictMode turns the flag into an error.
#[derive(Debug, Clone, PartialEq)]
pub struct Token<'a> {
    pub decoded: Cow<'a, str>,
    pub unknown_escape: Option<u8>,
}

/// The structural view of one input line, shared by every Mode.
#[derive(Debug, Clone, PartialEq)]
pub enum LineView<'a> {
    Blank,
    Comment,
    Help {
        name: Token<'a>,
        text: Token<'a>,
    },
    Type {
        name: Token<'a>,
        metric_type: MetricType,
    },
    Sample(SampleLine<'a>),
}

/// One sample line before family folding.
#[derive(Debug, Clone, PartialEq)]
pub struct SampleLine<'a> {
    pub name: Token<'a>,
    pub labels: Vec<Label<'a>>,
    pub value: f64,
    /// Raw timestamp token; LenientMode additionally tolerates fractional
    /// values by truncating to whole milliseconds.
    pub timestamp_raw: Option<Cow<'a, str>>,
}

/// A lex failure.
#[derive(Debug, Clone)]
pub(crate) struct LexError {
    pub kind: ErrorKind,
}

impl LexError {
    fn new(kind: ErrorKind) -> Self {
        LexError { kind }
    }
}

struct Lexer<'a, 'l, S: Stage<'a, 'l>> {
    line: &'a [u8],
    pos: usize,
    cur: Option<u8>,
    /// Whether this line slice was terminated by `\n` (as opposed to being
    /// truncated by the end of input); a line-end inside a quoted token is a
    /// "contains unescaped new-line" error in the former case and an EOF
    /// error in the latter.
    eol_is_newline: bool,
    stage: S,
    _target: PhantomData<&'l ()>,
}

impl<'a, 'l, S: Stage<'a, 'l>> Lexer<'a, 'l, S> {
    fn new(line: &'a [u8], eol_is_newline: bool, stage: S) -> Self {
        Lexer {
            line,
            pos: 0,
            cur: None,
            eol_is_newline,
            stage,
            _target: PhantomData,
        }
    }

    /// Mirrors Go's `ReadByte`: `None` replaces both '\n' (which line slices
    /// never contain) and end-of-input.
    fn read(&mut self) {
        self.cur = if self.pos < self.line.len() {
            let b = self.line[self.pos];
            self.pos += 1;
            Some(b)
        } else {
            None
        };
    }

    /// Mirrors Go's `skipBlankTab`: always consumes one byte first, so the
    /// byte the caller last inspected is discarded.
    fn skip_blanks(&mut self) {
        loop {
            self.read();
            if !matches!(self.cur, Some(b' ') | Some(b'\t')) {
                return;
            }
        }
    }

    fn skip_blanks_if_current_blank(&mut self) {
        if matches!(self.cur, Some(b' ') | Some(b'\t')) {
            self.skip_blanks();
        }
    }

    /// Token up to the next blank, tab, or end of line. Empty when the
    /// current byte already ends the line.
    fn read_token_until_whitespace(&mut self) -> &'a [u8] {
        if self.cur.is_none() {
            return &self.line[self.pos..];
        }
        let start = self.pos - 1;
        let end = match find_byte2(b' ', b'\t', &self.line[start..]) {
            Some(i) => {
                let hit = start + i;
                self.cur = Some(self.line[hit]);
                self.pos = hit + 1;
                hit
            }
            None => {
                self.pos = self.line.len();
                self.cur = None;
                self.pos
            }
        };
        &self.line[start..end]
    }

    /// Rest of the line starting at the current byte.
    fn rest_from_current(&self) -> &'a [u8] {
        &self.line[self.pos - 1..]
    }

    /// The escape-decoded bytes consumed so far in `read_name`, for error
    /// messages that quote the partial token (Go's currentToken).
    fn partial_name(
        &self,
        start: usize,
        verbatim: bool,
        event_start: usize,
        scratch: &[u8],
    ) -> String {
        if verbatim {
            String::from_utf8_lossy(&self.line[start..self.pos]).into_owned()
        } else {
            let mut bytes = Vec::with_capacity(event_start - start + scratch.len());
            bytes.extend_from_slice(&self.line[start..event_start]);
            bytes.extend_from_slice(scratch);
            String::from_utf8_lossy(&bytes).into_owned()
        }
    }

    /// Metric or label name, bare or quoted. Mirrors Go's `readTokenAsMetricName` /
    /// `readTokenAsLabelName`: quotes toggle in place (and may appear mid-name),
    /// escapes decode, unquoted names stop at ' ' (metric) or '=' (label).
    /// Unlike Go, bytes >= 0x80 continue a quoted name instead of ending it.
    fn read_name(&mut self, metric: bool, scratch: &mut Vec<u8>) -> Result<Token<'l>, ErrorKind> {
        scratch.clear();
        let is_start = match self.cur {
            Some(b) => is_label_name_start(b) || (metric && b == b':'),
            None => false,
        };
        if !is_start {
            return Ok(Token {
                decoded: Cow::Borrowed(""),
                unknown_escape: None,
            });
        }
        let start = self.pos - 1;
        let mut quoted = false;
        let mut escaped = false;
        let mut verbatim = true;
        let mut unknown: Option<u8> = None;
        let mut event_start = 0;
        let end;
        let context = if metric { "metric name" } else { "label name" };
        loop {
            if escaped {
                match self.cur {
                    Some(b'\\') | Some(b'n') | Some(b'"') => {
                        if verbatim {
                            event_start = self.pos - 2;
                            verbatim = false;
                        }
                        scratch.push(match self.cur {
                            Some(b'\\') => b'\\',
                            Some(b'n') => b'\n',
                            _ => b'"',
                        });
                    }
                    Some(_) => {
                        if verbatim {
                            event_start = self.pos - 2;
                            verbatim = false;
                        }
                        scratch.push(b'\\');
                        scratch.push(self.cur.unwrap());
                        if unknown.is_none() {
                            unknown = Some(self.cur.unwrap());
                        }
                    }
                    None => {
                        return Err(self.backslash_at_line_end());
                    }
                }
                escaped = false;
            } else {
                match self.cur {
                    Some(b'"') => {
                        quoted = !quoted;
                        if verbatim {
                            event_start = self.pos - 1;
                            verbatim = false;
                        }
                        if !quoted {
                            self.read();
                            end = self.pos - 1;
                            break;
                        }
                    }
                    Some(b'\\') => escaped = true,
                    _ => {
                        if !verbatim {
                            scratch.push(self.cur.unwrap());
                        }
                    }
                }
            }
            self.read();
            let cont = match self.cur {
                None => false,
                Some(b) => {
                    let base = quoted
                        || is_label_name_start(b)
                        || b.is_ascii_digit()
                        || (metric && b == b':');
                    let stop = !quoted && if metric { b == b' ' } else { b == b'=' };
                    base && !stop
                }
            };
            if !cont {
                if self.cur.is_none() && quoted {
                    if self.eol_is_newline {
                        let name = self.partial_name(start, verbatim, event_start, scratch);
                        return Err(if metric {
                            ErrorKind::NewlineInMetricName { name }
                        } else {
                            ErrorKind::NewlineInLabelName { name }
                        });
                    }
                    return Err(ErrorKind::UnexpectedEof);
                }
                end = if self.cur.is_some() {
                    self.pos - 1
                } else {
                    self.pos
                };
                break;
            }
        }
        let decoded = if verbatim {
            match std::str::from_utf8(&self.line[start..end]) {
                Ok(s) => self.stage.place(s),
                Err(_) => return Err(ErrorKind::InvalidUtf8 { context }),
            }
        } else {
            let mut bytes = Vec::with_capacity(event_start - start + scratch.len());
            bytes.extend_from_slice(&self.line[start..event_start]);
            bytes.append(scratch);
            match String::from_utf8(bytes) {
                Ok(s) => Cow::Owned(s),
                Err(_) => return Err(ErrorKind::InvalidUtf8 { context }),
            }
        };
        Ok(Token {
            decoded,
            unknown_escape: unknown,
        })
    }

    /// Backslash at the line end: an invalid `\<newline>` escape on a
    /// newline-terminated line, plain EOF on truncated input.
    fn backslash_at_line_end(&self) -> ErrorKind {
        if self.eol_is_newline {
            ErrorKind::InvalidEscapeSequence { got: b'\n' }
        } else {
            ErrorKind::UnexpectedEof
        }
    }

    /// Quoted label value. Like Go, it ignores the current byte (the opening
    /// quote) and starts with a fresh read; the closing quote terminates it.
    fn read_label_value(&mut self, scratch: &mut Vec<u8>) -> Result<Token<'l>, ErrorKind> {
        scratch.clear();
        let content_start = self.pos;
        let mut escaped = false;
        let mut verbatim = true;
        let mut unknown: Option<u8> = None;
        let mut event_start = 0;
        let end;
        loop {
            // Verbatim payload bytes need no per-byte work: jump to the next
            // quote or backslash. Past the first escape every byte lands in
            // scratch, and while escaped the next byte is the escape payload
            // itself — both go through the per-byte path.
            if verbatim && !escaped {
                match find_byte2(b'"', b'\\', &self.line[self.pos..]) {
                    Some(i) => self.pos += i,
                    None => self.pos = self.line.len(),
                }
            }
            self.read();
            match self.cur {
                None => {
                    if self.eol_is_newline {
                        let bytes = if verbatim {
                            self.line[content_start..self.pos].to_vec()
                        } else {
                            let mut b =
                                Vec::with_capacity(event_start - content_start + scratch.len());
                            b.extend_from_slice(&self.line[content_start..event_start]);
                            b.extend_from_slice(scratch);
                            b
                        };
                        return Err(ErrorKind::NewlineInLabelValue {
                            value: String::from_utf8_lossy(&bytes).into_owned(),
                        });
                    }
                    return Err(ErrorKind::UnexpectedEof);
                }
                Some(b'"') if !escaped => {
                    end = self.pos - 1;
                    break;
                }
                Some(b'\\') if !escaped => escaped = true,
                Some(_) if escaped => {
                    if verbatim {
                        event_start = self.pos - 2;
                        verbatim = false;
                    }
                    match self.cur {
                        Some(b'\\') | Some(b'"') => scratch.push(self.cur.unwrap()),
                        Some(b'n') => scratch.push(b'\n'),
                        _ => {
                            scratch.push(b'\\');
                            scratch.push(self.cur.unwrap());
                            if unknown.is_none() {
                                unknown = Some(self.cur.unwrap());
                            }
                        }
                    }
                    escaped = false;
                }
                Some(_) => {
                    if !verbatim {
                        scratch.push(self.cur.unwrap());
                    }
                }
            }
        }
        let decoded = if verbatim {
            match std::str::from_utf8(&self.line[content_start..end]) {
                Ok(s) => self.stage.place(s),
                Err(_) => {
                    return Err(ErrorKind::InvalidLabelValueUtf8 {
                        value: self.line[content_start..end].to_vec(),
                    });
                }
            }
        } else {
            let mut bytes = Vec::with_capacity(event_start - content_start + scratch.len());
            bytes.extend_from_slice(&self.line[content_start..event_start]);
            bytes.append(scratch);
            match String::from_utf8(bytes) {
                Ok(s) => Cow::Owned(s),
                Err(e) => {
                    return Err(ErrorKind::InvalidLabelValueUtf8 {
                        value: e.as_bytes().to_vec(),
                    });
                }
            }
        };
        Ok(Token {
            decoded,
            unknown_escape: unknown,
        })
    }
}

fn is_label_name_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_' || b == b'"'
}

/// Lex one line into its structural view. `labels` and `scratch` are
/// caller-owned buffers reused across lines; `labels` is taken by the
/// returned SampleLine and must be cleared by the caller before reuse.
/// `eol_is_newline` distinguishes a `\n`-terminated line from one truncated
/// by the end of input; `scheme` validates metric names at the positions
/// Go's setOrCreateCurrentMF runs at. `stage` decides the provenance of the
/// decoded text (borrow the line verbatim, or detach it as owned text).
/// `strict` runs Go's spurious-text-after-timestamp check; lenient only
/// rejects leftover tokens past the timestamp.
pub(crate) fn lex_line<'a, 'l, S: Stage<'a, 'l>>(
    line: &'a [u8],
    eol_is_newline: bool,
    scheme: Option<Scheme>,
    labels: &mut Vec<Label<'l>>,
    scratch: &mut Vec<u8>,
    stage: S,
    strict: bool,
) -> Result<LineView<'l>, LexError> {
    let mut lx = Lexer::new(line, eol_is_newline, stage);
    lx.skip_blanks();
    match lx.cur {
        None => Ok(LineView::Blank),
        Some(b'#') => lex_comment(&mut lx, scheme, scratch),
        Some(b'{') => lex_sample(&mut lx, true, scheme, labels, scratch, strict),
        Some(_) => lex_sample(&mut lx, false, scheme, labels, scratch, strict),
    }
}

/// The family name a staged line prefix determines, for the carry probe at
/// an instance boundary: the name lives in the line's head (a bare, quoted,
/// or `# HELP`/`# TYPE` name), so only the head is scanned — that keeps
/// re-probing a growing staged line linear in the appended bytes. `None`
/// means the head does not (yet) determine a name. Unlike a full
/// [`lex_line`], the prefix need not lex completely — a cut like
/// `h_bucket{le="1"` names its family just as surely as a complete line.
pub(crate) fn probe_staged_family<'a, 'l, S: Stage<'a, 'l>>(
    line: &'a [u8],
    eol_is_newline: bool,
    scheme: Option<Scheme>,
    scratch: &mut Vec<u8>,
    stage: S,
) -> Option<Token<'l>> {
    let mut lx = Lexer::new(line, eol_is_newline, stage);
    lx.skip_blanks();
    let name = match lx.cur {
        None => return None,
        Some(b'#') => {
            lx.skip_blanks();
            let keyword = lx.read_token_until_whitespace();
            if lx.cur.is_none() || (keyword != b"HELP" && keyword != b"TYPE") {
                return None;
            }
            lx.skip_blanks();
            let name = lx.read_name(true, scratch).ok()?;
            check_escape(&name, scheme).ok()?;
            if !matches!(lx.cur, Some(b' ') | Some(b'\t')) {
                return None;
            }
            check_metric_name_scheme(&name.decoded, scheme).ok()?;
            name
        }
        Some(b'{') => {
            lx.skip_blanks();
            let name = lx.read_name(false, scratch).ok()?;
            check_escape(&name, scheme).ok()?;
            if !matches!(lx.cur, Some(b',') | Some(b'}')) {
                return None;
            }
            if name.decoded.is_empty() {
                return None;
            }
            check_metric_name_scheme(&name.decoded, scheme).ok()?;
            name
        }
        Some(_) => {
            let name = lx.read_name(true, scratch).ok()?;
            check_escape(&name, scheme).ok()?;
            // A token ending at the prefix's own end does not fix the name:
            // `metric_na` may still grow into `metric_name`.
            if !matches!(lx.cur, Some(b' ') | Some(b'{')) {
                return None;
            }
            if name.decoded.is_empty() {
                return None;
            }
            check_metric_name_scheme(&name.decoded, scheme).ok()?;
            name
        }
    };
    Some(name)
}

/// Scheme validation at the point Go's setOrCreateCurrentMF runs: an invalid
/// name preempts every later error on the same line.
fn check_metric_name_scheme(name: &str, scheme: Option<Scheme>) -> Result<(), ErrorKind> {
    match scheme {
        None => Ok(()),
        Some(s) if s.is_valid_metric_name(name) => Ok(()),
        Some(_) => Err(ErrorKind::InvalidMetricNameQuoted {
            name: name.to_string(),
        }),
    }
}

/// Strict modes (the only ones carrying a scheme) reject unknown escapes at
/// Go's read-time positions; LenientMode keeps them verbatim.
fn check_escape(tok: &Token<'_>, scheme: Option<Scheme>) -> Result<(), ErrorKind> {
    match (scheme, tok.unknown_escape) {
        (_, None) | (None, Some(_)) => Ok(()),
        (Some(_), Some(b)) => Err(ErrorKind::InvalidEscapeSequence { got: b }),
    }
}

fn lex_comment<'a, 'l, S: Stage<'a, 'l>>(
    lx: &mut Lexer<'a, 'l, S>,
    scheme: Option<Scheme>,
    scratch: &mut Vec<u8>,
) -> Result<LineView<'l>, LexError> {
    lx.skip_blanks();
    if lx.cur.is_none() {
        return Ok(LineView::Comment);
    }
    let keyword = lx.read_token_until_whitespace();
    if lx.cur.is_none() {
        return Ok(LineView::Comment);
    }
    if keyword != b"HELP" && keyword != b"TYPE" {
        return Ok(LineView::Comment);
    }
    let is_help = keyword == b"HELP";
    lx.skip_blanks();
    let name = lx.read_name(true, scratch).map_err(LexError::new)?;
    check_escape(&name, scheme).map_err(LexError::new)?;
    if lx.cur.is_none() {
        return Ok(LineView::Comment);
    }
    if !matches!(lx.cur, Some(b' ') | Some(b'\t')) {
        return Err(LexError::new(ErrorKind::InvalidMetricNameInComment));
    }
    check_metric_name_scheme(&name.decoded, scheme).map_err(LexError::new)?;
    lx.skip_blanks();
    if lx.cur.is_none() {
        return Ok(LineView::Comment);
    }
    if is_help {
        let raw = lx.rest_from_current();
        let (decoded, unknown_escape) =
            decode_token(raw, "help text", lx.eol_is_newline, &lx.stage).map_err(LexError::new)?;
        let text = Token {
            decoded,
            unknown_escape,
        };
        check_escape(&text, scheme).map_err(LexError::new)?;
        Ok(LineView::Help { name, text })
    } else {
        let raw = lx.rest_from_current();
        match parse_metric_type(raw) {
            Some(metric_type) => Ok(LineView::Type { name, metric_type }),
            None => Err(LexError::new(ErrorKind::UnknownMetricType {
                got: String::from_utf8_lossy(raw).into_owned(),
            })),
        }
    }
}

fn lex_sample<'a, 'l, S: Stage<'a, 'l>>(
    lx: &mut Lexer<'a, 'l, S>,
    started_with_brace: bool,
    scheme: Option<Scheme>,
    labels: &mut Vec<Label<'l>>,
    scratch: &mut Vec<u8>,
    strict: bool,
) -> Result<LineView<'l>, LexError> {
    let mut name: Option<Token<'l>> = None;
    let mut name_inside = false;
    let err = |e: ErrorKind| LexError::new(e);
    if !started_with_brace {
        let tok = lx.read_name(true, scratch).map_err(err)?;
        if tok.decoded.is_empty() {
            return Err(LexError::new(ErrorKind::InvalidMetricName));
        }
        check_escape(&tok, scheme).map_err(LexError::new)?;
        check_metric_name_scheme(&tok.decoded, scheme).map_err(LexError::new)?;
        name = Some(tok);
        lx.skip_blanks_if_current_blank();
    }
    if lx.cur == Some(b'{') {
        loop {
            lx.skip_blanks();
            if lx.cur == Some(b'}') {
                if name.is_none() {
                    return Err(LexError::new(ErrorKind::InvalidMetricName));
                }
                lx.skip_blanks();
                break;
            }
            let label_name = lx.read_name(false, scratch).map_err(err)?;
            check_escape(&label_name, scheme).map_err(err)?;
            if label_name.decoded.is_empty() {
                return Err(err(ErrorKind::InvalidLabelNameForMetric {
                    family: name
                        .as_ref()
                        .map(|n| n.decoded.to_string())
                        .unwrap_or_default(),
                }));
            }
            lx.skip_blanks_if_current_blank();
            if lx.cur != Some(b'=') {
                if started_with_brace {
                    if name_inside {
                        return Err(LexError::new(ErrorKind::MultipleMetricNames {
                            family: name.as_ref().unwrap().decoded.to_string(),
                        }));
                    }
                    match lx.cur {
                        Some(b',') => {
                            check_metric_name_scheme(&label_name.decoded, scheme)
                                .map_err(LexError::new)?;
                            name = Some(label_name);
                            name_inside = true;
                            continue;
                        }
                        Some(b'}') => {
                            check_metric_name_scheme(&label_name.decoded, scheme)
                                .map_err(LexError::new)?;
                            name = Some(label_name);
                            lx.skip_blanks();
                            break;
                        }
                        _ => {
                            return Err(err(ErrorKind::UnexpectedEndOfMetricName {
                                got: lx.cur.unwrap_or(b'\n'),
                            }));
                        }
                    }
                }
                return Err(err(ErrorKind::ExpectedEqAfterLabelName {
                    got: lx.cur.unwrap_or(b'\n'),
                }));
            }
            lx.skip_blanks();
            let got = lx.cur;
            if got != Some(b'"') {
                return Err(err(ErrorKind::ExpectedQuoteAtLabelValueStart {
                    got: got.unwrap_or(b'\n'),
                }));
            }
            let label_value = lx.read_label_value(scratch).map_err(err)?;
            check_escape(&label_value, scheme).map_err(err)?;
            lx.skip_blanks();
            match lx.cur {
                Some(b',') => {
                    labels.push(Label {
                        name: label_name.decoded,
                        value: label_value.decoded,
                    });
                    continue;
                }
                Some(b'}') => {
                    labels.push(Label {
                        name: label_name.decoded,
                        value: label_value.decoded,
                    });
                    lx.skip_blanks();
                    break;
                }
                _ => {
                    return Err(err(ErrorKind::UnexpectedEndOfLabelValue {
                        value: label_value.decoded.to_string(),
                    }));
                }
            }
        }
    }
    let name = match name {
        Some(n) => n,
        None => return Err(LexError::new(ErrorKind::InvalidMetricName)),
    };
    let raw_value = lx.read_token_until_whitespace();
    let value = parse_sample_value(raw_value).map_err(LexError::new)?;
    let mut timestamp_raw = None;
    if lx.cur.is_some() {
        lx.skip_blanks();
        let raw_ts = lx.read_token_until_whitespace();
        let ts = match std::str::from_utf8(raw_ts) {
            Ok(s) => s,
            Err(_) => {
                return Err(LexError::new(ErrorKind::ExpectedTimestamp {
                    got: String::from_utf8_lossy(raw_ts).into_owned(),
                }));
            }
        };
        timestamp_raw = Some(lx.stage.place(ts));
        let (spurious, non_empty) = if strict && lx.cur.is_some() {
            // Go's spurious text starts at the blank that ended the token,
            // and its backslashes are dropped along the way.
            let rest = lx.rest_from_current();
            (rest, rest.iter().any(|&b| b != b'\\'))
        } else {
            let rest = &lx.line[lx.pos..];
            (rest, !rest.is_empty())
        };
        if non_empty {
            let got: Vec<u8> = if strict {
                spurious.iter().copied().filter(|&b| b != b'\\').collect()
            } else {
                spurious.to_vec()
            };
            return Err(LexError::new(ErrorKind::SpuriousAfterTimestamp {
                got: String::from_utf8_lossy(&got).into_owned(),
            }));
        }
    }
    Ok(LineView::Sample(SampleLine {
        name,
        labels: std::mem::take(labels),
        value,
        timestamp_raw,
    }))
}

/// Mirrors Go's `parseFloat`: rejects `p`/`P`/`_` outright, and only
/// spelled-out infinities may parse to infinity (Go's ParseFloat errors on
/// numeric overflow).
fn parse_sample_value(raw: &[u8]) -> Result<f64, ErrorKind> {
    let s = match std::str::from_utf8(raw) {
        Ok(s) => s,
        Err(_) => {
            return Err(ErrorKind::ExpectedFloatValue {
                got: String::from_utf8_lossy(raw).into_owned(),
            });
        }
    };
    if s.contains(['p', 'P', '_']) {
        return Err(ErrorKind::ExpectedFloatValue { got: s.to_string() });
    }
    match s.parse::<f64>() {
        Ok(v) if !v.is_infinite() || s.to_ascii_lowercase().contains("inf") => Ok(v),
        _ => Err(ErrorKind::ExpectedFloatValue { got: s.to_string() }),
    }
}

/// Parses the same float grammar as [`parse_sample_value`] for quantile and
/// `le` label values.
pub(crate) fn parse_float_token(raw: &str) -> Result<f64, ()> {
    let value = raw.as_bytes();
    parse_sample_value(value).map_err(|_| ())
}

/// Type tokens are read raw (no escape processing) and matched
/// case-insensitively; `gaugehistogram` is the OpenMetrics spelling alias.
fn parse_metric_type(tok: &[u8]) -> Option<MetricType> {
    if tok.eq_ignore_ascii_case(b"counter") {
        Some(MetricType::Counter)
    } else if tok.eq_ignore_ascii_case(b"gauge") {
        Some(MetricType::Gauge)
    } else if tok.eq_ignore_ascii_case(b"summary") {
        Some(MetricType::Summary)
    } else if tok.eq_ignore_ascii_case(b"untyped") {
        Some(MetricType::Untyped)
    } else if tok.eq_ignore_ascii_case(b"histogram") {
        Some(MetricType::Histogram)
    } else if tok.eq_ignore_ascii_case(b"gauge_histogram")
        || tok.eq_ignore_ascii_case(b"gaugehistogram")
    {
        Some(MetricType::GaugeHistogram)
    } else {
        None
    }
}

/// Decodes `\\`, `\n`, `\"` escapes. Unknown escapes are kept verbatim
/// (backslash and the following byte) and reported through the flag. A
/// trailing backslash is an invalid `\<newline>` escape on a
/// newline-terminated line and plain EOF on truncated input.
fn decode_token<'a, 'l, S: Stage<'a, 'l>>(
    raw: &'a [u8],
    context: &'static str,
    eol_is_newline: bool,
    stage: &S,
) -> Result<(Cow<'l, str>, Option<u8>), ErrorKind> {
    let Some(first) = find_byte(b'\\', raw) else {
        return match std::str::from_utf8(raw) {
            Ok(s) => Ok((stage.place(s), None)),
            Err(_) => Err(ErrorKind::InvalidUtf8 { context }),
        };
    };
    let mut out = Vec::with_capacity(raw.len());
    out.extend_from_slice(&raw[..first]);
    let mut unknown: Option<u8> = None;
    let mut i = first;
    while i < raw.len() {
        if raw[i] == b'\\' {
            match raw.get(i + 1) {
                Some(b'\\') => {
                    out.push(b'\\');
                    i += 2;
                }
                Some(b'n') => {
                    out.push(b'\n');
                    i += 2;
                }
                Some(b'"') => {
                    out.push(b'"');
                    i += 2;
                }
                Some(&b) => {
                    out.push(b'\\');
                    out.push(b);
                    if unknown.is_none() {
                        unknown = Some(b);
                    }
                    i += 2;
                }
                None => {
                    if eol_is_newline {
                        return Err(ErrorKind::InvalidEscapeSequence { got: b'\n' });
                    }
                    return Err(ErrorKind::UnexpectedEof);
                }
            }
        } else {
            out.push(raw[i]);
            i += 1;
        }
    }
    match String::from_utf8(out) {
        Ok(s) => Ok((Cow::Owned(s), unknown)),
        Err(_) => Err(ErrorKind::InvalidUtf8 { context }),
    }
}
