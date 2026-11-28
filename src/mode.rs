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

use rustc_hash::FxHashMap;

use crate::assemble::resolve_family_name;
use crate::error::{Disposition, Error, ErrorKind};
use crate::lexer::LineView;
use crate::model::MetricType;

/// Per-line validation policy. The lexer produces the structural view of a
/// line; the mode decides whether it is acceptable and what happens when it
/// is not. Modes carry per-parse validation state; a mode instance is
/// single-use — the zero-copy parser moves it across instances through
/// [`crate::TextParser::release`].
pub trait Mode: Send {
    fn check(&mut self, line: &mut LineView<'_>, line_no: u64) -> Result<(), Error>;
    fn disposition(&self) -> Disposition;

    /// Name-validation scheme woven into lexing at Go's setOrCreateCurrentMF
    /// positions, so an invalid metric name preempts later same-line errors
    /// exactly like Go's reading order. `None` performs no name validation.
    fn name_scheme(&self) -> Option<Scheme> {
        None
    }

    /// Clears the per-stream validation state so the instance can
    /// serve a fresh stream: the [`Parser`](crate::Parser) facade calls it
    /// from `reset` after folding the stream's tail.
    fn reset(&mut self) {}
}

/// Accepts everything the lexer can express structurally. Name charsets,
/// `__name__`, duplicate labels/HELP/TYPE, and unknown escapes are left
/// unchecked; escapes are kept verbatim. A row that cannot fold into its
/// family's type — an unparseable `quantile`/`le` label value, a negative
/// count — is dropped whole with the error recorded: malformed values
/// never enter the output, and the family's earlier rows survive.
#[derive(Debug, Clone, Copy, Default)]
pub struct LenientMode;

impl Mode for LenientMode {
    fn check(&mut self, _line: &mut LineView<'_>, _line_no: u64) -> Result<(), Error> {
        Ok(())
    }

    fn disposition(&self) -> Disposition {
        Disposition::SkipLine
    }
}

/// Name validation scheme, mirroring Go's model.ValidationScheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    /// `[a-zA-Z_:][a-zA-Z0-9_:]*` for metric names, no colon for labels.
    Legacy,
    /// Any non-empty valid UTF-8 name.
    Utf8,
}

impl Scheme {
    /// Mirrors model.ValidationScheme.IsValidMetricName.
    pub(crate) fn is_valid_metric_name(&self, name: &str) -> bool {
        match self {
            Scheme::Legacy => {
                let mut bytes = name.bytes();
                match bytes.next() {
                    Some(b) if b.is_ascii_alphabetic() || b == b'_' || b == b':' => {}
                    _ => return false,
                }
                bytes.all(|b| {
                    b.is_ascii_alphabetic() || b == b'_' || b == b':' || b.is_ascii_digit()
                })
            }
            Scheme::Utf8 => !name.is_empty(),
        }
    }

    /// Mirrors model.ValidationScheme.IsValidLabelName (no colon).
    fn is_valid_label_name(&self, name: &str) -> bool {
        match self {
            Scheme::Legacy => {
                let mut bytes = name.bytes();
                match bytes.next() {
                    Some(b) if b.is_ascii_alphabetic() || b == b'_' => {}
                    _ => return false,
                }
                bytes.all(|b| b.is_ascii_alphabetic() || b == b'_' || b.is_ascii_digit())
            }
            Scheme::Utf8 => !name.is_empty(),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct FamilyState {
    help_info_captured: bool,
    metric_type: Option<MetricType>,
    sample: bool,
}

/// Enforces the full classic-format rule set: name charsets per scheme, the
/// `__name__` reserved label, unknown escapes, and the per-family
/// HELP/TYPE/sample state machine keyed by the suffix-resolved family name
/// (mirroring Go's setOrCreateCurrentMF). The first violation aborts the
/// parse.
#[derive(Debug, Clone)]
pub struct StrictMode {
    scheme: Scheme,
    families: FxHashMap<String, FamilyState>,
}

impl Default for StrictMode {
    fn default() -> Self {
        StrictMode::legacy()
    }
}

impl StrictMode {
    pub fn legacy() -> Self {
        StrictMode::with_scheme(Scheme::Legacy)
    }

    pub fn utf8() -> Self {
        StrictMode::with_scheme(Scheme::Utf8)
    }

    pub fn with_scheme(scheme: Scheme) -> Self {
        StrictMode {
            scheme,
            families: FxHashMap::default(),
        }
    }
}

impl Mode for StrictMode {
    fn check(&mut self, line: &mut LineView<'_>, line_no: u64) -> Result<(), Error> {
        let at = |kind: ErrorKind| Error {
            line: line_no,
            kind,
        };
        match line {
            LineView::Blank | LineView::Comment => Ok(()),
            LineView::Help { name, text: _ } => {
                let target = resolve_family_name(&name.decoded, |n| {
                    self.families.get(n).map(|st| st.metric_type)
                });
                let st = match self.families.get_mut(target) {
                    Some(st) => st,
                    None => {
                        self.families
                            .insert(target.to_string(), FamilyState::default());
                        self.families.get_mut(target).unwrap()
                    }
                };
                if st.help_info_captured {
                    return Err(at(ErrorKind::SecondHelpLine {
                        name: target.to_string(),
                    }));
                }
                st.help_info_captured = true;
                Ok(())
            }
            LineView::Type { name, metric_type } => {
                let target = resolve_family_name(&name.decoded, |n| {
                    self.families.get(n).map(|st| st.metric_type)
                });
                let st = match self.families.get_mut(target) {
                    Some(st) => st,
                    None => {
                        self.families
                            .insert(target.to_string(), FamilyState::default());
                        self.families.get_mut(target).unwrap()
                    }
                };
                if st.metric_type.is_some() || st.sample {
                    return Err(at(ErrorKind::SecondTypeLine {
                        name: target.to_string(),
                    }));
                }
                st.metric_type = Some(*metric_type);
                Ok(())
            }
            LineView::Sample(s) => {
                for l in &s.labels {
                    if l.name == "__name__" {
                        return Err(at(ErrorKind::ReservedLabelName));
                    }
                    // A plain label set's character class was proven by the
                    // name scan; only the reserved-name rule still applies.
                    if !s.labels_plain && !self.scheme.is_valid_label_name(&l.name) {
                        return Err(at(ErrorKind::InvalidLabelNameQuoted {
                            name: l.name.to_string(),
                        }));
                    }
                }
                let target = resolve_family_name(&s.name.decoded, |n| {
                    self.families.get(n).map(|st| st.metric_type)
                });
                let st = match self.families.get_mut(target) {
                    Some(st) => st,
                    None => {
                        self.families
                            .insert(target.to_string(), FamilyState::default());
                        self.families.get_mut(target).unwrap()
                    }
                };
                st.sample = true;
                st.metric_type.get_or_insert(MetricType::Untyped);
                Ok(())
            }
        }
    }

    fn disposition(&self) -> Disposition {
        Disposition::Abort
    }

    fn name_scheme(&self) -> Option<Scheme> {
        Some(self.scheme)
    }

    fn reset(&mut self) {
        self.families.clear();
    }
}
