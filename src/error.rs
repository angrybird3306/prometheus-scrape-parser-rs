use std::fmt;

/// A parse error at a specific 1-based input position: the input line for
/// the text format, the message or metric ordinal for the protobuf format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    pub line: u64,
    pub kind: ErrorKind,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.kind.is_proto() {
            write!(
                f,
                "protobuf exposition parsing error in message {}: {}",
                self.line, self.kind
            )
        } else {
            write!(
                f,
                "text format parsing error in line {}: {}",
                self.line, self.kind
            )
        }
    }
}

impl std::error::Error for Error {}

/// Error kinds with message texts aligned to Go's expfmt parser.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorKind {
    /// `invalid metric name`.
    InvalidMetricName,
    /// `invalid metric name %q` from scheme validation.
    InvalidMetricNameQuoted { name: String },
    /// `invalid metric name in comment`.
    InvalidMetricNameInComment,
    /// `invalid label name for metric %q` for an empty label name.
    InvalidLabelNameForMetric { family: String },
    /// `invalid label name %q` from scheme validation.
    InvalidLabelNameQuoted { name: String },
    /// `label name "__name__" is reserved`.
    ReservedLabelName,
    /// `invalid escape sequence '\%c'` (every readToken* path).
    InvalidEscapeSequence { got: u8 },
    /// `metric name %q contains unescaped new-line`.
    NewlineInMetricName { name: String },
    /// `label name %q contains unescaped new-line`.
    NewlineInLabelName { name: String },
    /// `label value %q contains unescaped new-line`.
    NewlineInLabelValue { value: String },
    /// `invalid label value %q` for invalid UTF-8.
    InvalidLabelValueUtf8 { value: Vec<u8> },
    /// `multiple metric names for metric %q`.
    MultipleMetricNames { family: String },
    /// `unexpected end of metric name %q`.
    UnexpectedEndOfMetricName { got: u8 },
    /// `expected '=' after label name, found %q`.
    ExpectedEqAfterLabelName { got: u8 },
    /// `expected '"' at start of label value, found %q`.
    ExpectedQuoteAtLabelValueStart { got: u8 },
    /// `unexpected end of label value %q`.
    UnexpectedEndOfLabelValue { value: String },
    /// `expected float as value, got %q`.
    ExpectedFloatValue { got: String },
    /// `expected float as value for 'quantile' label, got %q`.
    ExpectedQuantileFloat { got: String },
    /// `expected float as value for 'le' label, got %q`.
    ExpectedLeFloat { got: String },
    /// `expected integer as timestamp, got %q`.
    ExpectedTimestamp { got: String },
    /// `spurious string after timestamp: %q`.
    SpuriousAfterTimestamp { got: String },
    /// `second HELP line for metric name %q`.
    SecondHelpLine { name: String },
    /// `second TYPE line for metric name %q, or TYPE reported after samples`.
    SecondTypeLine { name: String },
    /// `unknown metric type %q`.
    UnknownMetricType { got: String },
    /// `duplicate label names for metric %q`.
    DuplicateLabelNames { family: String },
    /// `negative count for histogram %q`.
    NegativeHistogramCount { family: String },
    /// `negative bucket population for histogram %q`.
    NegativeHistogramBucket { family: String },
    /// Our own phrasing after `negative count for histogram %q`; Go instead
    /// wraps negative summary counts through amd64 `uint64(value)`.
    NegativeSummaryCount { family: String },
    /// Invalid UTF-8 in a name token. Label-value UTF-8 errors use
    /// [`ErrorKind::InvalidLabelValueUtf8`] instead.
    InvalidUtf8 { context: &'static str },
    /// `unexpected end of input stream`.
    UnexpectedEof,
    /// Invalid wire format in a protobuf exposition message; `got` carries
    /// the reason (malformed length prefix, truncated final message, or the
    /// decoder's own detail).
    InvalidProtoMessage { got: String },
    /// A protobuf Metric carries no value field matching its family's type;
    /// only that metric is dropped.
    MissingMetricValue { family: String },
    /// Unknown protobuf MetricType enum value on the wire; the whole family
    /// message is dropped.
    UnknownProtoMetricType { got: i32 },
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorKind::InvalidMetricName => write!(f, "invalid metric name"),
            ErrorKind::InvalidMetricNameQuoted { name } => {
                write!(f, "invalid metric name {name:?}")
            }
            ErrorKind::InvalidMetricNameInComment => {
                write!(f, "invalid metric name in comment")
            }
            ErrorKind::InvalidLabelNameForMetric { family } => {
                write!(f, "invalid label name for metric {family:?}")
            }
            ErrorKind::InvalidLabelNameQuoted { name } => {
                write!(f, "invalid label name {name:?}")
            }
            ErrorKind::ReservedLabelName => write!(f, "label name \"__name__\" is reserved"),
            ErrorKind::InvalidEscapeSequence { got } => {
                write!(f, "invalid escape sequence '\\{}'", *got as char)
            }
            ErrorKind::NewlineInMetricName { name } => {
                write!(f, "metric name {name:?} contains unescaped new-line")
            }
            ErrorKind::NewlineInLabelName { name } => {
                write!(f, "label name {name:?} contains unescaped new-line")
            }
            ErrorKind::NewlineInLabelValue { value } => {
                write!(f, "label value {value:?} contains unescaped new-line")
            }
            ErrorKind::InvalidLabelValueUtf8 { value } => {
                write!(f, "invalid label value {}", go_quote(value))
            }
            ErrorKind::MultipleMetricNames { family } => {
                write!(f, "multiple metric names for metric {family:?}")
            }
            ErrorKind::UnexpectedEndOfMetricName { got } => {
                write!(f, "unexpected end of metric name {:?}", *got as char)
            }
            ErrorKind::ExpectedEqAfterLabelName { got } => {
                write!(f, "expected '=' after label name, found {:?}", *got as char)
            }
            ErrorKind::ExpectedQuoteAtLabelValueStart { got } => {
                write!(
                    f,
                    "expected '\"' at start of label value, found {:?}",
                    *got as char
                )
            }
            ErrorKind::UnexpectedEndOfLabelValue { value } => {
                write!(f, "unexpected end of label value {value:?}")
            }
            ErrorKind::ExpectedFloatValue { got } => {
                write!(f, "expected float as value, got {got:?}")
            }
            ErrorKind::ExpectedQuantileFloat { got } => {
                write!(
                    f,
                    "expected float as value for 'quantile' label, got {got:?}"
                )
            }
            ErrorKind::ExpectedLeFloat { got } => {
                write!(f, "expected float as value for 'le' label, got {got:?}")
            }
            ErrorKind::ExpectedTimestamp { got } => {
                write!(f, "expected integer as timestamp, got {got:?}")
            }
            ErrorKind::SpuriousAfterTimestamp { got } => {
                write!(f, "spurious string after timestamp: {got:?}")
            }
            ErrorKind::SecondHelpLine { name } => {
                write!(f, "second HELP line for metric name {name:?}")
            }
            ErrorKind::SecondTypeLine { name } => {
                write!(
                    f,
                    "second TYPE line for metric name {name:?}, or TYPE reported after samples"
                )
            }
            ErrorKind::UnknownMetricType { got } => {
                write!(f, "unknown metric type {got:?}")
            }
            ErrorKind::DuplicateLabelNames { family } => {
                write!(f, "duplicate label names for metric {family:?}")
            }
            ErrorKind::NegativeHistogramCount { family } => {
                write!(f, "negative count for histogram {family:?}")
            }
            ErrorKind::NegativeHistogramBucket { family } => {
                write!(f, "negative bucket population for histogram {family:?}")
            }
            ErrorKind::NegativeSummaryCount { family } => {
                write!(f, "negative count for summary {family:?}")
            }
            ErrorKind::InvalidUtf8 { context } => {
                write!(f, "invalid UTF-8 in {context}")
            }
            ErrorKind::UnexpectedEof => write!(f, "unexpected end of input stream"),
            ErrorKind::InvalidProtoMessage { got } => {
                write!(f, "invalid protobuf message: {got}")
            }
            ErrorKind::MissingMetricValue { family } => {
                write!(f, "missing value for metric of family {family:?}")
            }
            ErrorKind::UnknownProtoMetricType { got } => {
                write!(f, "unknown metric type {got}")
            }
        }
    }
}

impl ErrorKind {
    /// Proto-format kinds report their position as a message or metric
    /// ordinal in [`Error`]'s Display instead of an input line.
    fn is_proto(&self) -> bool {
        matches!(
            self,
            ErrorKind::InvalidProtoMessage { .. }
                | ErrorKind::MissingMetricValue { .. }
                | ErrorKind::UnknownProtoMetricType { .. }
        )
    }
}

/// Go `%q` formatting for raw byte strings: double quotes with `\` escapes
/// and `\xNN` for non-printable or invalid UTF-8 bytes.
pub(crate) fn go_quote(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() + 2);
    out.push('"');
    for &b in bytes {
        match b {
            b'"' => out.push_str("\\\""),
            b'\\' => out.push_str("\\\\"),
            b'\n' => out.push_str("\\n"),
            b'\t' => out.push_str("\\t"),
            b'\r' => out.push_str("\\r"),
            0x20..=0x7e => out.push(b as char),
            _ => out.push_str(&format!("\\x{b:02x}")),
        }
    }
    out.push('"');
    out
}

/// What a [`crate::Mode`] wants to happen when a line violates it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// Abort the whole parse with the error (StrictMode).
    Abort,
    /// Skip the offending family and keep parsing (LenientMode).
    SkipFamily,
}
