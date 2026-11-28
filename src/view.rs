//! The zero-copy read mirror of the model's [`MetricValue`]. Scalar leaves
//! are the same values; the aggregation containers borrow instead of
//! owning — the text model's rows lend their `Vec`s as slices, and the
//! protobuf wire path lends the dto message's decoded repeated fields,
//! converting each element (the int/float twin rule) as the iterator
//! advances. Every read is allocation-free; [`MetricValueRef::to_owned`]
//! materializes the model's own value when ownership is wanted.

use crate::model::{Bucket, HistogramValue, IntOrFloat, MetricValue, Quantile, SummaryValue};
use crate::proto::io::prometheus::client as dto;

/// A borrowed metric value: see [`MetricValueRef`].
#[derive(Debug, Clone)]
pub enum MetricValueRef<'a> {
    Sample(f64),
    Summary(SummaryValueRef<'a>),
    Histogram(HistogramValueRef<'a>),
}

/// Borrowed summary: the quantiles lend as an iterator of the same leaf
/// values the owned model stores.
#[derive(Debug, Clone)]
pub struct SummaryValueRef<'a> {
    pub sample_count: Option<u64>,
    pub sample_sum: Option<f64>,
    pub quantiles: QuantilesRef<'a>,
}

/// Borrowed histogram: the buckets lend as an iterator of the same leaf
/// values the owned model stores.
#[derive(Debug, Clone)]
pub struct HistogramValueRef<'a> {
    pub sample_count: Option<IntOrFloat>,
    pub sample_sum: Option<f64>,
    pub buckets: BucketsRef<'a>,
}

/// Lends the quantiles of a summary: model rows lend their slice, wire
/// messages lend the dto's decoded entries converted per element.
#[derive(Debug, Clone, Copy)]
pub enum QuantilesRef<'a> {
    Slice(&'a [Quantile]),
    Wire(&'a [dto::QuantileView<'a>]),
}

impl QuantilesRef<'_> {
    pub fn len(&self) -> usize {
        match self {
            QuantilesRef::Slice(slice) => slice.len(),
            QuantilesRef::Wire(slice) => slice.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Iterator for QuantilesRef<'_> {
    type Item = Quantile;

    fn next(&mut self) -> Option<Quantile> {
        match self {
            QuantilesRef::Slice(slice) => {
                let (first, rest) = slice.split_first()?;
                *slice = rest;
                Some(first.clone())
            }
            QuantilesRef::Wire(slice) => {
                let (first, rest) = slice.split_first()?;
                *slice = rest;
                Some(Quantile {
                    quantile: first.quantile,
                    value: first.value,
                })
            }
        }
    }
}

/// Lends the buckets of a histogram: model rows lend their slice, wire
/// messages lend the dto's decoded entries converted per element (the
/// int/float twin rule applied on the fly).
#[derive(Debug, Clone, Copy)]
pub enum BucketsRef<'a> {
    Slice(&'a [Bucket]),
    Wire(&'a [dto::BucketView<'a>]),
}

impl BucketsRef<'_> {
    pub fn len(&self) -> usize {
        match self {
            BucketsRef::Slice(slice) => slice.len(),
            BucketsRef::Wire(slice) => slice.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Iterator for BucketsRef<'_> {
    type Item = Bucket;

    fn next(&mut self) -> Option<Bucket> {
        match self {
            BucketsRef::Slice(slice) => {
                let (first, rest) = slice.split_first()?;
                *slice = rest;
                Some(first.clone())
            }
            BucketsRef::Wire(slice) => {
                let (first, rest) = slice.split_first()?;
                *slice = rest;
                Some(Bucket {
                    upper_bound: first.upper_bound,
                    cumulative_count: count(first.cumulative_count, first.cumulative_count_float),
                })
            }
        }
    }
}

/// The dto's int/float twin rule: the float overrides when it is set.
fn count(int: u64, float: f64) -> IntOrFloat {
    if float > 0.0 {
        IntOrFloat::Float(float)
    } else {
        IntOrFloat::Int(int)
    }
}

impl<'a> From<&'a MetricValue> for MetricValueRef<'a> {
    fn from(value: &'a MetricValue) -> Self {
        match value {
            MetricValue::Sample(sample) => MetricValueRef::Sample(*sample),
            MetricValue::Summary(summary) => MetricValueRef::Summary(SummaryValueRef {
                sample_count: summary.sample_count,
                sample_sum: summary.sample_sum,
                quantiles: QuantilesRef::Slice(&summary.quantiles),
            }),
            MetricValue::Histogram(histogram) => MetricValueRef::Histogram(HistogramValueRef {
                sample_count: histogram.sample_count,
                sample_sum: histogram.sample_sum,
                buckets: BucketsRef::Slice(&histogram.buckets),
            }),
        }
    }
}

impl MetricValueRef<'_> {
    /// Materializes the model's own value, decoding any lent container
    /// into its owned `Vec`s.
    pub fn to_owned_value(&self) -> MetricValue {
        match self {
            MetricValueRef::Sample(sample) => MetricValue::Sample(*sample),
            MetricValueRef::Summary(summary) => MetricValue::Summary(SummaryValue {
                sample_count: summary.sample_count,
                sample_sum: summary.sample_sum,
                quantiles: summary.quantiles.collect(),
            }),
            MetricValueRef::Histogram(histogram) => MetricValue::Histogram(HistogramValue {
                sample_count: histogram.sample_count,
                sample_sum: histogram.sample_sum,
                buckets: histogram.buckets.collect(),
            }),
        }
    }
}

/// Compares across the ownership split with the model's own equality
/// (NaN-equal included). Allocates through `to_owned_value` — iterate the
/// lent containers directly for allocation-free reads.
impl PartialEq<MetricValue> for MetricValueRef<'_> {
    fn eq(&self, other: &MetricValue) -> bool {
        self.to_owned_value() == *other
    }
}
