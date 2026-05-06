/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! PPL datetime scalar UDFs for DataFusion.

use std::sync::Arc;

use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, Utc};
use datafusion::arrow::array::{
    Array, ArrayRef, Date32Array, IntervalDayTimeArray, StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

/// Register all datetime UDFs on the given session context.
pub fn register_datetime_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::new_from_impl(DateAddUdf::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(LastDayUdf::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(DateUdf::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(TimestampParseUdf::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(NowUdf::new()));
}

// Helper: convert nanoseconds since epoch to NaiveDateTime
fn nanos_to_datetime(nanos: i64) -> Result<NaiveDateTime> {
    DateTime::<Utc>::from_timestamp(
        nanos.div_euclid(1_000_000_000),
        nanos.rem_euclid(1_000_000_000) as u32,
    )
    .map(|dt| dt.naive_utc())
    .ok_or_else(|| DataFusionError::Execution("Invalid timestamp value".into()))
}

// Helper: convert NaiveDateTime to nanoseconds since epoch
fn datetime_to_nanos(dt: NaiveDateTime) -> Result<i64> {
    dt.and_utc()
        .timestamp_nanos_opt()
        .ok_or_else(|| DataFusionError::Execution("Timestamp overflow".into()))
}

// ═══════════════════════════════════════════════════════════════════════════
// DateAddUdf
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug)]
struct DateAddUdf {
    signature: Signature,
}

impl DateAddUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Interval(IntervalUnit::DayTime),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl std::hash::Hash for DateAddUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}
impl PartialEq for DateAddUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name()
    }
}
impl Eq for DateAddUdf {}

impl ScalarUDFImpl for DateAddUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "date_add"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ts_array = args.args[0].to_owned().into_array(1)?;
        let interval_array = args.args[1].to_owned().into_array(1)?;

        let timestamps = ts_array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| DataFusionError::Internal("Expected TimestampNanosecondArray".into()))?;
        let intervals = interval_array
            .as_any()
            .downcast_ref::<IntervalDayTimeArray>()
            .ok_or_else(|| DataFusionError::Internal("Expected IntervalDayTimeArray".into()))?;

        let mut builder: Vec<Option<i64>> = Vec::with_capacity(timestamps.len());
        for i in 0..timestamps.len() {
            if timestamps.is_null(i) || intervals.is_null(i) {
                builder.push(None);
            } else {
                let dt = nanos_to_datetime(timestamps.value(i))?;
                let interval = intervals.value(i);
                let result =
                    dt + Duration::days(interval.days as i64) + Duration::milliseconds(interval.milliseconds as i64);
                builder.push(Some(datetime_to_nanos(result)?));
            }
        }

        let result_array: ArrayRef = Arc::new(TimestampNanosecondArray::from(builder));
        Ok(ColumnarValue::Array(result_array))
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// LastDayUdf
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug)]
struct LastDayUdf {
    signature: Signature,
}

impl LastDayUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Timestamp(TimeUnit::Nanosecond, None)],
                Volatility::Immutable,
            ),
        }
    }
}

impl std::hash::Hash for LastDayUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}
impl PartialEq for LastDayUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name()
    }
}
impl Eq for LastDayUdf {}

impl ScalarUDFImpl for LastDayUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "last_day"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ts_array = args.args[0].to_owned().into_array(1)?;
        let timestamps = ts_array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| DataFusionError::Internal("Expected TimestampNanosecondArray".into()))?;

        let mut builder: Vec<Option<i64>> = Vec::with_capacity(timestamps.len());
        for i in 0..timestamps.len() {
            if timestamps.is_null(i) {
                builder.push(None);
            } else {
                let dt = nanos_to_datetime(timestamps.value(i))?;
                let (y, m) = if dt.month() == 12 {
                    (dt.year() + 1, 1)
                } else {
                    (dt.year(), dt.month() + 1)
                };
                let last_day = NaiveDate::from_ymd_opt(y, m, 1)
                    .ok_or_else(|| DataFusionError::Execution("Invalid date".into()))?
                    .pred_opt()
                    .ok_or_else(|| DataFusionError::Execution("Date underflow".into()))?
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| DataFusionError::Execution("Invalid time".into()))?;
                builder.push(Some(datetime_to_nanos(last_day)?));
            }
        }

        let result_array: ArrayRef = Arc::new(TimestampNanosecondArray::from(builder));
        Ok(ColumnarValue::Array(result_array))
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// DateUdf
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug)]
struct DateUdf {
    signature: Signature,
}

impl DateUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl std::hash::Hash for DateUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}
impl PartialEq for DateUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name()
    }
}
impl Eq for DateUdf {}

impl ScalarUDFImpl for DateUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "date"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let str_array = args.args[0].to_owned().into_array(1)?;
        let strings = str_array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Internal("Expected StringArray".into()))?;

        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
            .ok_or_else(|| DataFusionError::Internal("Invalid epoch".into()))?;

        let mut builder: Vec<Option<i32>> = Vec::with_capacity(strings.len());
        for i in 0..strings.len() {
            if strings.is_null(i) {
                builder.push(None);
            } else {
                let date = NaiveDate::parse_from_str(strings.value(i), "%Y-%m-%d").map_err(
                    |e| {
                        DataFusionError::Execution(format!(
                            "Failed to parse date '{}': {}",
                            strings.value(i),
                            e
                        ))
                    },
                )?;
                builder.push(Some((date - epoch).num_days() as i32));
            }
        }

        let result_array: ArrayRef = Arc::new(Date32Array::from(builder));
        Ok(ColumnarValue::Array(result_array))
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// TimestampParseUdf
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug)]
struct TimestampParseUdf {
    signature: Signature,
}

impl TimestampParseUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl std::hash::Hash for TimestampParseUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}
impl PartialEq for TimestampParseUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name()
    }
}
impl Eq for TimestampParseUdf {}

impl ScalarUDFImpl for TimestampParseUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "timestamp_parse"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let str_array = args.args[0].to_owned().into_array(1)?;
        let strings = str_array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Internal("Expected StringArray".into()))?;

        let mut builder: Vec<Option<i64>> = Vec::with_capacity(strings.len());
        for i in 0..strings.len() {
            if strings.is_null(i) {
                builder.push(None);
            } else {
                let s = strings.value(i);
                let dt = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                    .or_else(|_| {
                        NaiveDate::parse_from_str(s, "%Y-%m-%d").map(|d| {
                            d.and_hms_opt(0, 0, 0)
                                .expect("midnight is always valid")
                        })
                    })
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to parse timestamp '{}': {}",
                            s, e
                        ))
                    })?;
                builder.push(Some(datetime_to_nanos(dt)?));
            }
        }

        let result_array: ArrayRef = Arc::new(TimestampNanosecondArray::from(builder));
        Ok(ColumnarValue::Array(result_array))
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// NowUdf
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug)]
struct NowUdf {
    signature: Signature,
}

impl NowUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![], Volatility::Volatile),
        }
    }
}

impl std::hash::Hash for NowUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}
impl PartialEq for NowUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name()
    }
}
impl Eq for NowUdf {}

impl ScalarUDFImpl for NowUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "now"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
    }
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let now_nanos = Utc::now().timestamp_nanos_opt().ok_or_else(|| {
            DataFusionError::Execution("Current time exceeds nanosecond range".into())
        })?;
        Ok(ColumnarValue::Scalar(
            datafusion::common::ScalarValue::TimestampNanosecond(Some(now_nanos), None),
        ))
    }
}
