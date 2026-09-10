/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Partial VALUES collection and cross-shard list-concatenation UDAFs.
//!
//! `values_partial(value)`      — exact distinct sorted shard-local values.
//! `list_merge(state)`         — concatenates per-shard `List<elem>` states.
//! `list_merge_distinct(state)` — concatenates and re-deduplicates.
//!
//! LIST partials still run DataFusion's native `array_agg`; FINAL routes here so
//! the per-shard arrays flatten into one list rather than
//! getting re-wrapped (DataFusion's substrait consumer ignores AggregationPhase
//! and would lower a FINAL `array_agg(state)` as a single-pass aggregate that
//! treats each row's list as one element).

use std::collections::HashSet;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, ListArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udaf(AggregateUDF::from(ValuesPartialUdaf::new()));
    ctx.register_udaf(AggregateUDF::from(ListMergeUdaf::new(false)));
    ctx.register_udaf(AggregateUDF::from(ListMergeUdaf::new(true)));
}

#[derive(Debug)]
pub struct ValuesPartialUdaf {
    signature: Signature,
}

impl ValuesPartialUdaf {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl PartialEq for ValuesPartialUdaf {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for ValuesPartialUdaf {}
impl Hash for ValuesPartialUdaf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "values_partial".hash(state);
    }
}

impl AggregateUDFImpl for ValuesPartialUdaf {
    fn name(&self) -> &str {
        "values_partial"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        ))))
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ValuesPartialAccumulator::default()))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format!("{}[buf]", args.name),
            args.return_field.data_type().clone(),
            true,
        ))])
    }
}

#[derive(Debug, Default)]
struct ValuesPartialAccumulator {
    values: HashSet<ScalarValue>,
}

impl ValuesPartialAccumulator {
    fn result(&self) -> Result<ScalarValue> {
        let mut values: Vec<ScalarValue> = self.values.iter().cloned().collect();
        let mut compare_error = Ok(());
        values.sort_by(|left, right| {
            left.try_cmp(right).unwrap_or_else(|error| {
                compare_error = Err(error);
                Ordering::Equal
            })
        });
        compare_error?;
        Ok(ScalarValue::List(ScalarValue::new_list_nullable(
            &values,
            &DataType::Utf8,
        )))
    }

    fn add_array(&mut self, values: &ArrayRef) -> Result<()> {
        for i in 0..values.len() {
            if values.is_null(i) {
                continue;
            }
            let value = ScalarValue::try_from_array(values, i)?;
            self.values.insert(value.cast_to(&DataType::Utf8)?);
        }
        Ok(())
    }
}

impl Accumulator for ValuesPartialAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if let Some(values) = values.first() {
            self.add_array(values)?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.result()
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.values.iter().map(ScalarValue::size).sum::<usize>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.result()?])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let Some(states) = states.first() else {
            return Ok(());
        };
        let lists: &ListArray = states.as_list();
        for i in 0..lists.len() {
            if !lists.is_null(i) {
                self.add_array(&lists.value(i))?;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ListMergeUdaf {
    name: &'static str,
    distinct: bool,
    signature: Signature,
}

impl ListMergeUdaf {
    pub fn new(distinct: bool) -> Self {
        Self {
            name: if distinct {
                "list_merge_distinct"
            } else {
                "list_merge"
            },
            distinct,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl PartialEq for ListMergeUdaf {
    fn eq(&self, other: &Self) -> bool {
        self.distinct == other.distinct
    }
}
impl Eq for ListMergeUdaf {}
impl Hash for ListMergeUdaf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

fn list_element_type(dt: &DataType) -> Result<DataType> {
    match dt {
        DataType::List(f) | DataType::LargeList(f) => Ok(f.data_type().clone()),
        DataType::FixedSizeList(f, _) => Ok(f.data_type().clone()),
        other => exec_err!("list_merge: arg 0 must be a list type, got {other:?}"),
    }
}

fn declared_element_type(dt: &DataType) -> Result<DataType> {
    match list_element_type(dt)? {
        DataType::Utf8View => Ok(DataType::Utf8),
        other => Ok(other),
    }
}

impl AggregateUDFImpl for ListMergeUdaf {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return exec_err!("{}() requires one argument", self.name);
        }
        let element = declared_element_type(&arg_types[0])?;
        Ok(DataType::List(Arc::new(Field::new("item", element, true))))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        // The physical input can contain Utf8View even when Substrait declared
        // List<Utf8>. Emit the UDAF's declared return element type so the
        // evaluated ScalarValue matches the aggregate output schema.
        let element_type = list_element_type(acc_args.return_type())?;
        Ok(Box::new(ListMergeAccumulator::new(
            element_type,
            self.distinct,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format!("{}[buf]", args.name),
            args.return_field.data_type().clone(),
            true,
        ))])
    }
}

pub struct ListMergeAccumulator {
    element_type: DataType,
    distinct: bool,
    buf: Vec<ScalarValue>,
    seen: Option<HashSet<ScalarValue>>,
}

impl ListMergeAccumulator {
    pub fn new(element_type: DataType, distinct: bool) -> Self {
        Self {
            element_type,
            distinct,
            buf: Vec::new(),
            seen: if distinct { Some(HashSet::new()) } else { None },
        }
    }

    fn push(&mut self, value: ScalarValue) {
        if let Some(seen) = self.seen.as_mut() {
            if !seen.insert(value.clone()) {
                return;
            }
        }
        self.buf.push(value);
    }

    fn current_list(&self) -> ScalarValue {
        ScalarValue::List(ScalarValue::new_list_nullable(
            &self.buf,
            &self.element_type,
        ))
    }
}

impl Debug for ListMergeAccumulator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListMergeAccumulator")
            .field("distinct", &self.distinct)
            .field("len", &self.buf.len())
            .finish()
    }
}

impl Accumulator for ListMergeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // Each row of values[0] is itself a List of pre-aggregated per-shard elements.
        // Un-nest into the buffer (deduplicating when distinct).
        if values.is_empty() {
            return Ok(());
        }
        let lists: &ListArray = values[0].as_list();
        for i in 0..lists.len() {
            if lists.is_null(i) {
                continue;
            }
            let inner = lists.value(i);
            for j in 0..inner.len() {
                let value = ScalarValue::try_from_array(&inner, j)?;
                let value = if value.data_type() == self.element_type {
                    value
                } else {
                    value.cast_to(&self.element_type)?
                };
                self.push(value);
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.distinct {
            let mut compare_error = Ok(());
            self.buf.sort_by(|left, right| {
                left.try_cmp(right).unwrap_or_else(|error| {
                    compare_error = Err(error);
                    Ordering::Equal
                })
            });
            compare_error?;
        }
        Ok(self.current_list())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.buf.iter().map(|s| s.size()).sum::<usize>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.current_list()])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // Same un-nest as update_batch — DataFusion's substrait consumer ignores
        // AggregationPhase, so this path is only hit by in-process callers (tests).
        self.update_batch(states)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        Int32Array, ListBuilder, StringArray, StringBuilder, StringViewBuilder,
    };

    fn list_of_ints(rows: &[&[Option<i32>]]) -> ArrayRef {
        let mut builder = ListBuilder::new(datafusion::arrow::array::Int32Builder::new());
        for row in rows {
            for v in *row {
                match v {
                    Some(x) => builder.values().append_value(*x),
                    None => builder.values().append_null(),
                }
            }
            builder.append(true);
        }
        Arc::new(builder.finish()) as ArrayRef
    }

    fn extract_ints(s: ScalarValue) -> Vec<Option<i32>> {
        match s {
            ScalarValue::List(l) => {
                let inner = l.value(0);
                let typed = inner.as_any().downcast_ref::<Int32Array>().unwrap();
                (0..typed.len())
                    .map(|i| {
                        if typed.is_null(i) {
                            None
                        } else {
                            Some(typed.value(i))
                        }
                    })
                    .collect()
            }
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn concat_un_nests_per_shard_lists() {
        let mut acc = ListMergeAccumulator::new(DataType::Int32, false);
        let input = list_of_ints(&[&[Some(1), Some(2)], &[Some(3), Some(4), Some(5)]]);
        acc.update_batch(&[input]).unwrap();
        assert_eq!(
            extract_ints(acc.evaluate().unwrap()),
            vec![Some(1), Some(2), Some(3), Some(4), Some(5)]
        );
    }

    #[test]
    fn distinct_dedupes_across_shards() {
        let mut acc = ListMergeAccumulator::new(DataType::Int32, true);
        let input = list_of_ints(&[&[Some(1), Some(2), Some(3)], &[Some(2), Some(3), Some(4)]]);
        acc.update_batch(&[input]).unwrap();
        let mut out = extract_ints(acc.evaluate().unwrap());
        out.sort();
        assert_eq!(out, vec![Some(1), Some(2), Some(3), Some(4)]);
    }

    #[test]
    fn null_lists_are_skipped() {
        let mut builder = ListBuilder::new(datafusion::arrow::array::Int32Builder::new());
        builder.values().append_value(1);
        builder.values().append_value(2);
        builder.append(true);
        builder.append(false); // null list
        builder.values().append_value(3);
        builder.append(true);
        let input: ArrayRef = Arc::new(builder.finish());

        let mut acc = ListMergeAccumulator::new(DataType::Int32, false);
        acc.update_batch(&[input]).unwrap();
        assert_eq!(
            extract_ints(acc.evaluate().unwrap()),
            vec![Some(1), Some(2), Some(3)]
        );
    }

    #[test]
    fn string_concat() {
        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.values().append_value("a");
        builder.values().append_value("b");
        builder.append(true);
        builder.values().append_value("c");
        builder.append(true);
        let input: ArrayRef = Arc::new(builder.finish());

        let mut acc = ListMergeAccumulator::new(DataType::Utf8, false);
        acc.update_batch(&[input]).unwrap();
        let result = match acc.evaluate().unwrap() {
            ScalarValue::List(l) => {
                let inner = l.value(0);
                let typed = inner.as_any().downcast_ref::<StringArray>().unwrap();
                (0..typed.len())
                    .map(|i| typed.value(i).to_string())
                    .collect::<Vec<_>>()
            }
            o => panic!("expected list, got {o:?}"),
        };
        assert_eq!(result, vec!["a".to_string(), "b".into(), "c".into()]);
    }

    #[test]
    fn string_view_input_conforms_to_declared_utf8_output() {
        let mut builder = ListBuilder::new(StringViewBuilder::new());
        builder.values().append_value("a");
        builder.values().append_value("b");
        builder.append(true);
        let input: ArrayRef = Arc::new(builder.finish());

        let mut acc = ListMergeAccumulator::new(DataType::Utf8, true);
        acc.update_batch(&[input]).unwrap();
        let result = acc.evaluate().unwrap();
        assert_eq!(
            result.data_type(),
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        );

        let udaf = ListMergeUdaf::new(true);
        assert_eq!(
            udaf.return_type(&[DataType::List(Arc::new(Field::new(
                "item",
                DataType::Utf8View,
                true
            )))])
            .unwrap(),
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        );
    }

    #[test]
    fn values_partial_normalizes_deduplicates_and_sorts_string_views() {
        let input: ArrayRef = Arc::new(datafusion::arrow::array::StringViewArray::from(vec![
            Some("host-c"),
            Some("host-a"),
            Some("host-c"),
            None,
            Some("host-b"),
        ]));
        let mut acc = ValuesPartialAccumulator::default();
        acc.update_batch(&[input]).unwrap();

        let result = acc.evaluate().unwrap();
        assert_eq!(
            result.data_type(),
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        );
        let ScalarValue::List(list) = result else {
            panic!("expected list");
        };
        let values = list.value(0);
        let values = values.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            (0..values.len()).map(|i| values.value(i)).collect::<Vec<_>>(),
            vec!["host-a", "host-b", "host-c"]
        );
    }
}
