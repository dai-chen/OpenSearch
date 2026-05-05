# PoC Plan: PPL UDF Cleanup — No UDT, Uniform Rewriter

## Problem

PPL datetime functions (DATE_ADD, LAST_DAY, DATE, TIMESTAMP, NOW) return `ExprSqlType` UDT
(VARCHAR with `EXPR_TIMESTAMP` typeString). This UDT:
- Cannot be converted by isthmus `TypeConverter.DEFAULT` → Substrait conversion fails
- Triggers `PPLBuiltinOperators.TIMESTAMP(...)` cast wrapping for cross-family comparisons
- Creates a non-standard type that doesn't map to Substrait or DataFusion

## Solution: Option A — Uniform Rewriter

One rewriter replaces UDT return types with standard Calcite types on ALL PPL datetime function
RexCalls. No special-casing for literal constructors vs computation UDFs. All go through Substrait
as function calls → DataFusion UDFs.

## Architecture

```
PPL Query
  │
  ▼
UnifiedQueryPlanner.plan(ppl) → RelNode (with UDT types on datetime UDF RexCalls)
  │
  ▼
DatetimeTypeRewriter (applied in analytics engine before Substrait conversion)
  ├─ Walks RelNode via RelHomogeneousShuttle + RexShuttle
  ├─ For each RexCall to a PPL datetime function:
  │     Replace return type from UDT → standard TIMESTAMP/DATE/TIME
  └─ Adds final LogicalProject: CAST(datetime_field → VARCHAR) for all output columns
  │
  ▼
DataFusionFragmentConvertor.convertToSubstrait()
  │  (Isthmus handles standard types natively)
  ▼
Substrait Plan (functions referenced via functions_opensearch_datetime.yaml)
  │
  ▼
DataFusion (ScalarUDFs registered: date_add, last_day, date, timestamp, now)
  │
  ▼
Arrow result (VARCHAR output columns)
```

## Changes

### Change 1: DatetimeTypeRewriter (unified-query-api `api` module)

Applied BEFORE Substrait conversion, AFTER UnifiedQueryPlanner.plan():

1. **RexShuttle**: For each RexCall to a PPL datetime function with UDT return type,
   replace return type with standard Calcite type (TIMESTAMP/DATE/TIME)
2. **Final Project**: Add top-level LogicalProject that wraps all datetime output fields
   in CAST(field AS VARCHAR)

No changes to SQL plugin `core` module. No changes to function operator definitions.

### Change 2: Substrait Extension YAML + Loader (this repo)

File: `src/main/resources/extensions/functions_opensearch_datetime.yaml`

Declares: date_add, last_day, date, timestamp, now with standard type signatures.

SubstraitExtensionLoader: Add YAML to CUSTOM_YAML_RESOURCES.
DataFusionFragmentConvertor: Map Calcite operators → Substrait function names.

### Change 3: DataFusion ScalarUDFs (this repo, Rust)

| UDF       | Input                      | Output             | Logic                        |
|-----------|----------------------------|--------------------|-----------------------------|
| date_add  | Timestamp + IntervalDayTime| Timestamp          | timestamp + interval         |
| last_day  | Timestamp                  | Timestamp          | last day of month            |
| date      | Utf8                       | Date32             | parse string → date          |
| timestamp | Utf8                       | TimestampNanosecond| parse string → timestamp     |
| now       | (none)                     | TimestampNanosecond| current time                 |

### Change 4: E2E Test

Tests proving full pipeline works without UDT:
1. `source=t | eval d = DATE_ADD(created, INTERVAL 1 DAY)` → d is VARCHAR string
2. `source=t | where created > TIMESTAMP('2024-01-01 00:00:00')` → filter works
3. `source=t | eval d = LAST_DAY(created) | where d > created` → comparison works
4. `source=t | where created > DATE('2024-01-01') | eval d = DATE_ADD(created, INTERVAL 1 DAY)`

## Dependency Strategy

```bash
# 1. Build SQL plugin with DatetimeTypeRewriter:
cd /path/to/sql-repo
./gradlew :api:publishToMavenLocal

# 2. Build & test in OpenSearch repo:
./gradlew :sandbox:plugins:analytics-backend-datafusion:test --tests "*.DatetimeUdfE2ETests"
```

## What We Don't Touch

- ❌ SQL plugin `core` module
- ❌ PPL function operator definitions (they keep UDT internally)
- ❌ TimestampFunctionAdapter (becomes dead code)
- ❌ ExtendedRexBuilder UDT cast wrapping logic

## Execution Order

| Phase | Where          | What                                                    |
|-------|----------------|---------------------------------------------------------|
| 1     | This repo (Java)| Substrait YAML + extension loader + operator mapping   |
| 2     | This repo (Rust)| DataFusion ScalarUDFs                                  |
| 3     | SQL plugin api  | DatetimeTypeRewriter                                   |
| 4     | SQL plugin      | publishToMavenLocal                                    |
| 5     | This repo (Java)| E2E test                                               |
