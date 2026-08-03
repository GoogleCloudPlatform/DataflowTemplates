# Design Spec: Symmetric ResultSet Parsing for Collation Orders

- **Date**: 2026-08-03
- **Status**: Approved

## 1. Context & Motivation

When querying database collation character orders for string partitioning, different database engines return different result set shapes:
- **`WITH_RANKS`**: Query returns pre-computed dense ranks (`codepoint_rank`, `equivalent_charset_char`, etc.). Used by PostgreSQL and offline benchmark TSV files.
- **`WEIGHT_BYTES`**: Query returns raw `WEIGHT_STRING` sort-key bytes (`weight_non_trailing`, `weight_trailing`). Java performs sorting and rank assignment. Used by MySQL 5.7 and 8.0.

Previously, `CollationOrderRow.fromRS(rs)` was named generically, but it specifically expected `WITH_RANKS` columns, creating ambiguity when reading MySQL `WEIGHT_BYTES` result sets.

This design standardizes the JDBC `ResultSet` parsing interface across dialects using symmetric factory methods on `CollationOrderRow`.

---

## 2. Architecture & Design

### A. `CollationOrderRow.java`

1. **Standardized Ranked Query Contract (`fromRS`)**:
   - `public static CollationOrderRow fromRS(ResultSet rs) throws SQLException`
   - Explicitly parses standard pre-computed rank rows using `CollationsOrderQueryColumns` (`charset_char`, `equivalent_charset_char`, `codepoint_rank`, etc.).
   - Used by PostgreSQL and offline TSV benchmark files.

---

### B. `MysqlDialectAdapter.java`

1. **Dialect-Specific Query Contract & Intermediate Model**:
   - Defines private static `MysqlCollationOrderQueryColumns` (`charset_char`, `is_empty`, `is_space`, `weight_non_trailing`, `weight_trailing`).
   - Owns private static class `CharacterWeightRow` and `CharacterWeightRow.fromRS(rs)` representing intermediate MySQL sort-key bytes.
2. **Result Set Processing**:
   - `processCollationResultSet(ResultSet rs, CollationReference collationReference)` iterates through `rs` by calling `CharacterWeightRow.fromRS(rs)`.
   - Performs grouping, sorting, and ranking on `CharacterWeightRow` instances in Java memory to produce `List<CollationOrderRow>`.

---

### C. `UniformSplitterDBAdapter.java`

- Default `processCollationResultSet(ResultSet rs, CollationReference collationReference)`:
  - Iterates through `rs` by calling `CollationOrderRow.fromRS(rs)`.

---

### D. Unit Tests

- `CollationOrderRowTest.java`: Tests standard `fromRS` parsing for ranked collation rows.
- `MysqlDialectAdapterTest.java`: Verifies in-memory sorting and ranking of MySQL collation rows.

---

## 3. Verification & Acceptance Criteria

1. **Unit Tests**:
   - All tests in `CollationOrderRowTest`, `CollationIndexTest`, `CollationMapperTest`, `CollationMapperDoFnTest`, `PostgreSQLDialectAdapterTest`, and `MysqlDialectAdapterTest` must pass.
2. **Code Style**:
   - `mvn checkstyle:check` passes with zero violations.
