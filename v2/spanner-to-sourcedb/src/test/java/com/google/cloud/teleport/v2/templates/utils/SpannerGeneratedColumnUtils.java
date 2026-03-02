/*
 * Copyright (C) 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates.utils;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatRecords;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import java.util.*;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerGeneratedColumnUtils {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerGeneratedColumnUtils.class);

  public static ConditionCheck buildConditionCheck(
      Map<String, List<Map<String, Value>>> spannerTableData,
      MySQLResourceManager jdbcResourceManager) {
    ConditionCheck combinedCondition = null;
    for (Map.Entry<String, List<Map<String, Value>>> entry : spannerTableData.entrySet()) {
      String tableName = getTableName(entry.getKey());
      int numRows = entry.getValue().size();
      ConditionCheck c =
          new ConditionCheck() {
            @Override
            protected @UnknownKeyFor @NonNull @Initialized String getDescription() {
              return "Checking num rows in table " + tableName + " with " + numRows + " rows";
            }

            @Override
            protected @UnknownKeyFor @NonNull @Initialized CheckResult check() {
              return new CheckResult(
                  jdbcResourceManager.getRowCount(tableName) == numRows, getDescription());
            }
          };
      if (combinedCondition == null) {
        combinedCondition = c;
      } else {
        combinedCondition = combinedCondition.and(c);
      }
    }

    return combinedCondition;
  }

  public static void assertRowInMySQL(
      Map<String, List<Map<String, Object>>> expectedData,
      MySQLResourceManager jdbcResourceManager) {
    for (Map.Entry<String, List<Map<String, Object>>> expectedTableData : expectedData.entrySet()) {
      String type = expectedTableData.getKey();
      String tableName = getTableName(type);

      List<Map<String, Object>> rawRows;
      if (tableName.equals("time_table")) {
        // JDBC Time objects represent a wall-clock time and not a duration (as MySQL
        // treats them).
        // Need to read them as a string to avoid a DataReadException
        rawRows =
            jdbcResourceManager.runSQLQuery(
                "SELECT id, CAST(time_col as char) as time_col FROM time_table");
      } else {
        rawRows = jdbcResourceManager.readTable(tableName);
      }

      List<Map<String, Object>> rows = cleanValues(rawRows);
      for (Map<String, Object> row : rows) {
        // Limit logs printed for very large strings.
        String rowString = row.toString();
        if (rowString.length() > 1000) {
          rowString = rowString.substring(0, 1000);
        }
        LOG.info("Found row: {}", rowString);
      }

      assertThatRecords(rows)
          .hasRecordsUnorderedCaseInsensitiveColumns(cleanValues(expectedTableData.getValue()));
    }
  }

  // Replaces `null` values with the string "NULL" and byte arrays with the base64
  // encoding of the
  // bytes
  public static List<Map<String, Object>> cleanValues(List<Map<String, Object>> rows) {
    for (Map<String, Object> row : rows) {
      for (Map.Entry<String, Object> entry : row.entrySet()) {
        if (entry.getValue() == null) {
          entry.setValue("NULL");
        } else if (entry.getValue() instanceof byte[]) {
          entry.setValue(Base64.getEncoder().encodeToString((byte[]) entry.getValue()));
        }
      }
    }
    return rows;
  }

  public static void writeRowsInSpanner(
      Map<String, List<Map<String, Value>>> spannerTableData,
      SpannerResourceManager spannerResourceManager) {
    for (Map.Entry<String, List<Map<String, Value>>> tableDataEntry : spannerTableData.entrySet()) {
      String tableName = getTableName(tableDataEntry.getKey());
      List<Map<String, Value>> rows = tableDataEntry.getValue();
      List<Mutation> mutations = new ArrayList<>(rows.size());
      for (Map<String, Value> row : rows) {
        Mutation.WriteBuilder m = Mutation.newInsertOrUpdateBuilder(tableName);
        for (Map.Entry<String, Value> entry : row.entrySet()) {
          m.set(getColumnName(entry.getKey())).to(entry.getValue());
        }
        mutations.add(m.build());
      }
      spannerResourceManager.write(mutations);
    }
  }

  public static void addInitialMultiColSpannerData(
      Map<String, List<Map<String, Value>>> spannerTableData) {
    spannerTableData.put(
        "generated_pk_column",
        List.of(
            Map.of(
                "first_name", Value.string("AA"),
                "last_name", Value.string("BB")),
            Map.of(
                "first_name", Value.string("BB"),
                "last_name", Value.string("CC"))));

    spannerTableData.put(
        "generated_non_pk_column",
        List.of(
            Map.of(
                "id", Value.int64(1),
                "first_name", Value.string("AA"),
                "last_name", Value.string("BB")),
            Map.of(
                "id", Value.int64(2),
                "first_name", Value.string("BB"),
                "last_name", Value.string("CC"))));

    spannerTableData.put(
        "non_generated_to_generated_column",
        List.of(
            Map.of(
                "first_name", Value.string("AA"),
                "last_name", Value.string("BB")),
            Map.of(
                "first_name", Value.string("BB"),
                "last_name", Value.string("CC"))));

    spannerTableData.put(
        "generated_to_non_generated_column",
        List.of(
            Map.of(
                "first_name", Value.string("AA"),
                "last_name", Value.string("BB"),
                "generated_column", Value.string("AA "),
                "generated_column_pk", Value.string("AA ")),
            Map.of(
                "first_name", Value.string("BB"),
                "last_name", Value.string("CC"),
                "generated_column", Value.string("BB "),
                "generated_column_pk", Value.string("BB "))));
  }

  public static Map<String, List<Map<String, Value>>> updateGeneratedColRowsInSpanner(
      SpannerResourceManager spannerResourceManager) {
    Map<String, List<Map<String, Value>>> spannerTableData = new HashMap<>();
    spannerTableData.put(
        "generated_pk_column",
        List.of(
            Map.of(
                "first_name", Value.string("AA"),
                "last_name", Value.string("CC"))));
    spannerTableData.put(
        "generated_non_pk_column",
        List.of(
            Map.of(
                "id", Value.int64(1),
                "first_name", Value.string("AA"),
                "last_name", Value.string("CC"))));
    spannerTableData.put(
        "non_generated_to_generated_column",
        List.of(
            Map.of(
                "first_name", Value.string("AA"),
                "last_name", Value.string("CC"))));
    spannerTableData.put(
        "generated_to_non_generated_column",
        List.of(
            Map.of(
                "first_name", Value.string("AA"),
                "last_name", Value.string("CC"),
                "generated_column", Value.string("AA "),
                "generated_column_pk", Value.string("AA "))));

    writeRowsInSpanner(spannerTableData, spannerResourceManager);
    List<Mutation> deleteMutations = new ArrayList<>();
    deleteMutations.add(Mutation.delete("generated_pk_column_table", Key.of("BB ")));
    deleteMutations.add(Mutation.delete("generated_non_pk_column_table", Key.of(2)));
    deleteMutations.add(Mutation.delete("non_generated_to_generated_column_table", Key.of("BB ")));
    deleteMutations.add(Mutation.delete("generated_to_non_generated_column_table", Key.of("BB ")));
    spannerResourceManager.write(deleteMutations);

    return spannerTableData;
  }

  public static void addInitialGeneratedColumnData(
      Map<String, List<Map<String, Object>>> expectedData) {
    expectedData.put(
        "generated_pk_column",
        List.of(
            Map.of(
                "first_name_col",
                Value.string("AA"),
                "last_name_col",
                Value.string("BB"),
                "generated_column_col",
                Value.string("AA ")),
            Map.of(
                "first_name_col", Value.string("BB"),
                "last_name_col", Value.string("CC"),
                "generated_column_col", Value.string("BB "))));

    expectedData.put(
        "generated_non_pk_column",
        List.of(
            Map.of(
                "id", Value.int64(1),
                "first_name_col", Value.string("AA"),
                "last_name_col", Value.string("BB"),
                "generated_column_col", Value.string("AA ")),
            Map.of(
                "id", Value.int64(2),
                "first_name_col", Value.string("BB"),
                "last_name_col", Value.string("CC"),
                "generated_column_col", Value.string("BB "))));

    expectedData.put(
        "generated_to_non_generated_column",
        List.of(
            Map.of(
                "first_name_col", Value.string("AA"),
                "last_name_col", Value.string("BB"),
                "generated_column_col", Value.string("AA "),
                "generated_column_pk_col", Value.string("AA ")),
            Map.of(
                "first_name_col", Value.string("BB"),
                "last_name_col", Value.string("CC"),
                "generated_column_col", Value.string("BB "),
                "generated_column_pk_col", Value.string("BB "))));

    expectedData.put(
        "non_generated_to_generated_column",
        List.of(
            Map.of(
                "first_name_col",
                Value.string("AA"),
                "last_name_col",
                Value.string("BB"),
                "generated_column_col",
                Value.string("AA "),
                "generated_column_pk_col",
                Value.string("AA ")),
            Map.of(
                "first_name_col", Value.string("BB"),
                "last_name_col", Value.string("CC"),
                "generated_column_col", Value.string("BB "),
                "generated_column_pk_col", Value.string("BB "))));
  }

  public static void addUpdatedGeneratedColumnData(
      Map<String, List<Map<String, Object>>> expectedData) {
    expectedData.put(
        "generated_pk_column",
        List.of(
            Map.of(
                "first_name_col", Value.string("AA"),
                "last_name_col", Value.string("CC"),
                "generated_column_col", Value.string("AA "))));

    expectedData.put(
        "generated_non_pk_column",
        List.of(
            Map.of(
                "id", Value.int64(1),
                "first_name_col", Value.string("AA"),
                "last_name_col", Value.string("CC"),
                "generated_column_col", Value.string("AA "))));

    expectedData.put(
        "generated_to_non_generated_column",
        List.of(
            Map.of(
                "first_name_col", Value.string("AA"),
                "last_name_col", Value.string("CC"),
                "generated_column_col", Value.string("AA "),
                "generated_column_pk_col", Value.string("AA "))));

    expectedData.put(
        "non_generated_to_generated_column",
        List.of(
            Map.of(
                "first_name_col", Value.string("AA"),
                "last_name_col", Value.string("CC"),
                "generated_column_col", Value.string("AA "),
                "generated_column_pk_col", Value.string("AA "))));
  }

  public static String getTableName(String type) {
    return type + "_table";
  }

  public static String getColumnName(String type) {
    if (type.equals("id")) {
      return type;
    }
    return type + "_col";
  }
}
