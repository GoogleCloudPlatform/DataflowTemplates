/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.it.matchers;

import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.datastore.Entity;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.common.truth.Fact;
import com.google.common.truth.FailureMetadata;
import com.google.common.truth.Subject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericRecord;

/**
 * Subject that has assertion operations for record lists, usually coming from the result of a
 * template.
 */
public final class RecordsSubject extends Subject {

  @Nullable private final List<Map<String, Object>> actual;

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static final TypeReference<Map<String, Object>> recordTypeReference =
      new TypeReference<>() {};

  private RecordsSubject(FailureMetadata metadata, @Nullable List<Map<String, Object>> actual) {
    super(metadata, actual);
    this.actual = actual;
  }

  public static Factory<RecordsSubject, List<Map<String, Object>>> records() {
    return RecordsSubject::new;
  }

  /** Check if records list has rows (i.e., is not empty). */
  public void hasRows() {
    check("there are rows").that(actual.size()).isGreaterThan(0);
  }

  /**
   * Check if records list has a specific number of rows.
   *
   * @param expectedRows Expected Rows
   */
  public void hasRows(int expectedRows) {
    check("there are %s rows", expectedRows).that(actual.size()).isEqualTo(expectedRows);
  }

  /**
   * Check if the records list has a specific row.
   *
   * @param record Expected row to search
   */
  public void hasRecord(Map<String, Object> record) {
    check("expected that contains record %s", record.toString()).that(actual).contains(record);
  }

  /**
   * Check if the records list matches a specific row (using partial / subset comparison).
   *
   * @param subset Expected subset to search in a record.
   */
  public void hasRecordSubset(Map<String, Object> subset) {

    Map<String, Object> expected = new TreeMap<>(subset);
    for (Map<String, Object> candidate : actual) {
      boolean match = true;
      for (Map.Entry<String, Object> entry : subset.entrySet()) {
        if (!candidate.containsKey(entry.getKey())
            || !candidate.get(entry.getKey()).equals(entry.getValue())) {
          match = false;
          break;
        }
      }

      if (match) {
        return;
      }
    }

    failWithoutActual(
        Fact.simpleFact(
            "expected that contains partial record " + expected + ", but only had " + actual));
  }

  /**
   * Check if the records list has specific rows, without guarantees of ordering. The way that
   * ordering is taken out of the equation is by converting all records to TreeMap, which guarantee
   * natural key ordering.
   *
   * @param records Expected rows to search
   */
  public void hasRecordsUnordered(List<Map<String, Object>> records) {

    for (Map<String, Object> record : records) {
      String expected = new TreeMap<>(record).toString();
      if (actual.stream()
          .noneMatch(candidate -> new TreeMap<>(candidate).toString().equals(expected))) {
        failWithoutActual(
            Fact.simpleFact(
                "expected that contains unordered record "
                    + expected
                    + ", but only had "
                    + actual));
      }
    }
  }

  /**
   * Check if the records list has specific rows, without guarantees of ordering. The way that
   * ordering is taken out of the equation is by converting all records to TreeMap, which guarantee
   * natural key ordering.
   *
   * <p>In this particular method, force the columns to be case-insensitive to maximize
   * compatibility.
   *
   * @param records Expected rows to search
   */
  public void hasRecordsUnorderedCaseInsensitiveColumns(List<Map<String, Object>> records) {

    for (Map<String, Object> record : records) {
      String expected = convertKeysToUpperCase(new TreeMap<>(record)).toString();
      if (actual.stream()
          .noneMatch(
              candidate ->
                  convertKeysToUpperCase(new TreeMap<>(candidate)).toString().equals(expected))) {
        failWithoutActual(
            Fact.simpleFact(
                "expected that contains unordered record (and case insensitive) "
                    + expected
                    + ", but only had "
                    + actual));
      }
    }
  }

  private TreeMap<String, Object> convertKeysToUpperCase(Map<String, Object> map) {
    return new TreeMap<>(
        map.entrySet().stream()
            .collect(Collectors.toMap(entry -> entry.getKey().toUpperCase(), Entry::getValue)));
  }

  /**
   * Check if the records list has a specific row, without guarantees of ordering. The way that
   * ordering is taken out of the equation is by converting all records to TreeMap, which guarantee
   * natural key ordering.
   *
   * @param record Expected row to search
   */
  public void hasRecordUnordered(Map<String, Object> record) {
    this.hasRecordsUnordered(List.of(record));
  }

  /**
   * Check if the records list match exactly another list.
   *
   * @param records Expected records
   */
  public void hasRecords(List<Map<String, Object>> records) {
    check("records %s are there", records.toString())
        .that(actual)
        .containsExactlyElementsIn(records);
  }

  /**
   * Check if all the records match given record.
   *
   * @param record Expected record
   */
  public void allMatch(Map<String, Object> record) {
    List<Map<String, Object>> records = Collections.nCopies(actual.size(), record);
    hasRecords(records);
  }

  /**
   * Convert BigQuery {@link TableResult} to a list of maps.
   *
   * @param tableResult Table Result to parse
   * @return List of maps to use in {@link RecordsSubject}
   */
  public static List<Map<String, Object>> tableResultToRecords(TableResult tableResult) {
    try {
      List<Map<String, Object>> records = new ArrayList<>();

      for (FieldValueList row : tableResult.iterateAll()) {
        String jsonRow = row.get(0).getStringValue();
        Map<String, Object> converted = objectMapper.readValue(jsonRow, recordTypeReference);
        records.add(converted);
      }

      return records;
    } catch (Exception e) {
      throw new RuntimeException("Error converting TableResult to Records", e);
    }
  }

  /**
   * Convert Spanner {@link Struct} list to a list of maps.
   *
   * @param structs Structs to parse
   * @return List of maps to use in {@link RecordsSubject}
   */
  public static List<Map<String, Object>> structsToRecords(List<Struct> structs) {
    try {
      List<Map<String, Object>> records = new ArrayList<>();

      for (Struct struct : structs) {
        Map<String, Object> record = new HashMap<>();

        for (Type.StructField field : struct.getType().getStructFields()) {
          Value fieldValue = struct.getValue(field.getName());
          // May need to explore using typed methods instead of .toString()
          record.put(field.getName(), fieldValue.toString());
        }

        records.add(record);
      }

      return records;
    } catch (Exception e) {
      throw new RuntimeException("Error converting TableResult to Records", e);
    }
  }

  /**
   * Convert Avro {@link GenericRecord} to a list of maps.
   *
   * @param avroRecords Avro Records to parse
   * @return List of maps to use in {@link RecordsSubject}
   */
  public static List<Map<String, Object>> genericRecordToRecords(List<GenericRecord> avroRecords) {
    try {
      List<Map<String, Object>> records = new ArrayList<>();

      for (GenericRecord row : avroRecords) {
        Map<String, Object> converted = objectMapper.readValue(row.toString(), recordTypeReference);
        records.add(converted);
      }

      return records;
    } catch (Exception e) {
      throw new RuntimeException("Error converting Avro Record to Map", e);
    }
  }

  /**
   * Convert BigQuery {@link InsertAllRequest.RowToInsert} to a list of maps.
   *
   * @param rows BigQuery rows to parse.
   * @return List of maps to use in {@link RecordsSubject}
   */
  public static List<Map<String, Object>> bigQueryRowsToRecords(
      List<InsertAllRequest.RowToInsert> rows) {
    try {
      List<Map<String, Object>> records = new ArrayList<>();
      rows.forEach(row -> records.add(new HashMap<>(row.getContent())));

      return records;
    } catch (Exception e) {
      throw new RuntimeException("Error converting BigQuery Row to Map", e);
    }
  }

  /**
   * Convert BigQuery {@link InsertAllRequest.RowToInsert} to a list of maps.
   *
   * @param rows BigQuery rows to parse.
   * @param excludeCols BigQuery columns to filter out of result.
   * @return List of maps to use in {@link RecordsSubject}
   */
  public static List<Map<String, Object>> bigQueryRowsToRecords(
      List<InsertAllRequest.RowToInsert> rows, List<String> excludeCols) {
    List<Map<String, Object>> records = bigQueryRowsToRecords(rows);
    try {
      excludeCols.forEach(col -> records.forEach(row -> row.remove(col)));

      return records;
    } catch (Exception e) {
      throw new RuntimeException("Error converting BigQuery Row to Map", e);
    }
  }

  /**
   * Convert Bigtable {@link com.google.cloud.bigtable.data.v2.models.Row} to a list of maps.
   *
   * @param rows Bigtable rows to parse.
   * @param family Bigtable column family to parse from.
   * @return List of maps to use in {@link RecordsSubject}
   */
  public static List<Map<String, Object>> bigtableRowsToRecords(
      Iterable<com.google.cloud.bigtable.data.v2.models.Row> rows, String family) {
    try {
      List<Map<String, Object>> records = new ArrayList<>();

      for (com.google.cloud.bigtable.data.v2.models.Row row : rows) {
        Map<String, Object> converted = new HashMap<>();
        for (RowCell cell : row.getCells(family)) {

          String col = cell.getQualifier().toStringUtf8();
          String val = cell.getValue().toStringUtf8();
          converted.put(col, val);
        }
        records.add(converted);
      }

      return records;
    } catch (Exception e) {
      throw new RuntimeException("Error converting Bigtable Row to Map", e);
    }
  }

  /**
   * Convert Cassandra {@link Row} list to a list of maps.
   *
   * @param rows Rows to parse.
   * @return List of maps to use in {@link RecordsSubject}.
   */
  public static List<Map<String, Object>> cassandraRowsToRecords(Iterable<Row> rows) {
    try {
      List<Map<String, Object>> records = new ArrayList<>();

      for (Row row : rows) {
        Map<String, Object> converted = new HashMap<>();
        for (ColumnDefinition columnDefinition : row.getColumnDefinitions()) {

          Object value = null;
          if (columnDefinition.getType().equals(DataTypes.TEXT)) {
            value = row.getString(columnDefinition.getName());
          } else if (columnDefinition.getType().equals(DataTypes.INT)) {
            value = row.getInt(columnDefinition.getName());
          }
          converted.put(columnDefinition.getName().toString(), value);
        }
        records.add(converted);
      }

      return records;
    } catch (Exception e) {
      throw new RuntimeException("Error converting Cassandra Rows to Records", e);
    }
  }

  /**
   * Convert Datastore {@link com.google.cloud.datastore.QueryResults} to a list of maps.
   *
   * @param results Results to parse.
   * @return List of maps to use in {@link RecordsSubject}.
   */
  public static List<Map<String, Object>> datastoreResultsToRecords(Collection<Entity> results) {
    try {
      List<Map<String, Object>> records = new ArrayList<>();

      for (Entity entity : results) {
        Map<String, Object> converted = new HashMap<>();

        for (Map.Entry<String, com.google.cloud.datastore.Value<?>> entry :
            entity.getProperties().entrySet()) {
          converted.put(entry.getKey(), entry.getValue().get());
        }
        records.add(converted);
      }

      return records;
    } catch (Exception e) {
      throw new RuntimeException("Error converting Datastore Entities to Records", e);
    }
  }
}
