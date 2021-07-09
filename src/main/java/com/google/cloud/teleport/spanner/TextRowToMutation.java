/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.spanner;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.spanner.TextImportProtos.ImportManifest.TableManifest;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.Table;
import com.google.common.base.Strings;
import com.google.common.primitives.Longs;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/**
 * Converts a line in text file into a {@link Mutation} objects.
 *
 * <p>This class receives a pair of file name and table name. It reads the file and converts each
 * row into a {@link Mutation}, using the table schema retrieved from {@link Ddl} passed in as a
 * side input. When the text cannot be converted to the requested table schema, an
 * IllegalArgumentException will be raised.
 *
 * <p>Input PCollection is a @{code KV<filePath, tableName>}
 */
class TextRowToMutation extends DoFn<KV<String, String>, Mutation> {

  // Schema of the destination Spanner database.
  private final PCollectionView<Ddl> ddlView;
  private final PCollectionView<Map<String, List<TableManifest.Column>>> tableColumnsView;
  private final ValueProvider<Character> columnDelimiter;
  private final ValueProvider<Character> fieldQualifier;
  private final ValueProvider<Boolean> trailingDelimiter;
  private final ValueProvider<Character> escape;
  private final ValueProvider<String> nullString;
  private final ValueProvider<String> dateFormat;
  private final ValueProvider<String> timestampFormat;

  private Mutation.WriteBuilder writeBuilder = null;

  public TextRowToMutation(
      PCollectionView<Ddl> ddlView,
      PCollectionView<Map<String, List<TableManifest.Column>>> tableColumnsView,
      ValueProvider<Character> columnDelimiter,
      ValueProvider<Character> fieldQualifier,
      ValueProvider<Boolean> trailingDelimiter,
      ValueProvider<Character> escape,
      ValueProvider<String> nullString,
      ValueProvider<String> dateFormat,
      ValueProvider<String> timestampFormat) {
    this.ddlView = ddlView;
    this.tableColumnsView = tableColumnsView;
    this.columnDelimiter = columnDelimiter;
    this.fieldQualifier = fieldQualifier;
    this.trailingDelimiter = trailingDelimiter;
    this.escape = escape;
    this.nullString = nullString;
    this.dateFormat = dateFormat;
    this.timestampFormat = timestampFormat;
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws IOException {
    /**
     * Input string is one line but Apache CSVParser process multiple lines, so we only take the
     * first item in the result list
     */
    KV<String, String> kv = c.element();
    String tableName = kv.getKey();
    Ddl ddl = c.sideInput(ddlView);
    Map<String, List<TableManifest.Column>> tableColumnsMap = c.sideInput(tableColumnsView);
    Table table = ddl.table(tableName);
    Reader in = new StringReader(kv.getValue());
    CSVFormat csvFormat =
        CSVFormat.newFormat(columnDelimiter.get())
            .withQuote(fieldQualifier.get())
            .withIgnoreEmptyLines(true)
            .withTrailingDelimiter(trailingDelimiter.get())
            .withEscape(escape.get())
            .withNullString(nullString.get());
    CSVParser parser = new CSVParser(in, csvFormat);
    List<CSVRecord> list = parser.getRecords();
    if (list.isEmpty()) {
      return;
    }
    if (list.size() > 1) {
      throw new RuntimeException("Unable to parse this row: " + c.element());
    }
    CSVRecord row = list.get(0);
    writeBuilder = Mutation.newInsertOrUpdateBuilder(table.name());
    try {
      c.output(parseRow(writeBuilder, row, table, tableColumnsMap.get(tableName)));
    } catch (IllegalArgumentException e) {
      throw new RuntimeException(
          String.format("Error to parseRow. row: %s, table: %s", row, table), e);
    }
  }

  /**
   * Takes a {@link CSVRecord} and converts it into a Mutation object base on the input table's
   * schema.
   *
   * @param builder MutationBuilder to construct
   * @param row CSVRecord parsed list of data cell
   * @param Table table with column names and column data types
   * @return the Mutation object built from the CSVRecord
   */
  protected final Mutation parseRow(
      Mutation.WriteBuilder builder,
      CSVRecord row,
      Table table,
      List<TableManifest.Column> manifestColumns)
      throws IllegalArgumentException {
    // The input row's column count could be less than or equal to that of DB schema's.
    if (row.size() > table.columns().size()) {
      throw new RuntimeException(
          String.format(
              "Parsed row's column count is larger than that of the schema's. "
                  + "Row size: %d, Column size: %d, Row content: %s",
              row.size(), table.columns().size(), row.toString()));
    }

    if (manifestColumns.size() > 0 && row.size() > manifestColumns.size()) {
      throw new RuntimeException(
          String.format(
              "Parsed row's column count is larger than that of the manifest's column list. "
                  + "Row size: %d, Manifest column size: %d, Row content: %s",
              row.size(), manifestColumns.size(), row.toString()));
    }

    // Extract cell by cell and construct Mutation object
    for (int i = 0; i < row.size(); i++) {
      // If column info is provided in manifest, we use the name from manifest.
      // Otherwise, we use the column name read from DB.
      String columnName =
          manifestColumns != null && manifestColumns.size() > 0
              ? manifestColumns.get(i).getColumnName()
              : table.columns().get(i).name();
      com.google.cloud.teleport.spanner.common.Type columnType = table.column(columnName).type();
      String cellValue = row.get(i);
      boolean isNullValue = Strings.isNullOrEmpty(cellValue);
      Value columnValue = null;
      // TODO: make the tests below match Spanner's SQL literal rules wherever possible,
      // in terms of how input is accepted, and throw exceptions on invalid input.
      switch (columnType.getCode()) {
        case BOOL:
          if (isNullValue) {
            columnValue = Value.bool(null);
          } else {
            Boolean bCellValue;
            if (cellValue.trim().equalsIgnoreCase("true")) {
              bCellValue = Boolean.TRUE;
            } else if (cellValue.trim().equalsIgnoreCase("false")) {
              bCellValue = Boolean.FALSE;
            } else {
              throw new IllegalArgumentException(
                  cellValue.trim() + " is not recognizable value " + "for BOOL type");
            }
            columnValue = Value.bool(Boolean.valueOf(cellValue));
          }
          break;
        case INT64:
          columnValue =
              isNullValue ? Value.int64(null) : Value.int64(Long.valueOf(cellValue.trim()));
          break;
        case FLOAT64:
          columnValue =
              isNullValue ? Value.float64(null) : Value.float64(Double.valueOf(cellValue.trim()));
          break;
        case STRING:
          columnValue = Value.string(cellValue);
          break;
        case DATE:
          if (isNullValue) {
            columnValue = Value.date(null);
          } else {
            LocalDate dt =
                LocalDate.parse(
                    cellValue.trim(),
                    DateTimeFormatter.ofPattern(
                        dateFormat.get() == null
                            ? "yyyy-M[M]-d[d][' 00:00:00']"
                            : dateFormat.get()));
            columnValue =
                Value.date(
                    com.google.cloud.Date.fromYearMonthDay(
                        dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth()));
          }
          break;
        case TIMESTAMP:
          if (isNullValue) {
            columnValue = Value.timestamp(null);
          } else {
            // Timestamp is either a long integer representing Unix epoch time or a string, which
            // will be parsed using the pattern corresponding to the timestampFormat flag.
            Long microseconds = Longs.tryParse(cellValue);
            if (microseconds != null) {
              columnValue =
                  Value.timestamp(com.google.cloud.Timestamp.ofTimeMicroseconds(microseconds));
            } else {
              DateTimeFormatter formatter =
                  timestampFormat.get() == null
                      ? DateTimeFormatter.ISO_INSTANT
                      : DateTimeFormatter.ofPattern(timestampFormat.get());
              Instant ts = Instant.from(formatter.parse(cellValue.trim()));
              columnValue =
                  Value.timestamp(
                      com.google.cloud.Timestamp.ofTimeSecondsAndNanos(
                          ts.getEpochSecond(), ts.getNano()));
            }
          }
          break;
        case NUMERIC:
        case JSON:
          columnValue = isNullValue ? Value.string(null) : Value.string(cellValue.trim());
          break;
        case BYTES:
          columnValue = isNullValue ? Value.bytes(null) : Value.bytes(ByteArray.fromBase64(cellValue.trim()));
          break;
        default:
          throw new IllegalArgumentException(
              "Unrecognized column data type: " + columnType.getCode());
      }

      builder.set(columnName).to(columnValue);
    }

    return builder.build();
  }
}
