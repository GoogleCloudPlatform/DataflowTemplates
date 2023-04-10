/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.it.gcp.bigtable.matchers;

import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatRecords;

import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.teleport.it.common.matchers.RecordsSubject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

public class BigtableAsserts {

  /**
   * Convert Bigtable {@link Row} to a list of maps.
   *
   * @param rows Bigtable rows to parse.
   * @param family Bigtable column family to parse from.
   * @return List of maps to use in {@link RecordsSubject}
   */
  public static List<Map<String, Object>> bigtableRowsToRecords(Iterable<Row> rows, String family) {
    try {
      List<Map<String, Object>> records = new ArrayList<>();

      for (Row row : rows) {
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
   * Creates a {@link RecordsSubject} to assert information within a list of records.
   *
   * @param rows Records in Bigtable's {@link Row} format to use in the comparison.
   * @param family The column family to read records from.
   * @return Truth Subject to chain assertions.
   */
  public static RecordsSubject assertThatBigtableRecords(
      @Nullable Iterable<Row> rows, String family) {
    return assertThatRecords(bigtableRowsToRecords(rows, family));
  }
}
