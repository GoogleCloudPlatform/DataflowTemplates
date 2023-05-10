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
package com.google.cloud.teleport.it.cassandra.matchers;

import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatRecords;

import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.google.cloud.teleport.it.common.matchers.RecordsSubject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

public class CassandraAsserts {

  /**
   * Convert Cassandra {@link com.datastax.oss.driver.api.core.cql.Row} list to a list of maps.
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
   * Creates a {@link RecordsSubject} to assert information within a list of records.
   *
   * @param rows Records in Cassandra's {@link Row} format to use in the comparison.
   * @return Truth Subject to chain assertions.
   */
  public static RecordsSubject assertThatCassandraRecords(@Nullable Iterable<Row> rows) {
    return assertThatRecords(cassandraRowsToRecords(rows));
  }
}
