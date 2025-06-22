/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.spanner.sourceddl;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** MySQL implementation of {@link SourceInformationSchemaScanner}. */
public class MySqlInformationSchemaScanner extends AbstractSourceInformationSchemaScanner {

  private static final Logger LOG = LoggerFactory.getLogger(MySqlInformationSchemaScanner.class);

  public MySqlInformationSchemaScanner(Connection connection, String databaseName) {
    super(connection, databaseName, SourceDatabaseType.MYSQL);
  }

  @Override
  protected String getTablesQuery() {
    return String.format(
        "SELECT table_name, table_schema "
            + "FROM information_schema.tables "
            + "WHERE table_schema = '%s' "
            + "AND table_type = 'BASE TABLE'",
        databaseName);
  }

  @Override
  protected List<SourceColumn> scanColumns(String tableName, String schema) throws SQLException {
    List<SourceColumn> columns = new ArrayList<>();
    String query =
        String.format(
            "SELECT column_name, data_type, character_maximum_length, "
                + "numeric_precision, numeric_scale, is_nullable, column_key "
                + "FROM information_schema.columns "
                + "WHERE table_schema = '%s' AND table_name = '%s' "
                + "ORDER BY ordinal_position",
            schema, tableName);

    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        SourceColumn.Builder columnBuilder =
            SourceColumn.builder(sourceType)
                .name(rs.getString("column_name"))
                .type(rs.getString("data_type"))
                .isNullable("YES".equals(rs.getString("is_nullable")))
                .isPrimaryKey("PRI".equals(rs.getString("column_key")));

        // Handle size/precision/scale
        String maxLength = rs.getString("character_maximum_length");
        if (maxLength != null) {
          columnBuilder.size(Integer.parseInt(maxLength));
        }

        String precision = rs.getString("numeric_precision");
        if (precision != null) {
          columnBuilder.precision(Integer.parseInt(precision));
        }

        String scale = rs.getString("numeric_scale");
        if (scale != null) {
          columnBuilder.scale(Integer.parseInt(scale));
        }

        columnBuilder.columnOptions(ImmutableList.of());
        columns.add(columnBuilder.build());
      }
    }
    return columns;
  }

  @Override
  protected List<String> scanPrimaryKeys(String tableName, String schema) throws SQLException {
    List<String> primaryKeys = new ArrayList<>();
    String query =
        String.format(
            "SELECT column_name "
                + "FROM information_schema.key_column_usage "
                + "WHERE table_schema = '%s' AND table_name = '%s' "
                + "AND constraint_name = 'PRIMARY' "
                + "ORDER BY ordinal_position",
            schema, tableName);

    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(query)) {
      while (rs.next()) {
        primaryKeys.add(rs.getString("column_name"));
      }
    }
    return primaryKeys;
  }
}
