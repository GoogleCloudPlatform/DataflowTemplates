/*
 * Copyright (C) 2018 Google Inc.
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

package com.google.cloud.teleport.spanner.ddl;

import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Scans INFORMATION_SCHEMA.* tables and build {@link Ddl}. */
public class InformationSchemaScanner {

  private static final Logger LOG = LoggerFactory.getLogger(InformationSchemaScanner.class);
  private static final Escaper ESCAPER =
      Escapers.builder()
          .addEscape('"', "\\\"")
          .addEscape('\\', "\\\\")
          .addEscape('\r', "\\r")
          .addEscape('\n', "\\n")
          .build();

  private final ReadContext context;

  public InformationSchemaScanner(ReadContext context) {
    this.context = context;
  }

  public Ddl scan() {
    Ddl.Builder builder = Ddl.builder();
    listTables(builder);
    listColumns(builder);
    listColumnOptions(builder);
    Map<String, NavigableMap<String, Index.Builder>> indexes = Maps.newHashMap();
    listIndexes(indexes);
    listIndexColumns(builder, indexes);

    for (Map.Entry<String, NavigableMap<String, Index.Builder>> tableEntry : indexes.entrySet()) {
      String tableName = tableEntry.getKey();
      ImmutableList.Builder<String> tableIndexes = ImmutableList.builder();
      for (Map.Entry<String, Index.Builder> entry : tableEntry.getValue().entrySet()) {
        Index.Builder indexBuilder = entry.getValue();
        tableIndexes.add(indexBuilder.build().prettyPrint());
      }
      builder.createTable(tableName).indexes(tableIndexes.build()).endTable();
    }

    return builder.build();
  }

  private void listTables(Ddl.Builder builder) {
    ResultSet resultSet =
        context.executeQuery(
            Statement.of(
                "SELECT t.table_name, t.parent_table_name"
                    + " FROM information_schema.tables AS t"
                    + " WHERE t.table_catalog = '' AND t.table_schema = ''"));
    while (resultSet.next()) {
      String tableName = resultSet.getString(0);
      String parentTableName = resultSet.isNull(1) ? null : resultSet.getString(1);
      LOG.debug("Adding table %s", tableName);
      builder.createTable(tableName).interleaveInParent(parentTableName).endTable();
    }
  }

  private void listColumns(Ddl.Builder builder) {
    ResultSet resultSet =
        context.executeQuery(
            Statement.newBuilder(
                    "SELECT c.table_name, c.column_name,"
                        + " c.ordinal_position, c.spanner_type, c.is_nullable"
                        + " FROM information_schema.columns as c"
                        + " WHERE c.table_catalog = '' AND c.table_schema = '' "
                        + " ORDER BY c.table_name, c.ordinal_position")
                .build());
    while (resultSet.next()) {
      String tableName = resultSet.getString(0);
      String columnName = resultSet.getString(1);
      String spannerType = resultSet.getString(3);
      boolean nullable = resultSet.getString(4).equalsIgnoreCase("YES");
      builder
          .createTable(tableName)
          .column(columnName)
          .parseType(spannerType)
          .notNull(!nullable)
          .endColumn()
          .endTable();
    }
  }

  private void listIndexes(Map<String, NavigableMap<String, Index.Builder>> indexes) {
    ResultSet resultSet =
        context.executeQuery(
            Statement.of(
                "SELECT t.table_name, t.index_name, t.parent_table_name,"
                    + " t.is_unique, t.is_null_filtered"
                    + " FROM information_schema.indexes AS t "
                    + " WHERE t.table_catalog = '' AND t.table_schema = '' AND t.index_type='INDEX'"
                    + " ORDER BY t.table_name, t.index_name"));
    while (resultSet.next()) {
      String tableName = resultSet.getString(0);
      String indexName = resultSet.getString(1);
      String parent = resultSet.isNull(2) ? null : resultSet.getString(2);
      // should be NULL but is an empty string in practice.
      if (Strings.isNullOrEmpty(parent)) {
        parent = null;
      }
      boolean unique = resultSet.getBoolean(3);
      boolean nullFiltered = resultSet.getBoolean(3);

      Map<String, Index.Builder> tableIndexes =
          indexes.computeIfAbsent(tableName, k -> Maps.newTreeMap());

      tableIndexes.put(
          indexName,
          Index.builder()
              .name(indexName)
              .table(tableName)
              .unique(unique)
              .nullFiltered(nullFiltered)
              .interleaveIn(parent));
    }
  }

  private void listIndexColumns(
      Ddl.Builder builder, Map<String, NavigableMap<String, Index.Builder>> indexes) {
    ResultSet resultSet =
        context.executeQuery(
            Statement.of(
                "SELECT t.table_name, t.column_name, t.column_ordering, t.index_name "
                    + "FROM information_schema.index_columns AS t "
                    + "WHERE t.table_catalog = '' AND t.table_schema "
                    + "= ''  ORDER BY t.table_name, t.index_name, t.ordinal_position"));
    while (resultSet.next()) {
      String tableName = resultSet.getString(0);
      String columnName = resultSet.getString(1);
      String ordering = resultSet.isNull(2) ? null : resultSet.getString(2);
      String indexName = resultSet.getString(3);

      if (indexName.equals("PRIMARY_KEY")) {
        IndexColumn.IndexColumnsBuilder<Table.Builder> pkBuilder =
            builder.createTable(tableName).primaryKey();
        if (ordering.equalsIgnoreCase("ASC")) {
          pkBuilder.asc(columnName).end();
        } else {
          pkBuilder.desc(columnName).end();
        }
        pkBuilder.end().endTable();
      } else {
        Index.Builder indexBuilder = indexes.get(tableName).get(indexName);
        if (ordering == null) {
          indexBuilder.columns().storing(columnName).end();
        } else if (ordering.equalsIgnoreCase("ASC")) {
          indexBuilder.columns().asc(columnName).end();
        } else if (ordering.equalsIgnoreCase("DESC")) {
          indexBuilder.columns().desc(columnName).end();
        }
      }
    }
  }

  private void listColumnOptions(Ddl.Builder builder) {
    ResultSet resultSet =
        context.executeQuery(
            Statement.newBuilder(
                    "SELECT t.table_name, t.column_name, t.option_name, t.option_type, t.option_value "
                        + " FROM information_schema.column_options AS t "
                        + " WHERE t.table_catalog = '' AND t.table_schema = ''"
                        + " ORDER BY t.table_name, t.column_name")
                .build());

    Map<KV<String, String>, ImmutableList.Builder<String>> allOptions = Maps.newHashMap();
    while (resultSet.next()) {
      String tableName = resultSet.getString(0);
      String columnName = resultSet.getString(1);
      String optionName = resultSet.getString(2);
      String optionType = resultSet.getString(3);
      String optionValue = resultSet.getString(4);

      KV<String, String> kv = KV.of(tableName, columnName);
      ImmutableList.Builder<String> options =
          allOptions.computeIfAbsent(kv, k -> ImmutableList.builder());
      if (optionType.equalsIgnoreCase("STRING")) {
        options.add(optionName + "=\"" + ESCAPER.escape(optionValue) + "\"");
      } else {
        options.add(optionName + "=" + optionValue);
      }
    }

    for (Map.Entry<KV<String, String>, ImmutableList.Builder<String>> entry :
        allOptions.entrySet()) {
      String tableName = entry.getKey().getKey();
      String columnName = entry.getKey().getValue();
      ImmutableList<String> options = entry.getValue().build();
      builder
          .createTable(tableName)
          .column(columnName)
          .columnOptions(options)
          .endColumn()
          .endTable();
    }
  }
}
