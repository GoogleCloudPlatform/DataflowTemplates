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
package com.google.cloud.teleport.v2.spanner.ddl;

import com.google.cloud.spanner.Dialect;
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
  private final Dialect dialect;

  public InformationSchemaScanner(ReadContext context) {
    this.context = context;
    this.dialect = Dialect.GOOGLE_STANDARD_SQL;
  }

  public InformationSchemaScanner(ReadContext context, Dialect dialect) {
    this.context = context;
    this.dialect = dialect;
  }

  public Ddl scan() {
    Ddl.Builder builder = Ddl.builder(dialect);
    listTables(builder);

    listColumns(builder);
    listColumnOptions(builder);
    Map<String, NavigableMap<String, Index.Builder>> indexes = Maps.newHashMap();
    listIndexes(indexes);
    listIndexColumns(builder, indexes);

    for (Map.Entry<String, NavigableMap<String, Index.Builder>> tableEntry : indexes.entrySet()) {
      String tableName = tableEntry.getKey();
      if (builder.getTable(tableName) == null) {
        continue; // Skipping as table does not exist
      }

      ImmutableList.Builder<String> tableIndexes = ImmutableList.builder();
      for (Map.Entry<String, Index.Builder> entry : tableEntry.getValue().entrySet()) {
        Index.Builder indexBuilder = entry.getValue();
        tableIndexes.add(indexBuilder.build().prettyPrint());
      }
      builder.createTable(tableName).indexes(tableIndexes.build()).endTable();
    }

    Map<String, NavigableMap<String, ForeignKey.Builder>> foreignKeys = Maps.newHashMap();
    listForeignKeys(foreignKeys);

    for (Map.Entry<String, NavigableMap<String, ForeignKey.Builder>> tableEntry :
        foreignKeys.entrySet()) {
      String tableName = tableEntry.getKey();
      if (builder.getTable(tableName) == null) {
        continue; // Skipping as table does not exist
      }

      ImmutableList.Builder<ForeignKey> tableForeignKeys = ImmutableList.builder();
      for (Map.Entry<String, ForeignKey.Builder> entry : tableEntry.getValue().entrySet()) {
        ForeignKey.Builder foreignKeyBuilder = entry.getValue();
        tableForeignKeys.add(foreignKeyBuilder.build());
      }
      builder.createTable(tableName).foreignKeys(tableForeignKeys.build()).endTable();
    }

    Map<String, NavigableMap<String, CheckConstraint>> checkConstraints = listCheckConstraints();
    for (Map.Entry<String, NavigableMap<String, CheckConstraint>> tableEntry :
        checkConstraints.entrySet()) {
      String tableName = tableEntry.getKey();
      if (builder.getTable(tableName) == null) {
        continue; // Skipping as table does not exist
      }

      ImmutableList.Builder<String> constraints = ImmutableList.builder();
      for (Map.Entry<String, CheckConstraint> entry : tableEntry.getValue().entrySet()) {
        constraints.add(entry.getValue().prettyPrint());
      }
      builder.createTable(tableName).checkConstraints(constraints.build()).endTable();
    }

    Ddl ddl = builder.build();
    LOG.info("spanner ddl: {}", ddl.prettyPrint());
    return ddl;
  }

  private void listTables(Ddl.Builder builder) {
    Statement query;
    switch (dialect) {
      case GOOGLE_STANDARD_SQL:
        query =
            Statement.of(
                "SELECT t.table_name, t.parent_table_name, t.on_delete_action, t.interleave_type"
                    + " FROM information_schema.tables AS t"
                    + " WHERE t.table_catalog = '' AND t.table_schema = ''"
                    + " AND t.table_type='BASE TABLE'");
        break;
      case POSTGRESQL:
        query =
            Statement.of(
                "SELECT t.table_name, t.parent_table_name, t.on_delete_action, t.interleave_type FROM"
                    + " information_schema.tables AS t"
                    + " WHERE t.table_schema NOT IN "
                    + "('information_schema', 'spanner_sys', 'pg_catalog')"
                    + " AND t.table_type='BASE TABLE'");
        break;
      default:
        throw new IllegalArgumentException("Unrecognized dialect: " + dialect);
    }
    ResultSet resultSet = context.executeQuery(query);
    while (resultSet.next()) {
      String tableName = resultSet.getString(0);
      String parentTableName = resultSet.isNull(1) ? null : resultSet.getString(1);
      String onDeleteAction = resultSet.isNull(2) ? null : resultSet.getString(2);
      String interleaveType = resultSet.isNull(3) ? null : resultSet.getString(3);

      // Error out when the parent table or on delete action are set incorrectly.
      if (interleaveType != null
          && interleaveType.equalsIgnoreCase("IN PARENT")
          && Strings.isNullOrEmpty(parentTableName) != Strings.isNullOrEmpty(onDeleteAction)) {
        throw new IllegalStateException(
            String.format(
                "Invalid combination of parentTableName %s and onDeleteAction %s",
                parentTableName, onDeleteAction));
      }

      boolean onDeleteCascade = false;
      if (onDeleteAction != null) {
        if (onDeleteAction.equals("CASCADE")) {
          onDeleteCascade = true;
        } else if (!onDeleteAction.equals("NO ACTION")) {
          // This is an unknown on delete action.
          throw new IllegalStateException("Unsupported on delete action " + onDeleteAction);
        }
      }
      LOG.debug(
          "Schema Table {} Parent {} OnDelete {} {}", tableName, parentTableName, onDeleteCascade);
      builder
          .createTable(tableName)
          .interleavingParent(parentTableName)
          .interleaveType(interleaveType)
          .onDeleteCascade(onDeleteCascade)
          .endTable();
    }
  }

  private void listColumns(Ddl.Builder builder) {
    Statement statement = listColumnsSQL();

    ResultSet resultSet = context.executeQuery(statement);
    while (resultSet.next()) {
      String tableName = resultSet.getString(0);
      String columnName = resultSet.getString(1);
      String spannerType = resultSet.getString(3);
      boolean nullable = resultSet.getString(4).equalsIgnoreCase("YES");
      boolean isGenerated = resultSet.getString(5).equalsIgnoreCase("ALWAYS");
      String generationExpression = resultSet.isNull(6) ? "" : resultSet.getString(6);
      boolean isStored =
          resultSet.isNull(7) ? false : resultSet.getString(7).equalsIgnoreCase("YES");
      if (builder.getTable(tableName) == null) {
        continue; // Skipping as table does not exist
      }
      builder
          .createTable(tableName)
          .column(columnName)
          .parseType(spannerType)
          .notNull(!nullable)
          .isGenerated(isGenerated)
          .generationExpression(generationExpression)
          .isStored(isStored)
          .endColumn()
          .endTable();
    }
  }

  Statement listColumnsSQL() {
    switch (dialect) {
      case GOOGLE_STANDARD_SQL:
        return Statement.of(
            "SELECT c.table_name, c.column_name,"
                + " c.ordinal_position, c.spanner_type, c.is_nullable,"
                + " c.is_generated, c.generation_expression, c.is_stored"
                + " FROM information_schema.columns as c"
                + " WHERE c.table_catalog = '' AND c.table_schema = '' "
                + " AND c.spanner_state = 'COMMITTED' "
                + " ORDER BY c.table_name, c.ordinal_position");
      case POSTGRESQL:
        return Statement.of(
            "SELECT c.table_name, c.column_name,"
                + " c.ordinal_position, c.spanner_type, c.is_nullable,"
                + " c.is_generated, c.generation_expression, c.is_stored"
                + " FROM information_schema.columns as c"
                + " WHERE c.table_schema NOT IN "
                + " ('information_schema', 'spanner_sys', 'pg_catalog') "
                + " AND c.spanner_state = 'COMMITTED' "
                + " ORDER BY c.table_name, c.ordinal_position");
      default:
        throw new IllegalArgumentException("Unrecognized dialect: " + dialect);
    }
  }

  private void listIndexes(Map<String, NavigableMap<String, Index.Builder>> indexes) {
    Statement statement = listIndexesSQL();

    ResultSet resultSet = context.executeQuery(statement);
    while (resultSet.next()) {
      String tableName = resultSet.getString(0);
      String indexName = resultSet.getString(1);
      String parent = resultSet.isNull(2) ? null : resultSet.getString(2);
      // should be NULL but is an empty string in practice.
      if (Strings.isNullOrEmpty(parent)) {
        parent = null;
      }

      boolean unique =
          (dialect == Dialect.GOOGLE_STANDARD_SQL)
              ? resultSet.getBoolean(3)
              : resultSet.getString(3).equalsIgnoreCase("YES");
      boolean nullFiltered =
          (dialect == Dialect.GOOGLE_STANDARD_SQL)
              ? resultSet.getBoolean(4)
              : resultSet.getString(4).equalsIgnoreCase("YES");
      String filter =
          (dialect == Dialect.GOOGLE_STANDARD_SQL || resultSet.isNull(5))
              ? null
              : resultSet.getString(5);

      Map<String, Index.Builder> tableIndexes =
          indexes.computeIfAbsent(tableName, k -> Maps.newTreeMap());

      tableIndexes.put(
          indexName,
          Index.builder(dialect)
              .name(indexName)
              .table(tableName)
              .unique(unique)
              .nullFiltered(nullFiltered)
              .interleaveIn(parent)
              .filter(filter));
    }
  }

  Statement listIndexesSQL() {
    switch (dialect) {
      case GOOGLE_STANDARD_SQL:
        return Statement.of(
            "SELECT t.table_name, t.index_name, t.parent_table_name, t.is_unique,"
                + " t.is_null_filtered"
                + " FROM information_schema.indexes AS t"
                + " WHERE t.table_catalog = '' AND t.table_schema = '' AND"
                + " t.index_type='INDEX' AND t.spanner_is_managed = FALSE"
                + " ORDER BY t.table_name, t.index_name");
      case POSTGRESQL:
        return Statement.of(
            "SELECT t.table_name, t.index_name, t.parent_table_name, t.is_unique,"
                + " t.is_null_filtered, t.filter FROM information_schema.indexes AS t "
                + " WHERE t.table_schema NOT IN "
                + " ('information_schema', 'spanner_sys', 'pg_catalog')"
                + " AND t.index_type='INDEX' AND t.spanner_is_managed = 'NO' "
                + " ORDER BY t.table_name, t.index_name");
      default:
        throw new IllegalArgumentException("Unrecognized dialect: " + dialect);
    }
  }

  private void listIndexColumns(
      Ddl.Builder builder, Map<String, NavigableMap<String, Index.Builder>> indexes) {
    Statement statement = listIndexColumnsSQL();

    ResultSet resultSet = context.executeQuery(statement);
    while (resultSet.next()) {
      String tableName = resultSet.getString(0);
      String columnName = resultSet.getString(1);
      String ordering = resultSet.isNull(2) ? null : resultSet.getString(2);
      String indexName = resultSet.getString(3);

      if (builder.getTable(tableName) == null) {
        continue; // Skipping as table does not exist
      }

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
        Map<String, Index.Builder> tableIndexes = indexes.get(tableName);
        if (tableIndexes == null) {
          continue;
        }
        Index.Builder indexBuilder = tableIndexes.get(indexName);
        if (indexBuilder == null) {
          continue;
        }
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

  Statement listIndexColumnsSQL() {
    switch (dialect) {
      case GOOGLE_STANDARD_SQL:
        return Statement.of(
            "SELECT t.table_name, t.column_name, t.column_ordering, t.index_name "
                + "FROM information_schema.index_columns AS t "
                + "WHERE t.table_catalog = '' AND t.table_schema = '' "
                + "ORDER BY t.table_name, t.index_name, t.ordinal_position");
      case POSTGRESQL:
        return Statement.of(
            "SELECT t.table_name, t.column_name, t.column_ordering, t.index_name "
                + "FROM information_schema.index_columns AS t "
                + "WHERE t.table_schema NOT IN "
                + "('information_schema', 'spanner_sys', 'pg_catalog') "
                + "ORDER BY t.table_name, t.index_name, t.ordinal_position");
      default:
        throw new IllegalArgumentException("Unrecognized dialect: " + dialect);
    }
  }

  private void listColumnOptions(Ddl.Builder builder) {
    Statement statement = listColumnOptionsSQL();

    ResultSet resultSet = context.executeQuery(statement);

    Map<KV<String, String>, ImmutableList.Builder<String>> allOptions = Maps.newHashMap();
    while (resultSet.next()) {
      String tableName = resultSet.getString(0);
      String columnName = resultSet.getString(1);
      String optionName = resultSet.getString(2);
      String optionType = resultSet.getString(3);
      String optionValue = resultSet.getString(4);

      if (builder.getTable(tableName) == null) {
        continue; // Skipping as table does not exist
      }

      KV<String, String> kv = KV.of(tableName, columnName);
      ImmutableList.Builder<String> options =
          allOptions.computeIfAbsent(kv, k -> ImmutableList.builder());
      if (optionType.equalsIgnoreCase("STRING")) {
        options.add(optionName + "=\"" + ESCAPER.escape(optionValue) + "\"");
      } else if (optionType.equalsIgnoreCase("character varying")) {
        options.add(optionName + "='" + ESCAPER.escape(optionValue) + "'");
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

  Statement listColumnOptionsSQL() {
    switch (dialect) {
      case GOOGLE_STANDARD_SQL:
        return Statement.of(
            "SELECT t.table_name, t.column_name, t.option_name, t.option_type,"
                + " t.option_value"
                + " FROM information_schema.column_options AS t"
                + " WHERE t.table_catalog = '' AND t.table_schema = ''"
                + " ORDER BY t.table_name, t.column_name");
      case POSTGRESQL:
        return Statement.of(
            "SELECT t.table_name, t.column_name, t.option_name, t.option_type,"
                + " t.option_value"
                + " FROM information_schema.column_options AS t"
                + " WHERE t.table_schema NOT IN "
                + " ('information_schema', 'spanner_sys', 'pg_catalog')"
                + " ORDER BY t.table_name, t.column_name");
      default:
        throw new IllegalArgumentException("Unrecognized dialect: " + dialect);
    }
  }

  private void listForeignKeys(Map<String, NavigableMap<String, ForeignKey.Builder>> foreignKeys) {
    Statement statement;

    switch (dialect) {
      case GOOGLE_STANDARD_SQL:
        statement =
            Statement.of(
                "SELECT rc.constraint_name,"
                    + " kcu1.table_name,"
                    + " kcu1.column_name,"
                    + " kcu2.table_name,"
                    + " kcu2.column_name"
                    + " FROM information_schema.referential_constraints as rc"
                    + " INNER JOIN information_schema.key_column_usage as kcu1"
                    + " ON kcu1.constraint_catalog = rc.constraint_catalog"
                    + " AND kcu1.constraint_schema = rc.constraint_schema"
                    + " AND kcu1.constraint_name = rc.constraint_name"
                    + " INNER JOIN information_schema.key_column_usage as kcu2"
                    + " ON kcu2.constraint_catalog = rc.unique_constraint_catalog"
                    + " AND kcu2.constraint_schema = rc.unique_constraint_schema"
                    + " AND kcu2.constraint_name = rc.unique_constraint_name"
                    + " AND kcu2.ordinal_position = kcu1.position_in_unique_constraint"
                    + " WHERE rc.constraint_catalog = ''"
                    + " AND rc.constraint_schema = ''"
                    + " AND kcu1.constraint_catalog = ''"
                    + " AND kcu1.constraint_schema = ''"
                    + " AND kcu2.constraint_catalog = ''"
                    + " AND kcu2.constraint_schema = ''"
                    + " ORDER BY rc.constraint_name, kcu1.ordinal_position;");
        break;
      case POSTGRESQL:
        statement =
            Statement.of(
                "SELECT rc.constraint_name,"
                    + " kcu1.table_name,"
                    + " kcu1.column_name,"
                    + " kcu2.table_name,"
                    + " kcu2.column_name"
                    + " FROM information_schema.referential_constraints as rc"
                    + " INNER JOIN information_schema.key_column_usage as kcu1"
                    + " ON kcu1.constraint_catalog = rc.constraint_catalog"
                    + " AND kcu1.constraint_schema = rc.constraint_schema"
                    + " AND kcu1.constraint_name = rc.constraint_name"
                    + " INNER JOIN information_schema.key_column_usage as kcu2"
                    + " ON kcu2.constraint_catalog = rc.unique_constraint_catalog"
                    + " AND kcu2.constraint_schema = rc.unique_constraint_schema"
                    + " AND kcu2.constraint_name = rc.unique_constraint_name"
                    + " AND kcu2.ordinal_position = kcu1.position_in_unique_constraint"
                    + " WHERE rc.constraint_catalog = kcu1.constraint_catalog"
                    + " AND rc.constraint_catalog = kcu2.constraint_catalog"
                    + " AND rc.constraint_schema NOT IN "
                    + " ('information_schema', 'spanner_sys', 'pg_catalog')"
                    + " AND rc.constraint_schema = kcu1.constraint_schema"
                    + " AND rc.constraint_schema = kcu2.constraint_schema"
                    + " ORDER BY rc.constraint_name, kcu1.ordinal_position;");
        break;
      default:
        throw new IllegalArgumentException("Unrecognized dialect: " + dialect);
    }

    ResultSet resultSet = context.executeQuery(statement);
    while (resultSet.next()) {
      String name = resultSet.getString(0);
      String table = resultSet.getString(1);
      String column = resultSet.getString(2);
      String referencedTable = resultSet.getString(3);
      String referencedColumn = resultSet.getString(4);
      Map<String, ForeignKey.Builder> tableForeignKeys =
          foreignKeys.computeIfAbsent(table, k -> Maps.newTreeMap());
      ForeignKey.Builder foreignKey =
          tableForeignKeys.computeIfAbsent(
              name,
              k ->
                  ForeignKey.builder(dialect)
                      .name(name)
                      .table(table)
                      .referencedTable(referencedTable));
      foreignKey.columnsBuilder().add(column);
      foreignKey.referencedColumnsBuilder().add(referencedColumn);
    }
  }

  private Map<String, NavigableMap<String, CheckConstraint>> listCheckConstraints() {
    Map<String, NavigableMap<String, CheckConstraint>> checkConstraints = Maps.newHashMap();
    Statement statement;

    switch (dialect) {
      case GOOGLE_STANDARD_SQL:
        statement =
            Statement.of(
                "SELECT ctu.TABLE_NAME,"
                    + " cc.CONSTRAINT_NAME,"
                    + " cc.CHECK_CLAUSE"
                    + " FROM INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE as ctu"
                    + " INNER JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS as cc"
                    + " ON ctu.constraint_catalog = cc.constraint_catalog"
                    + " AND ctu.constraint_schema = cc.constraint_schema"
                    + " AND ctu.CONSTRAINT_NAME = cc.CONSTRAINT_NAME"
                    + " WHERE NOT STARTS_WITH(cc.CONSTRAINT_NAME, 'CK_IS_NOT_NULL_')"
                    + " AND ctu.table_catalog = ''"
                    + " AND ctu.table_schema = ''"
                    + " AND ctu.constraint_catalog = ''"
                    + " AND ctu.constraint_schema = ''"
                    + " AND cc.SPANNER_STATE = 'COMMITTED';");
        break;
      case POSTGRESQL:
        statement =
            Statement.of(
                "SELECT ctu.TABLE_NAME,"
                    + " cc.CONSTRAINT_NAME,"
                    + " cc.CHECK_CLAUSE"
                    + " FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS as ctu"
                    + " INNER JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS as cc"
                    + " ON ctu.constraint_catalog = cc.constraint_catalog"
                    + " AND ctu.constraint_schema = cc.constraint_schema"
                    + " AND ctu.CONSTRAINT_NAME = cc.CONSTRAINT_NAME"
                    + " WHERE NOT STARTS_WITH(cc.CONSTRAINT_NAME, 'CK_IS_NOT_NULL_')"
                    + " AND ctu.table_catalog = ctu.constraint_catalog"
                    + " AND ctu.table_schema NOT IN"
                    + "('information_schema', 'spanner_sys', 'pg_catalog')"
                    + " AND ctu.table_schema = ctu.constraint_schema"
                    + " AND cc.SPANNER_STATE = 'COMMITTED';");
        break;
      default:
        throw new IllegalArgumentException("Unrecognized dialect: " + dialect);
    }

    ResultSet resultSet = context.executeQuery(statement);
    while (resultSet.next()) {
      String table = resultSet.getString(0);
      String name = resultSet.getString(1);
      String expression = resultSet.getString(2);
      Map<String, CheckConstraint> tableCheckConstraints =
          checkConstraints.computeIfAbsent(table, k -> Maps.newTreeMap());
      tableCheckConstraints.computeIfAbsent(
          name, k -> CheckConstraint.builder(dialect).name(name).expression(expression).build());
    }
    return checkConstraints;
  }
}
