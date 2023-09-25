/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.spanner.ddl;

import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.teleport.spanner.ddl.ForeignKey.ReferentialAction;
import com.google.cloud.teleport.spanner.proto.ExportProtos.Export;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.stream.Collectors;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Scans INFORMATION_SCHEMA.* tables and build {@link Ddl}. */
public class InformationSchemaScanner {

  private static final Logger LOG = LoggerFactory.getLogger(InformationSchemaScanner.class);

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
    listDatabaseOptions(builder);
    listTables(builder);
    listViews(builder);
    listColumns(builder);
    listColumnOptions(builder);
    if (isModelSupported()) {
      listModels(builder);
      listModelOptions(builder);
      listModelColumns(builder);
      listModelColumnOptions(builder);
    }
    if (isChangeStreamsSupported()) {
      listChangeStreams(builder);
      listChangeStreamOptions(builder);
    }
    if (isSequenceSupported()) {
      Map<String, Long> currentCounters = Maps.newHashMap();
      listSequences(builder, currentCounters);
      if (dialect == Dialect.GOOGLE_STANDARD_SQL) {
        listSequenceOptionsGoogleSQL(builder, currentCounters);
      } else {
        listSequenceOptionsPostgreSQL(builder, currentCounters);
      }
    }
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

    Map<String, NavigableMap<String, ForeignKey.Builder>> foreignKeys = Maps.newHashMap();
    listForeignKeys(foreignKeys);

    for (Map.Entry<String, NavigableMap<String, ForeignKey.Builder>> tableEntry :
        foreignKeys.entrySet()) {
      String tableName = tableEntry.getKey();
      ImmutableList.Builder<String> tableForeignKeys = ImmutableList.builder();
      for (Map.Entry<String, ForeignKey.Builder> entry : tableEntry.getValue().entrySet()) {
        ForeignKey.Builder foreignKeyBuilder = entry.getValue();
        ForeignKey fkBuilder = foreignKeyBuilder.build();
        // Add the table and referenced table to the referencedTables TreeMultiMap of the ddl
        builder.addReferencedTable(fkBuilder.table(), fkBuilder.referencedTable());
        tableForeignKeys.add(fkBuilder.prettyPrint());
      }
      builder.createTable(tableName).foreignKeys(tableForeignKeys.build()).endTable();
    }

    Map<String, NavigableMap<String, CheckConstraint>> checkConstraints = listCheckConstraints();
    for (Map.Entry<String, NavigableMap<String, CheckConstraint>> tableEntry :
        checkConstraints.entrySet()) {
      String tableName = tableEntry.getKey();
      ImmutableList.Builder<String> constraints = ImmutableList.builder();
      for (Map.Entry<String, CheckConstraint> entry : tableEntry.getValue().entrySet()) {
        constraints.add(entry.getValue().prettyPrint());
      }
      builder.createTable(tableName).checkConstraints(constraints.build()).endTable();
    }

    return builder.build();
  }

  private void listDatabaseOptions(Ddl.Builder builder) {
    Statement statement = databaseOptionsSQL();

    ResultSet resultSet = context.executeQuery(statement);

    ImmutableList.Builder<Export.DatabaseOption> options = ImmutableList.builder();
    while (resultSet.next()) {
      String optionName = resultSet.getString(0);
      String optionType = resultSet.getString(1);
      String optionValue = resultSet.getString(2);
      if (!DatabaseOptionAllowlist.DATABASE_OPTION_ALLOWLIST.contains(optionName)) {
        continue;
      }
      options.add(
          Export.DatabaseOption.newBuilder()
              .setOptionName(optionName)
              .setOptionType(optionType)
              .setOptionValue(optionValue)
              .build());
    }
    builder.mergeDatabaseOptions(options.build());
  }

  @VisibleForTesting
  Statement databaseOptionsSQL() {
    switch (dialect) {
      case GOOGLE_STANDARD_SQL:
        return Statement.of(
            "SELECT t.option_name, t.option_type, t.option_value "
                + " FROM information_schema.database_options AS t "
                + " WHERE t.catalog_name = '' AND t.schema_name = ''");
      case POSTGRESQL:
        return Statement.of(
            "SELECT t.option_name, t.option_type, t.option_value "
                + " FROM information_schema.database_options AS t "
                + " WHERE t.schema_name NOT IN "
                + "('information_schema', 'spanner_sys', 'pg_catalog')");
      default:
        throw new IllegalArgumentException("Unrecognized dialect: " + dialect);
    }
  }

  private void listTables(Ddl.Builder builder) {
    Statement.Builder queryBuilder;

    Statement preconditionStatement;

    switch (dialect) {
      case GOOGLE_STANDARD_SQL:
        queryBuilder =
            Statement.newBuilder(
                "SELECT t.table_name, t.parent_table_name, t.on_delete_action FROM"
                    + " information_schema.tables AS t"
                    + " WHERE t.table_catalog = '' AND t.table_schema = ''");
        preconditionStatement =
            Statement.of(
                "SELECT COUNT(1) FROM INFORMATION_SCHEMA.COLUMNS c WHERE c.TABLE_CATALOG = '' AND"
                    + " c.TABLE_SCHEMA = 'INFORMATION_SCHEMA' AND c.TABLE_NAME = 'TABLES' AND"
                    + " c.COLUMN_NAME = 'TABLE_TYPE';");
        break;
      case POSTGRESQL:
        queryBuilder =
            Statement.newBuilder(
                "SELECT t.table_name, t.parent_table_name, t.on_delete_action FROM"
                    + " information_schema.tables AS t"
                    + " WHERE t.table_schema NOT IN "
                    + "('information_schema', 'spanner_sys', 'pg_catalog')");
        preconditionStatement =
            Statement.of(
                "SELECT COUNT(1) FROM INFORMATION_SCHEMA.COLUMNS c WHERE "
                    + " c.TABLE_SCHEMA = 'information_schema' AND c.TABLE_NAME = 'tables' AND"
                    + " c.COLUMN_NAME = 'table_type';");
        break;
      default:
        throw new IllegalArgumentException("Unrecognized dialect: " + dialect);
    }

    try (ResultSet resultSet = context.executeQuery(preconditionStatement)) {
      // Returns a single row with a 1 if views are supported and a 0 if not.
      resultSet.next();
      if (resultSet.getLong(0) == 0) {
        LOG.info("INFORMATION_SCHEMA.TABLES.TABLE_TYPE is not present; assuming no views");
      } else {
        queryBuilder.append(" AND t.table_type != 'VIEW'");
      }
    }

    ResultSet resultSet = context.executeQuery(queryBuilder.build());
    while (resultSet.next()) {
      String tableName = resultSet.getString(0);
      String parentTableName = resultSet.isNull(1) ? null : resultSet.getString(1);
      String onDeleteAction = resultSet.isNull(2) ? null : resultSet.getString(2);

      // Error out when the parent table or on delete action are set incorrectly.
      if (Strings.isNullOrEmpty(parentTableName) != Strings.isNullOrEmpty(onDeleteAction)) {
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
          .interleaveInParent(parentTableName)
          .onDeleteCascade(onDeleteCascade)
          .endTable();
    }
  }

  private void listColumns(Ddl.Builder builder) {
    Statement statement = listColumnsSQL();

    ResultSet resultSet = context.executeQuery(statement);
    while (resultSet.next()) {
      String tableName = resultSet.getString(0);
      if (builder.hasView(tableName)) {
        // We do not need to collect columns from view definitions, and we will create phantom
        // tables with names that collide with views if we try.
        continue;
      }
      String columnName = resultSet.getString(1);
      String spannerType = resultSet.getString(3);
      boolean nullable = resultSet.getString(4).equalsIgnoreCase("YES");
      boolean isGenerated = resultSet.getString(5).equalsIgnoreCase("ALWAYS");
      String generationExpression = resultSet.isNull(6) ? "" : resultSet.getString(6);
      boolean isStored =
          resultSet.isNull(7) ? false : resultSet.getString(7).equalsIgnoreCase("YES");
      String defaultExpression = resultSet.isNull(8) ? null : resultSet.getString(8);
      builder
          .createTable(tableName)
          .column(columnName)
          .parseType(spannerType)
          .notNull(!nullable)
          .isGenerated(isGenerated)
          .generationExpression(generationExpression)
          .isStored(isStored)
          .defaultExpression(defaultExpression)
          .endColumn()
          .endTable();
    }
  }

  @VisibleForTesting
  Statement listColumnsSQL() {
    switch (dialect) {
      case GOOGLE_STANDARD_SQL:
        return Statement.of(
            "SELECT c.table_name, c.column_name,"
                + " c.ordinal_position, c.spanner_type, c.is_nullable,"
                + " c.is_generated, c.generation_expression, c.is_stored, c.column_default"
                + " FROM information_schema.columns as c"
                + " WHERE c.table_catalog = '' AND c.table_schema = '' "
                + " AND c.spanner_state = 'COMMITTED' "
                + " ORDER BY c.table_name, c.ordinal_position");
      case POSTGRESQL:
        return Statement.of(
            "SELECT c.table_name, c.column_name,"
                + " c.ordinal_position, c.spanner_type, c.is_nullable,"
                + " c.is_generated, c.generation_expression, c.is_stored, c.column_default"
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

  @VisibleForTesting
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
        IndexColumn.IndexColumnsBuilder<Index.Builder> indexColumnsBuilder =
            indexBuilder.columns().create().name(columnName);
        if (ordering == null) {
          indexColumnsBuilder.storing();
        } else {
          ordering = ordering.toUpperCase();
          if (ordering.startsWith("ASC")) {
            indexColumnsBuilder.asc();
          }
          if (ordering.startsWith("DESC")) {
            indexColumnsBuilder.desc();
          }
          if (ordering.endsWith("NULLS FIRST")) {
            indexColumnsBuilder.nullsFirst();
          }
          if (ordering.endsWith("NULLS LAST")) {
            indexColumnsBuilder.nullsLast();
          }
        }
        indexColumnsBuilder.endIndexColumn().end();
      }
    }
  }

  @VisibleForTesting
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

      KV<String, String> kv = KV.of(tableName, columnName);
      ImmutableList.Builder<String> options =
          allOptions.computeIfAbsent(kv, k -> ImmutableList.builder());

      if (optionType.equalsIgnoreCase("STRING")) {
        options.add(
            optionName
                + "=\""
                + DdlUtilityComponents.OPTION_STRING_ESCAPER.escape(optionValue)
                + "\"");
      } else if (optionType.equalsIgnoreCase("character varying")) {
        options.add(
            optionName
                + "='"
                + DdlUtilityComponents.OPTION_STRING_ESCAPER.escape(optionValue)
                + "'");
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

  @VisibleForTesting
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
        // Ignore the 'allow_commit_timestamp' option since it's not user-settable in POSTGRESQL.
        return Statement.of(
            "SELECT t.table_name, t.column_name, t.option_name, t.option_type,"
                + " t.option_value"
                + " FROM information_schema.column_options AS t"
                + " WHERE t.table_schema NOT IN "
                + " ('information_schema', 'spanner_sys', 'pg_catalog')"
                + " AND t.option_name NOT IN ('allow_commit_timestamp')"
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
                    + " kcu2.column_name,"
                    + " rc.delete_rule"
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
                    + " kcu2.column_name,"
                    + " rc.delete_rule"
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
      String deleteRule = resultSet.getString(5);
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
      if (!isNullOrEmpty(deleteRule)) {
        foreignKey.referentialAction(
            Optional.of(ReferentialAction.getReferentialAction("DELETE", deleteRule)));
      }
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

  private void listViews(Ddl.Builder builder) {
    Statement queryStatement;
    Statement preconditionStatement;

    switch (dialect) {
      case GOOGLE_STANDARD_SQL:
        queryStatement =
            Statement.of(
                "SELECT v.table_name, v.view_definition, v.security_type"
                    + " FROM information_schema.views AS v"
                    + " WHERE v.table_catalog = '' AND v.table_schema = ''");
        preconditionStatement =
            Statement.of(
                "SELECT COUNT(1)"
                    + " FROM INFORMATION_SCHEMA.TABLES t WHERE t.TABLE_CATALOG = '' AND"
                    + " t.TABLE_SCHEMA = 'INFORMATION_SCHEMA'"
                    + " AND t.TABLE_NAME = 'VIEWS'");
        break;
      case POSTGRESQL:
        queryStatement =
            Statement.of(
                "SELECT v.table_name, v.view_definition, v.security_type"
                    + " FROM information_schema.views AS v"
                    + " WHERE v.table_schema NOT IN"
                    + " ('information_schema', 'spanner_sys', 'pg_catalog')");
        preconditionStatement =
            Statement.of(
                "SELECT COUNT(1)"
                    + " FROM INFORMATION_SCHEMA.TABLES t WHERE "
                    + " t.TABLE_SCHEMA = 'information_schema'"
                    + " AND t.TABLE_NAME = 'views'");
        break;
      default:
        throw new IllegalArgumentException("Unrecognized dialect: " + dialect);
    }
    try (ResultSet resultSet = context.executeQuery(preconditionStatement)) {
      // Returns a single row with a 1 if views are supported and a 0 if not.
      resultSet.next();
      if (resultSet.getLong(0) == 0) {
        LOG.info("INFORMATION_SCHEMA.VIEWS is not present; assuming no views");
        return;
      }
    }

    ResultSet resultSet = context.executeQuery(queryStatement);

    while (resultSet.next()) {
      String viewName = resultSet.getString(0);
      String viewQuery = resultSet.getString(1);
      String viewSecurityType = resultSet.getString(2);
      LOG.debug("Schema View {}", viewName);
      builder
          .createView(viewName)
          .query(viewQuery)
          .security(View.SqlSecurity.valueOf(viewSecurityType))
          .endView();
    }
  }

  // TODO: Remove after models are supported in POSTGRESQL.
  private boolean isModelSupported() {
    return dialect == Dialect.GOOGLE_STANDARD_SQL;
  }

  private void listModels(Ddl.Builder builder) {
    ResultSet resultSet =
        context.executeQuery(
            Statement.of(
                "SELECT t.model_name, t.is_remote "
                    + " FROM information_schema.models AS t"
                    + " WHERE t.model_catalog = '' AND t.model_schema = ''"));
    while (resultSet.next()) {
      String modelName = resultSet.getString(0);
      boolean remote = resultSet.isNull(1) ? false : resultSet.getBoolean(1);
      LOG.debug("Schema Model {} Remote {} {}", modelName, remote);
      builder.createModel(modelName).remote(remote).endModel();
    }
  }

  private void listModelOptions(Ddl.Builder builder) {
    ResultSet resultSet =
        context.executeQuery(
            Statement.of(
                "SELECT t.model_name, t.option_name, t.option_type, t.option_value "
                    + " FROM information_schema.model_options AS t"
                    + " WHERE t.model_catalog = '' AND t.model_schema = ''"
                    + " ORDER BY t.model_name, t.option_name"));

    Map<String, ImmutableList.Builder<String>> allOptions = Maps.newHashMap();
    while (resultSet.next()) {
      String modelName = resultSet.getString(0);
      String optionName = resultSet.getString(1);
      String optionType = resultSet.getString(2);
      String optionValue = resultSet.getString(3);

      ImmutableList.Builder<String> options =
          allOptions.computeIfAbsent(modelName, k -> ImmutableList.builder());

      if (optionType.equalsIgnoreCase("STRING")) {
        options.add(
            optionName
                + "="
                + DdlUtilityComponents.GSQL_LITERAL_QUOTE
                + DdlUtilityComponents.OPTION_STRING_ESCAPER.escape(optionValue)
                + DdlUtilityComponents.GSQL_LITERAL_QUOTE);
      } else if (optionType.equalsIgnoreCase("character varying")) {
        options.add(
            optionName
                + "="
                + DdlUtilityComponents.POSTGRESQL_LITERAL_QUOTE
                + DdlUtilityComponents.OPTION_STRING_ESCAPER.escape(optionValue)
                + DdlUtilityComponents.POSTGRESQL_LITERAL_QUOTE);
      } else {
        options.add(optionName + "=" + optionValue);
      }
    }

    for (Map.Entry<String, ImmutableList.Builder<String>> entry : allOptions.entrySet()) {
      String modelName = entry.getKey();
      ImmutableList<String> options = entry.getValue().build();
      builder.createModel(modelName).options(options).endModel();
    }
  }

  private void listModelColumns(Ddl.Builder builder) {
    ResultSet resultSet =
        context.executeQuery(
            Statement.of(
                "SELECT t.model_name, t.column_kind, t.ordinal_position, t.column_name,"
                    + " t.data_type FROM information_schema.model_columns as t"
                    + " WHERE t.model_catalog = '' AND t.model_schema = ''"
                    + " ORDER BY t.model_name, t.column_kind, t.ordinal_position"));

    while (resultSet.next()) {
      String modelName = resultSet.getString(0);
      String columnKind = resultSet.getString(1);
      String columnName = resultSet.getString(3);
      String spannerType = resultSet.getString(4);
      if (columnKind.equalsIgnoreCase("INPUT")) {
        builder
            .createModel(modelName)
            .inputColumn(columnName)
            .parseType(spannerType)
            .endInputColumn()
            .endModel();
      } else if (columnKind.equalsIgnoreCase("OUTPUT")) {
        builder
            .createModel(modelName)
            .outputColumn(columnName)
            .parseType(spannerType)
            .endOutputColumn()
            .endModel();
      } else {
        throw new IllegalArgumentException("Unrecognized model column kind: " + columnKind);
      }
    }
  }

  private void listModelColumnOptions(Ddl.Builder builder) {
    ResultSet resultSet =
        context.executeQuery(
            Statement.of(
                "SELECT t.model_name, t.column_kind, t.column_name, t.option_name, t.option_type,"
                    + " t.option_value FROM information_schema.model_column_options as t WHERE"
                    + " t.model_catalog = '' AND t.model_schema = '' ORDER BY t.model_name,"
                    + " t.column_kind, t.column_name"));

    Map<KV<String, String>, ImmutableList.Builder<String>> inputOptions = Maps.newHashMap();
    Map<KV<String, String>, ImmutableList.Builder<String>> outputOptions = Maps.newHashMap();

    while (resultSet.next()) {
      String modelName = resultSet.getString(0);
      String columnKind = resultSet.getString(1);
      String columnName = resultSet.getString(2);
      String optionName = resultSet.getString(3);
      String optionType = resultSet.getString(4);
      String optionValue = resultSet.getString(5);

      KV<String, String> kv = KV.of(modelName, columnName);
      ImmutableList.Builder<String> options;
      if (columnKind.equals("INPUT")) {
        options = inputOptions.computeIfAbsent(kv, k -> ImmutableList.builder());
      } else if (columnKind.equals("OUTPUT")) {
        options = outputOptions.computeIfAbsent(kv, k -> ImmutableList.builder());
      } else {
        throw new IllegalArgumentException("Unrecognized model column kind: " + columnKind);
      }

      if (optionType.equalsIgnoreCase("STRING")) {
        options.add(
            optionName
                + "="
                + DdlUtilityComponents.GSQL_LITERAL_QUOTE
                + DdlUtilityComponents.OPTION_STRING_ESCAPER.escape(optionValue)
                + DdlUtilityComponents.GSQL_LITERAL_QUOTE);
      } else if (optionType.equalsIgnoreCase("character varying")) {
        options.add(
            optionName
                + "="
                + DdlUtilityComponents.POSTGRESQL_LITERAL_QUOTE
                + DdlUtilityComponents.OPTION_STRING_ESCAPER.escape(optionValue)
                + DdlUtilityComponents.POSTGRESQL_LITERAL_QUOTE);
      } else {
        options.add(optionName + "=" + optionValue);
      }
    }

    for (Map.Entry<KV<String, String>, ImmutableList.Builder<String>> entry :
        inputOptions.entrySet()) {
      String modelName = entry.getKey().getKey();
      String columnName = entry.getKey().getValue();
      ImmutableList<String> options = entry.getValue().build();
      builder
          .createModel(modelName)
          .inputColumn(columnName)
          .columnOptions(options)
          .endInputColumn()
          .endModel();
    }

    for (Map.Entry<KV<String, String>, ImmutableList.Builder<String>> entry :
        outputOptions.entrySet()) {
      String modelName = entry.getKey().getKey();
      String columnName = entry.getKey().getValue();
      ImmutableList<String> options = entry.getValue().build();
      builder
          .createModel(modelName)
          .outputColumn(columnName)
          .columnOptions(options)
          .endOutputColumn()
          .endModel();
    }
  }

  // TODO: Remove after change streams are supported in POSTGRESQL.
  private boolean isChangeStreamsSupported() {
    if (dialect == Dialect.GOOGLE_STANDARD_SQL) {
      return true;
    }

    Statement statement =
        Statement.of(
            "SELECT COUNT(1)"
                + " FROM INFORMATION_SCHEMA.TABLES t WHERE "
                + " t.TABLE_SCHEMA = 'information_schema'"
                + " AND t.TABLE_NAME = 'change_streams'");

    try (ResultSet resultSet = context.executeQuery(statement)) {
      // Returns a single row with a 1 if change streams are supported and a 0 if not.
      resultSet.next();
      if (resultSet.getLong(0) == 0) {
        LOG.info("information_schema.change_streams is not present");
        return false;
      }
    }
    return true;
  }

  private void listChangeStreams(Ddl.Builder builder) {
    String identifierQuote = DdlUtilityComponents.identifierQuote(dialect);
    ResultSet resultSet =
        context.executeQuery(
            Statement.of(
                "SELECT cs.change_stream_name,"
                    + " cs.all,"
                    + " cst.table_name,"
                    + " cst.all_columns,"
                    + " ARRAY_AGG(csc.column_name) AS column_name_list"
                    + " FROM information_schema.change_streams AS cs"
                    + " LEFT JOIN information_schema.change_stream_tables AS cst"
                    + " ON cs.change_stream_catalog = cst.change_stream_catalog"
                    + " AND cs.change_stream_schema = cst.change_stream_schema"
                    + " AND cs.change_stream_name = cst.change_stream_name"
                    + " LEFT JOIN information_schema.change_stream_columns AS csc"
                    + " ON cst.change_stream_catalog = csc.change_stream_catalog"
                    + " AND cst.change_stream_schema = csc.change_stream_schema"
                    + " AND cst.change_stream_name = csc.change_stream_name"
                    + " AND cst.table_catalog = csc.table_catalog"
                    + " AND cst.table_schema = csc.table_schema"
                    + " AND cst.table_name = csc.table_name"
                    + " GROUP BY cs.change_stream_name, cs.all, cst.table_name, cst.all_columns"
                    + " ORDER BY cs.change_stream_name, cs.all, cst.table_name"));

    Map<String, StringBuilder> allChangeStreams = Maps.newHashMap();
    while (resultSet.next()) {
      String changeStreamName = resultSet.getString(0);
      boolean all =
          (dialect == Dialect.GOOGLE_STANDARD_SQL)
              ? resultSet.getBoolean(1)
              : resultSet.getString(1).equalsIgnoreCase("YES");
      String tableName = resultSet.isNull(2) ? null : resultSet.getString(2);
      Boolean allColumns =
          resultSet.isNull(3)
              ? null
              : (dialect == Dialect.GOOGLE_STANDARD_SQL
                  ? resultSet.getBoolean(3)
                  : resultSet.getString(3).equalsIgnoreCase("YES"));
      List<String> columnNameList = resultSet.isNull(4) ? null : resultSet.getStringList(4);

      StringBuilder forClause =
          allChangeStreams.computeIfAbsent(changeStreamName, k -> new StringBuilder());
      if (all) {
        forClause.append("FOR ALL");
        continue;
      } else if (tableName == null) {
        // The change stream does not track any table/column, i.e., it does not have a for-clause.
        continue;
      }

      forClause.append(forClause.length() == 0 ? "FOR " : ", ");
      forClause.append(identifierQuote).append(tableName).append(identifierQuote);
      if (allColumns) {
        continue;
      } else if (columnNameList == null) {
        forClause.append("()");
      } else {
        String sortedColumns =
            columnNameList.stream()
                .filter(s -> s != null)
                .sorted()
                .map(s -> identifierQuote + s + identifierQuote)
                .collect(Collectors.joining(", "));
        forClause.append("(").append(sortedColumns).append(")");
      }
    }

    for (Map.Entry<String, StringBuilder> entry : allChangeStreams.entrySet()) {
      String changeStreamName = entry.getKey();
      StringBuilder forClause = entry.getValue();
      builder
          .createChangeStream(changeStreamName)
          .forClause(forClause.toString())
          .endChangeStream();
    }
  }

  private void listChangeStreamOptions(Ddl.Builder builder) {
    ResultSet resultSet =
        context.executeQuery(
            Statement.of(
                "SELECT t.change_stream_name, t.option_name, t.option_type, t.option_value"
                    + " FROM information_schema.change_stream_options AS t"
                    + " ORDER BY t.change_stream_name, t.option_name"));

    Map<String, ImmutableList.Builder<String>> allOptions = Maps.newHashMap();
    while (resultSet.next()) {
      String changeStreamName = resultSet.getString(0);
      String optionName = resultSet.getString(1);
      String optionType = resultSet.getString(2);
      String optionValue = resultSet.getString(3);

      ImmutableList.Builder<String> options =
          allOptions.computeIfAbsent(changeStreamName, k -> ImmutableList.builder());
      if (optionType.equalsIgnoreCase("STRING")) {
        options.add(
            optionName
                + "="
                + DdlUtilityComponents.GSQL_LITERAL_QUOTE
                + DdlUtilityComponents.OPTION_STRING_ESCAPER.escape(optionValue)
                + DdlUtilityComponents.GSQL_LITERAL_QUOTE);
      } else if (optionType.equalsIgnoreCase("character varying")) {
        options.add(
            optionName
                + "="
                + DdlUtilityComponents.POSTGRESQL_LITERAL_QUOTE
                + DdlUtilityComponents.OPTION_STRING_ESCAPER.escape(optionValue)
                + DdlUtilityComponents.POSTGRESQL_LITERAL_QUOTE);
      } else {
        options.add(optionName + "=" + optionValue);
      }
    }

    for (Map.Entry<String, ImmutableList.Builder<String>> entry : allOptions.entrySet()) {
      String changeStreamName = entry.getKey();
      ImmutableList<String> options = entry.getValue().build();
      builder.createChangeStream(changeStreamName).options(options).endChangeStream();
    }
  }

  private boolean isSequenceSupported() {
    Statement statement;
    switch (dialect) {
      case GOOGLE_STANDARD_SQL:
        statement =
            Statement.of(
                "SELECT COUNT(1)"
                    + " FROM INFORMATION_SCHEMA.TABLES t WHERE "
                    + " t.TABLE_SCHEMA = 'INFORMATION_SCHEMA'"
                    + " AND t.TABLE_NAME = 'SEQUENCES'");
        break;
      case POSTGRESQL:
        statement =
            Statement.of(
                "SELECT COUNT(1)"
                    + " FROM INFORMATION_SCHEMA.TABLES t WHERE "
                    + " t.TABLE_SCHEMA = 'information_schema'"
                    + " AND t.TABLE_NAME = 'sequences'");
        break;
      default:
        throw new IllegalArgumentException("Unrecognized dialect: " + dialect);
    }

    try (ResultSet resultSet = context.executeQuery(statement)) {
      // Returns a single row with a 1 if sequences are supported and a 0 if not.
      resultSet.next();
      if (resultSet.getLong(0) == 0) {
        LOG.info("information_schema.sequences is not present");
        return false;
      }
    }
    return true;
  }

  private void listSequences(Ddl.Builder builder, Map<String, Long> currentCounters) {
    Statement queryStatement;
    switch (dialect) {
      case GOOGLE_STANDARD_SQL:
        queryStatement =
            Statement.of("SELECT s.name, s.data_type FROM information_schema.sequences AS s");
        break;
      case POSTGRESQL:
        queryStatement =
            Statement.of(
                "SELECT s.sequence_name, s.data_type FROM information_schema.sequences AS s");
        break;
      default:
        throw new IllegalArgumentException("Unrecognized dialect: " + dialect);
    }

    ResultSet resultSet = context.executeQuery(queryStatement);
    while (resultSet.next()) {
      String sequenceName = resultSet.getString(0);
      builder.createSequence(sequenceName).endSequence();

      Statement sequenceCounterStatement;
      switch (dialect) {
        case GOOGLE_STANDARD_SQL:
          sequenceCounterStatement =
              Statement.of("SELECT GET_INTERNAL_SEQUENCE_STATE(SEQUENCE " + sequenceName + ")");
          break;
        case POSTGRESQL:
          sequenceCounterStatement =
              Statement.of("SELECT spanner.GET_INTERNAL_SEQUENCE_STATE('" + sequenceName + "')");
          break;
        default:
          throw new IllegalArgumentException("Unrecognized dialect: " + dialect);
      }

      ResultSet resultSetForCounter = context.executeQuery(sequenceCounterStatement);
      if (resultSetForCounter.next() && !resultSetForCounter.isNull(0)) {
        Long counterValue = resultSetForCounter.getLong(0);
        currentCounters.put(sequenceName, counterValue);
      }
    }
  }

  private void listSequenceOptionsGoogleSQL(
      Ddl.Builder builder, Map<String, Long> currentCounters) {
    if (dialect != Dialect.GOOGLE_STANDARD_SQL) {
      throw new IllegalArgumentException("Unrecognized dialect: " + dialect);
    }
    ResultSet resultSet =
        context.executeQuery(
            Statement.of(
                "SELECT t.name, t.option_name, t.option_type, t.option_value"
                    + " FROM information_schema.sequence_options AS t"
                    + " ORDER BY t.name, t.option_name"));

    Map<String, ImmutableList.Builder<String>> allOptions = Maps.newHashMap();
    while (resultSet.next()) {
      String sequenceName = resultSet.getString(0);
      String optionName = resultSet.getString(1);
      String optionType = resultSet.getString(2);
      String optionValue = resultSet.getString(3);

      if (optionName == Sequence.SEQUENCE_START_WITH_COUNTER
          && currentCounters.containsKey(sequenceName)) {
        // The sequence is in use, we need to apply the current counter to
        // the DDL builder, instead of the one retrieved from Information Schema.
        continue;
      }
      ImmutableList.Builder<String> options =
          allOptions.computeIfAbsent(sequenceName, k -> ImmutableList.builder());
      if (optionType.equalsIgnoreCase("STRING")) {
        options.add(
            optionName
                + "="
                + DdlUtilityComponents.GSQL_LITERAL_QUOTE
                + DdlUtilityComponents.OPTION_STRING_ESCAPER.escape(optionValue)
                + DdlUtilityComponents.GSQL_LITERAL_QUOTE);
      } else {
        options.add(optionName + "=" + optionValue);
      }
    }

    // Inject the current counter value to sequences that are in use.
    for (Map.Entry<String, Long> entry : currentCounters.entrySet()) {
      ImmutableList.Builder<String> options =
          allOptions.computeIfAbsent(entry.getKey(), k -> ImmutableList.builder());
      // Add a buffer to accommodate writes that may happen after import
      // is run. Note that this is not 100% failproof, since more writes may
      // happen and they will make the sequence advances past the buffer.
      Long newCounterStartValue = entry.getValue() + Sequence.SEQUENCE_COUNTER_BUFFER;
      options.add(
          Sequence.SEQUENCE_START_WITH_COUNTER + "=" + String.valueOf(newCounterStartValue));
      LOG.info(
          "Sequence "
              + entry.getKey()
              + "'s current counter is updated to "
              + newCounterStartValue);
    }

    for (Map.Entry<String, ImmutableList.Builder<String>> entry : allOptions.entrySet()) {
      String sequenceName = entry.getKey();
      ImmutableList<String> options = entry.getValue().build();
      builder.createSequence(sequenceName).options(options).endSequence();
    }
  }

  private void listSequenceOptionsPostgreSQL(
      Ddl.Builder builder, Map<String, Long> currentCounters) {
    if (dialect != Dialect.POSTGRESQL) {
      throw new IllegalArgumentException("Unrecognized dialect: " + dialect);
    }

    ResultSet resultSet =
        context.executeQuery(
            Statement.of(
                "SELECT t.sequence_name, t.sequence_kind, t.counter_start_value, "
                    + " t.skip_range_min, t.skip_range_max"
                    + " FROM information_schema.sequences AS t"
                    + " ORDER BY t.sequence_name"));

    Map<String, ImmutableList.Builder<String>> allOptions = Maps.newHashMap();
    while (resultSet.next()) {
      String sequenceName = resultSet.getString(0);
      String sequenceKind = resultSet.isNull(1) ? null : resultSet.getString(1);
      Long counterStartValue = resultSet.isNull(2) ? null : resultSet.getLong(2);
      Long skipRangeMin = resultSet.isNull(3) ? null : resultSet.getLong(3);
      Long skipRangeMax = resultSet.isNull(4) ? null : resultSet.getLong(4);

      if (sequenceKind == null) {
        throw new IllegalArgumentException(
            "Sequence kind for sequence " + sequenceName + " cannot be null");
      }
      if (currentCounters.containsKey(sequenceName)) {
        // The sequence is in use, we need to apply the current counter to
        // the DDL builder, instead of the one retrieved from Information Schema.
        // Add a buffer to accommodate writes that may happen after import
        // is run. Note that this is not 100% failproof, since more writes may
        // happen and they will make the sequence advances past the buffer.
        counterStartValue = currentCounters.get(sequenceName) + Sequence.SEQUENCE_COUNTER_BUFFER;
        LOG.info(
            "Sequence " + sequenceName + "'s current counter is updated to " + counterStartValue);
      }

      builder
          .createSequence(sequenceName)
          .sequenceKind(sequenceKind)
          .counterStartValue(counterStartValue)
          .skipRangeMin(skipRangeMin)
          .skipRangeMax(skipRangeMax)
          .endSequence();
    }
  }
}
