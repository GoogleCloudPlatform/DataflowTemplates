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

import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.teleport.spanner.ExportProtos.Export;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Scans INFORMATION_SCHEMA.* tables and build {@link Ddl}. */
public class InformationSchemaScanner {

  private static final Logger LOG = LoggerFactory.getLogger(InformationSchemaScanner.class);

  private final ReadContext context;

  public InformationSchemaScanner(ReadContext context) {
    this.context = context;
  }

  public Ddl scan() {
    Ddl.Builder builder = Ddl.builder();
    listDatabaseOptions(builder);
    listTables(builder);
    listViews(builder);
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
    ResultSet resultSet =
        context.executeQuery(
            Statement.newBuilder(
                    "SELECT t.option_name, t.option_type, t.option_value "
                        + " FROM information_schema.database_options AS t "
                        + " WHERE t.catalog_name = '' AND t.schema_name = ''")
                .build());

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

  private void listTables(Ddl.Builder builder) {
    Statement.Builder queryBuilder =
        Statement.newBuilder(
            "SELECT t.table_name, t.parent_table_name, t.on_delete_action FROM"
                + " information_schema.tables AS t WHERE t.table_catalog = '' AND"
                + " t.table_schema = ''");
    ResultSet resultSet =
        context.executeQuery(
            Statement.of(
                "SELECT COUNT(1) FROM INFORMATION_SCHEMA.COLUMNS c WHERE c.TABLE_CATALOG = '' AND"
                    + " c.TABLE_SCHEMA = 'INFORMATION_SCHEMA' AND c.TABLE_NAME = 'TABLES' AND"
                    + " c.COLUMN_NAME = 'TABLE_TYPE';"));
    // Returns a single row with a 1 if views are supported and a 0 if not.
    resultSet.next();
    if (resultSet.getLong(0) == 0) {
      LOG.info("INFORMATION_SCHEMA.TABLES.TABLE_TYPE is not present; assuming no views");
    } else {
      queryBuilder.append(" AND t.table_type != 'VIEW'");
    }

    resultSet = context.executeQuery(queryBuilder.build());
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
    ResultSet resultSet =
        context.executeQuery(
            Statement.newBuilder(
                    "SELECT c.table_name, c.column_name,"
                        + " c.ordinal_position, c.spanner_type, c.is_nullable,"
                        + " c.is_generated, c.generation_expression, c.is_stored"
                        + " FROM information_schema.columns as c"
                        + " WHERE c.table_catalog = '' AND c.table_schema = '' "
                        + " AND c.spanner_state = 'COMMITTED' "
                        + " ORDER BY c.table_name, c.ordinal_position")
                .build());
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

  private void listIndexes(Map<String, NavigableMap<String, Index.Builder>> indexes) {
    ResultSet resultSet =
        context.executeQuery(
            Statement.of(
                "SELECT t.table_name, t.index_name, t.parent_table_name,"
                    + " t.is_unique, t.is_null_filtered"
                    + " FROM information_schema.indexes AS t "
                    + " WHERE t.table_catalog = '' AND t.table_schema = '' AND t.index_type='INDEX'"
                    + " AND t.spanner_is_managed = FALSE"
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
      boolean nullFiltered = resultSet.getBoolean(4);

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
        options.add(
            optionName
                + "=\""
                + DdlUtilityComponents.OPTION_STRING_ESCAPER.escape(optionValue)
                + "\"");
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

  private void listForeignKeys(Map<String, NavigableMap<String, ForeignKey.Builder>> foreignKeys) {
    ResultSet resultSet =
        context.executeQuery(
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
                    + " ORDER BY rc.constraint_name, kcu1.ordinal_position;"));
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
              k -> ForeignKey.builder().name(name).table(table).referencedTable(referencedTable));
      foreignKey.columnsBuilder().add(column);
      foreignKey.referencedColumnsBuilder().add(referencedColumn);
    }
  }

  private Map<String, NavigableMap<String, CheckConstraint>> listCheckConstraints() {
    Map<String, NavigableMap<String, CheckConstraint>> checkConstraints = Maps.newHashMap();
    ResultSet resultSet =
        context.executeQuery(
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
                    + " AND cc.SPANNER_STATE = 'COMMITTED'"
                    + ";"));
    while (resultSet.next()) {
      String table = resultSet.getString(0);
      String name = resultSet.getString(1);
      String expression = resultSet.getString(2);
      Map<String, CheckConstraint> tableCheckConstraints =
          checkConstraints.computeIfAbsent(table, k -> Maps.newTreeMap());
      tableCheckConstraints.computeIfAbsent(
          name, k -> CheckConstraint.builder().name(name).expression(expression).build());
    }
    return checkConstraints;
  }

  private void listViews(Ddl.Builder builder) {
    ResultSet resultSet =
        context.executeQuery(
            Statement.of(
                "SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES t WHERE t.TABLE_CATALOG = '' AND"
                    + " t.TABLE_SCHEMA = 'INFORMATION_SCHEMA' AND t.TABLE_NAME = 'VIEWS'"));
    // Returns a single row with a 1 if views are supported and a 0 if not.
    resultSet.next();
    if (resultSet.getLong(0) == 0) {
      LOG.info("INFORMATION_SCHEMA.VIEWS is not present; assuming no views");
      return;
    }

    resultSet =
        context.executeQuery(
            Statement.of(
                "SELECT v.table_name, v.view_definition"
                    + " FROM information_schema.views AS v"
                    + " WHERE v.table_catalog = '' AND v.table_schema = ''"));

    while (resultSet.next()) {
      String viewName = resultSet.getString(0);
      String viewQuery = resultSet.getString(1);
      LOG.debug("Schema View {}", viewName);
      builder.createView(viewName).query(viewQuery).security(View.SqlSecurity.INVOKER).endView();
    }
  }
}
