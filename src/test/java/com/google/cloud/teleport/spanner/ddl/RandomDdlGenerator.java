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

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.spanner.common.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

/** Generates a random {@link Ddl}. */
@AutoValue
public abstract class RandomDdlGenerator {

  // No bytes, no floats.
  private static final Type.Code[] PK_TYPES =
      new Type.Code[] {
        Type.Code.BOOL, Type.Code.INT64, Type.Code.STRING, Type.Code.TIMESTAMP, Type.Code.DATE
      };

  private static final Type.Code[] PG_PK_TYPES =
      new Type.Code[] {
        Type.Code.PG_BOOL,
        Type.Code.PG_INT8,
        Type.Code.PG_FLOAT8,
        Type.Code.PG_TEXT,
        Type.Code.PG_VARCHAR,
        Type.Code.PG_TIMESTAMPTZ,
        Type.Code.PG_DATE
      };

  private static final Type.Code[] COLUMN_TYPES =
      new Type.Code[] {
        Type.Code.BOOL,
        Type.Code.INT64,
        Type.Code.FLOAT64,
        Type.Code.STRING,
        Type.Code.BYTES,
        Type.Code.TIMESTAMP,
        Type.Code.DATE
      };

  private static final Type.Code[] PG_COLUMN_TYPES =
      new Type.Code[] {
        Type.Code.PG_BOOL,
        Type.Code.PG_INT8,
        Type.Code.PG_FLOAT8,
        Type.Code.PG_VARCHAR,
        Type.Code.PG_BYTEA,
        Type.Code.PG_TIMESTAMPTZ,
        Type.Code.PG_NUMERIC,
        Type.Code.PG_DATE
      };

  // Types that could be used by check constraint
  private static final Set<Type.Code> CHECK_CONSTRAINT_TYPES =
      new HashSet<>(
          Arrays.asList(
              Type.Code.BOOL,
              Type.Code.INT64,
              Type.Code.FLOAT64,
              Type.Code.STRING,
              Type.Code.TIMESTAMP,
              Type.Code.DATE));

  private static final Set<Type.Code> PG_CHECK_CONSTRAINT_TYPES =
      new HashSet<>(
          Arrays.asList(
              Type.Code.PG_BOOL,
              Type.Code.PG_INT8,
              Type.Code.PG_FLOAT8,
              Type.Code.PG_TEXT,
              Type.Code.PG_VARCHAR,
              Type.Code.PG_TIMESTAMPTZ,
              Type.Code.PG_NUMERIC,
              Type.Code.PG_DATE));

  private static final int MAX_PKS = 16;

  public abstract Dialect getDialect();

  public abstract Random getRandom();

  public abstract int getArrayChance();

  public abstract int[] getMaxBranchPerLevel();

  public abstract int getMaxPkComponents();

  public abstract int getMaxColumns();

  public abstract int getMaxIdLength();

  public abstract int getMaxIndex();

  public abstract int getMaxForeignKeys();

  public abstract boolean getEnableGeneratedColumns();

  public abstract boolean getEnableCheckConstraints();

  public abstract int getMaxViews();

  public abstract int getMaxChangeStreams();

  public static Builder builder() {
    return builder(Dialect.GOOGLE_STANDARD_SQL);
  }

  public static Builder builder(Dialect dialect) {

    return new AutoValue_RandomDdlGenerator.Builder()
        .setDialect(dialect)
        .setRandom(new Random())
        .setArrayChance(20)
        .setMaxPkComponents(3)
        .setMaxBranchPerLevel(new int[] {2, 2, 1, 1, 1, 1, 1})
        // TODO(b/187873097): Enable views here once supported in production.
        .setMaxViews(0)
        .setMaxIndex(2)
        .setMaxForeignKeys(2)
        .setEnableCheckConstraints(true)
        .setMaxColumns(8)
        .setMaxIdLength(11)
        .setEnableGeneratedColumns(true)
        // Change stream is only supported in GoogleSQL, not in PostgreSQL.
        .setMaxChangeStreams(dialect == Dialect.GOOGLE_STANDARD_SQL ? 2 : 0);
  }

  /** A builder for {@link RandomDdlGenerator}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setDialect(Dialect dialect);

    public abstract Builder setRandom(Random rnd);

    public abstract Builder setArrayChance(int chance);

    public abstract Builder setMaxBranchPerLevel(int[] arr);

    public abstract Builder setMaxPkComponents(int val);

    public abstract Builder setMaxIdLength(int val);

    public abstract Builder setMaxColumns(int val);

    public abstract RandomDdlGenerator build();

    public abstract Builder setMaxIndex(int indexes);

    public abstract Builder setMaxForeignKeys(int foreignKeys);

    public abstract Builder setEnableGeneratedColumns(boolean enable);

    public abstract Builder setEnableCheckConstraints(boolean checkConstraints);

    public abstract Builder setMaxViews(int maxViews);

    public abstract Builder setMaxChangeStreams(int maxChangeStreams);
  }

  public abstract Builder toBuilder();

  private Set<String> allIdentifiers = Sets.newHashSet();

  public Ddl generate() {
    Ddl.Builder builder = Ddl.builder(getDialect());
    int numParentTables = 1 + getRandom().nextInt(getMaxBranchPerLevel()[0]);
    for (int i = 0; i < numParentTables; i++) {
      generateTable(builder, null, 0);
    }
    int numViews = getRandom().nextInt(getMaxViews() + 1);
    for (int i = 0; i < numViews; i++) {
      generateView(builder);
    }
    int numChangeStreams = getRandom().nextInt(getMaxChangeStreams() + 1);
    for (int i = 0; i < numChangeStreams; i++) {
      generateChangeStream(builder);
    }

    return builder.build();
  }

  private void generateView(Ddl.Builder builder) {
    String name = generateIdentifier(getMaxIdLength());
    View.Builder viewBuilder = builder.createView(name).security(View.SqlSecurity.INVOKER);

    Table sourceTable = selectRandomTable(builder);
    if (sourceTable == null) {
      viewBuilder.query("select 1");
    } else {
      StringBuilder queryBuilder = new StringBuilder("select ");
      boolean firstIncluded = true;
      for (Column column : sourceTable.columns()) {
        if (getRandom().nextBoolean()) {
          if (!firstIncluded) {
            queryBuilder.append(", ");
          }
          if (getDialect() == Dialect.POSTGRESQL) {
            queryBuilder.append("\"");
          }
          queryBuilder.append(column.name());
          if (getDialect() == Dialect.POSTGRESQL) {
            queryBuilder.append("\"");
          }
          firstIncluded = false;
        }
      }
      if (firstIncluded) {
        queryBuilder.append("1");
      }
      queryBuilder.append(" from ");
      if (getDialect() == Dialect.POSTGRESQL) {
        queryBuilder.append("\"");
      }
      queryBuilder.append(sourceTable.name());
      if (getDialect() == Dialect.POSTGRESQL) {
        queryBuilder.append("\"");
      }
      viewBuilder.query(queryBuilder.toString());
    }

    viewBuilder.endView();
  }

  private void generateChangeStream(Ddl.Builder builder) {
    if (getDialect() == Dialect.POSTGRESQL) {
      throw new IllegalArgumentException("Change stream is not supported in PostgreSQL dialect.");
    }

    String name = generateIdentifier(getMaxIdLength());
    ChangeStream.Builder changeStreamBuilder = builder.createChangeStream(name);

    generateChangeStreamForClause(builder, changeStreamBuilder);

    ImmutableList.Builder<String> options = ImmutableList.builder();
    if (getRandom().nextBoolean()) {
      options.add("retention_period=\"7d\"");
    }
    if (getRandom().nextBoolean()) {
      options.add("value_capture_type=\"OLD_AND_NEW_VALUES\"");
    }
    changeStreamBuilder.options(options.build());

    changeStreamBuilder.endChangeStream();
  }

  private void generateChangeStreamForClause(
      Ddl.Builder builder, ChangeStream.Builder changeStreamBuilder) {
    boolean forAll = getRandom().nextBoolean();
    if (forAll) {
      changeStreamBuilder.forClause("FOR ALL");
      return;
    }

    Table table = selectRandomTable(builder);
    if (table == null) {
      return;
    }

    StringBuilder forClause = new StringBuilder("FOR `").append(table.name()).append("`");
    boolean allColumns = getRandom().nextBoolean();
    if (allColumns) {
      changeStreamBuilder.forClause(forClause.toString());
      return;
    }

    // Select a random set of watched columns, excluding primary keys and generated columns.
    Set<String> watchedColumns = Sets.newHashSet();
    Set<String> primaryKeys =
        table.primaryKeys().stream().map(pk -> pk.name()).collect(Collectors.toSet());
    for (Column column : table.columns()) {
      if (getRandom().nextBoolean()
          && !primaryKeys.contains(column.name())
          && !column.isGenerated()) {
        watchedColumns.add("`" + column.name() + "`");
      }
    }
    forClause.append("(").append(String.join(", ", watchedColumns)).append(")");
    changeStreamBuilder.forClause(forClause.toString());
  }

  private void generateTable(Ddl.Builder builder, Table parent, int level) {
    String name = generateIdentifier(getMaxIdLength());
    Table.Builder tableBuilder = builder.createTable(name);

    int pkSize = 0;
    if (parent != null) {
      tableBuilder.interleaveInParent(parent.name());
      for (IndexColumn pk : parent.primaryKeys()) {
        Column pkColumn = parent.column(pk.name());
        tableBuilder.addColumn(pkColumn);
        tableBuilder.primaryKey().set(pk).end();
        pkSize++;
      }
    }

    Random rnd = getRandom();
    int numPks = Math.min(1 + rnd.nextInt(getMaxPkComponents()), MAX_PKS - pkSize);
    for (int i = 0; i < numPks; i++) {
      Column pkColumn =
          generateColumn(
              (getDialect() == Dialect.GOOGLE_STANDARD_SQL) ? PK_TYPES : PG_PK_TYPES, -1);
      tableBuilder.addColumn(pkColumn);

      IndexColumn.Order order = rnd.nextBoolean() ? IndexColumn.Order.ASC : IndexColumn.Order.DESC;
      if (getDialect() == Dialect.POSTGRESQL) {
        order = IndexColumn.Order.ASC;
      }
      IndexColumn pk = IndexColumn.create(pkColumn.name(), order, getDialect());
      tableBuilder.primaryKey().set(pk).end();
    }

    int numColumns = rnd.nextInt(getMaxColumns());

    for (int i = 0; i < numColumns; i++) {
      Column column =
          generateColumn(
              (getDialect() == Dialect.GOOGLE_STANDARD_SQL) ? COLUMN_TYPES : PG_COLUMN_TYPES,
              getArrayChance());
      tableBuilder.addColumn(column);
    }

    Table table = tableBuilder.build();

    if (getEnableGeneratedColumns()) {
      // Add a generated column
      Column depColumn = table.columns().get(rnd.nextInt(table.columns().size()));
      String expr = depColumn.name();
      if (getDialect() == Dialect.POSTGRESQL) {
        expr = "\"" + expr + "\"";
      }
      Column generatedColumn =
          Column.builder(getDialect())
              .name("generated")
              .type(depColumn.type())
              .max()
              .notNull(depColumn.notNull())
              .generatedAs(expr)
              .stored()
              .autoBuild();
      tableBuilder.addColumn(generatedColumn);
      table = tableBuilder.build();
    }

    int numIndexes = rnd.nextInt(getMaxIndex());
    ImmutableList.Builder<String> indexes = ImmutableList.builder();
    for (int i = 0; i < numIndexes; i++) {
      Index.Builder index =
          Index.builder(getDialect()).name(generateIdentifier(getMaxIdLength())).table(name);
      IndexColumn.IndexColumnsBuilder<Index.Builder> columns = index.columns();
      ImmutableList.Builder<String> filters = ImmutableList.builder();
      boolean interleaved = rnd.nextBoolean();
      Set<String> pks = Sets.newHashSet();
      // Do not interleave indexes at the last table level.
      // This causes tests to fail as generated schema exceeds interleaving limit.
      int finalLevel = getMaxBranchPerLevel().length - 1;
      if (interleaved && level < finalLevel) {
        index.interleaveIn(table.name());
      }
      for (IndexColumn pk : table.primaryKeys()) {
        if (interleaved) {
          columns.set(pk);
          if (rnd.nextBoolean()) {
            filters.add("\"" + pk.name() + "\" IS NOT NULL");
          }
        }
        pks.add(pk.name());
      }

      int maxNumIndexColumns = MAX_PKS - pks.size();
      int indexColumns = 0;
      for (int j = 0; j < table.columns().size(); j++) {
        Column cm = table.columns().get(j);
        String columnName = cm.name();
        if (indexColumns >= maxNumIndexColumns) {
          break;
        }
        // Already added.
        if (interleaved && pks.contains(columnName)) {
          continue;
        }
        if (cm.type().getCode() == Type.Code.ARRAY || cm.type().getCode() == Type.Code.PG_ARRAY) {
          continue;
        }
        // Skip the types that may generate NaN value, as NaN cannot be used as a key
        if (cm.type().getCode() == Type.Code.FLOAT64
            || cm.type().getCode() == Type.Code.PG_FLOAT8
            || cm.type().getCode() == Type.Code.PG_NUMERIC) {
          continue;
        }
        int val = rnd.nextInt(4);
        switch (val) {
          case 0:
            columns.create().name(columnName).asc();
            if (!pks.contains(columnName)) {
              indexColumns++;
            }
            break;
          case 1:
            columns.create().name(columnName).desc();
            if (!pks.contains(columnName)) {
              indexColumns++;
            }
            break;
          case 2:
            if (!pks.contains(columnName)) {
              columns.create().name(columnName).storing();
            }
            break;
          default:
            // skip this column
        }
        // skip the primary key column if it is randomed to storing
        if (val < 2 || (val < 3 && !pks.contains(columnName))) {
          if (getDialect() == Dialect.POSTGRESQL) {
            if (rnd.nextBoolean()) {
              columns.nullsFirst();
            } else {
              columns.nullsLast();
            }
          }
          columns.endIndexColumn();
          if (rnd.nextBoolean()) {
            filters.add("\"" + columnName + "\" IS NOT NULL");
          }
        }
      }
      columns.end();
      index.nullFiltered(rnd.nextBoolean());
      index.filter(String.join(" AND ", filters.build()));
      // index.unique(rnd.nextBoolean());
      if (indexColumns > 0) {
        indexes.add(index.build().prettyPrint());
      }
    }
    tableBuilder.indexes(indexes.build());

    if (parent != null) {
      // Create redundant foreign keys to the parent table.
      int numForeignKeys = rnd.nextInt(getMaxForeignKeys());
      ImmutableList.Builder<String> foreignKeys = ImmutableList.builder();
      for (int i = 0; i < numForeignKeys; i++) {
        ForeignKey.Builder foreignKeyBuilder =
            ForeignKey.builder(getDialect())
                .name(generateIdentifier(getMaxIdLength()))
                .table(name)
                .referencedTable(parent.name());
        for (IndexColumn pk : parent.primaryKeys()) {
          foreignKeyBuilder.columnsBuilder().add(pk.name());
          foreignKeyBuilder.referencedColumnsBuilder().add(pk.name());
        }
        ForeignKey foreignKey = foreignKeyBuilder.build();
        if (foreignKey.columns().size() > 0) {
          foreignKeys.add(foreignKey.prettyPrint());
        }
      }
      tableBuilder.foreignKeys(foreignKeys.build());
    }

    while (getEnableCheckConstraints()) {
      ImmutableList.Builder<String> checkConstraints = ImmutableList.builder();
      // Pick a random column to add check constraint on.
      ImmutableList<Column> columns = table.columns();
      int colIndex = rnd.nextInt(columns.size());
      Column column = columns.get(colIndex);
      if (getDialect() == Dialect.GOOGLE_STANDARD_SQL
          && !CHECK_CONSTRAINT_TYPES.contains(column.type().getCode())) {
        continue;
      }
      if (getDialect() == Dialect.POSTGRESQL
          && !PG_CHECK_CONSTRAINT_TYPES.contains(column.type().getCode())) {
        continue;
      }
      // An expression that won't be trivially optimized away by query optimizer.

      String expr = "TO_HEX(SHA1(CAST(" + column.name() + " AS STRING))) <= '~'";
      String checkName = generateIdentifier(getMaxIdLength());
      if (getDialect() == Dialect.POSTGRESQL) {
        expr = "LENGTH(CAST(\"" + column.name() + "\" AS VARCHAR)) > '-1'::bigint";
        checkName = "\"" + checkName + "\"";
      }
      checkConstraints.add("CONSTRAINT " + checkName + " CHECK(" + expr + ")");
      tableBuilder.checkConstraints(checkConstraints.build());
      break;
    }

    tableBuilder.endTable();

    table = tableBuilder.build();

    int nextLevel = level + 1;
    int[] maxBranchPerLevel = getMaxBranchPerLevel();
    if (nextLevel < maxBranchPerLevel.length
        && maxBranchPerLevel[nextLevel] > 0
        && table.primaryKeys().size() < MAX_PKS) {
      generateTable(builder, table, nextLevel);
    }
  }

  private Column generateColumn(Type.Code[] codes, int arrayPercentage) {
    int length = 1 + getRandom().nextInt(getMaxIdLength());
    String name = generateIdentifier(length);
    Type type = generateType(codes, arrayPercentage);
    int size = -1;
    boolean nullable = getRandom().nextBoolean();
    return Column.builder(getDialect())
        .name(name)
        .type(type)
        .size(size)
        .notNull(nullable)
        .autoBuild();
  }

  private String generateIdentifier(int length) {
    String id;
    while (true) {
      id = RandomUtils.randomAlphanumeric(length);
      if (!allIdentifiers.contains(id.toLowerCase())) {
        break;
      }
    }
    allIdentifiers.add(id.toLowerCase());
    return id;
  }

  private Type generateType(Type.Code[] codes, int arrayPercentage) {
    boolean isArray = getRandom().nextInt(100) <= arrayPercentage;
    Type.Code code = randomCode(codes);
    if (isArray) {
      if (getDialect() == Dialect.POSTGRESQL) {
        return Type.pgArray(typeOf(code));
      }
      return Type.array(typeOf(code));
    }
    return typeOf(code);
  }

  private Table selectRandomTable(Ddl.Builder builder) {
    Collection<Table> tables = builder.tables();
    int tablesToSkip = getRandom().nextInt(tables.size());
    for (Table table : tables) {
      if (tablesToSkip > 0) {
        --tablesToSkip;
      } else {
        return table;
      }
    }
    return null;
  }

  private Type typeOf(Type.Code code) {
    switch (code) {
      case BOOL:
        return Type.bool();
      case FLOAT64:
        return Type.float64();
      case STRING:
        return Type.string();
      case BYTES:
        return Type.bytes();
      case TIMESTAMP:
        return Type.timestamp();
      case DATE:
        return Type.date();
      case INT64:
        return Type.int64();
      case PG_BOOL:
        return Type.pgBool();
      case PG_INT8:
        return Type.pgInt8();
      case PG_FLOAT8:
        return Type.pgFloat8();
      case PG_TEXT:
        return Type.pgText();
      case PG_VARCHAR:
        return Type.pgVarchar();
      case PG_BYTEA:
        return Type.pgBytea();
      case PG_TIMESTAMPTZ:
        return Type.pgTimestamptz();
      case PG_NUMERIC:
        return Type.pgNumeric();
      case PG_DATE:
        return Type.pgDate();
    }
    throw new IllegalArgumentException("Arrays and Structs are not supported");
  }

  private Type.Code randomCode(Type.Code[] codes) {
    return codes[getRandom().nextInt(codes.length)];
  }
}
