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

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/** Generates a random {@link Ddl}. */
@AutoValue
public abstract class RandomDdlGenerator {

  // No bytes, no floats.
  private static final Type.Code[] PK_TYPES =
      new Type.Code[] {
        Type.Code.BOOL, Type.Code.INT64, Type.Code.STRING, Type.Code.TIMESTAMP, Type.Code.DATE
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

  private static final int MAX_PKS = 16;

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

  public static Builder builder() {

    return new AutoValue_RandomDdlGenerator.Builder()
        .setRandom(new Random())
        .setArrayChance(20)
        .setMaxPkComponents(3)
        .setMaxBranchPerLevel(new int[] {3, 2, 1, 1, 1, 1, 1})
        .setMaxIndex(2)
        .setMaxForeignKeys(2)
        // TODO: enable once CHECK constraints are enabled
        .setEnableCheckConstraints(false)
        .setMaxColumns(8)
        .setMaxIdLength(11)
        // TODO: enable generated columns once they are supported.
        .setEnableGeneratedColumns(false);
  }

  /** A builder for {@link RandomDdlGenerator}. */
  @AutoValue.Builder
  public abstract static class Builder {

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
  }

  public abstract Builder toBuilder();

  private Set<String> allIdentifiers = Sets.newHashSet();

  public Ddl generate() {
    Ddl.Builder builder = Ddl.builder();
    int numParentTables = 1 + getRandom().nextInt(getMaxBranchPerLevel()[0]);
    for (int i = 0; i < numParentTables; i++) {
      generateTable(builder, null, 0);
    }

    return builder.build();
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
      Column pkColumn = generateColumn(PK_TYPES, -1);
      tableBuilder.addColumn(pkColumn);

      IndexColumn.Order order = rnd.nextBoolean() ? IndexColumn.Order.ASC : IndexColumn.Order.DESC;
      IndexColumn pk = IndexColumn.create(pkColumn.name(), order);
      tableBuilder.primaryKey().set(pk).end();
    }

    int numColumns = rnd.nextInt(getMaxColumns());

    for (int i = 0; i < numColumns; i++) {
      Column column = generateColumn(COLUMN_TYPES, getArrayChance());
      tableBuilder.addColumn(column);
    }

    Table table = tableBuilder.build();

    if (getEnableGeneratedColumns()) {
      // Add a generated column
      Column depColumn = table.columns().get(rnd.nextInt(table.columns().size()));
      Column generatedColumn = Column.builder().name("generated").type(depColumn.type()).max()
          .notNull(depColumn.notNull()).generatedAs(depColumn.name()).stored().autoBuild();
      tableBuilder.addColumn(generatedColumn);
      table = tableBuilder.build();
    }

    int numIndexes = rnd.nextInt(getMaxIndex());
    ImmutableList.Builder<String> indexes = ImmutableList.builder();
    for (int i = 0; i < numIndexes; i++) {
      Index.Builder index = Index.builder().name(generateIdentifier(getMaxIdLength())).table(name);
      IndexColumn.IndexColumnsBuilder<Index.Builder> columns = index.columns();
      boolean interleaved = rnd.nextBoolean();
      Set<String> pks = Sets.newHashSet();
      if (interleaved) {
        index.interleaveIn(table.name());
      }
      for (IndexColumn pk : table.primaryKeys()) {
        if (interleaved) {
          columns.set(pk);
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
        if (cm.type().getCode() == Type.Code.ARRAY) {
          continue;
        }
        int val = rnd.nextInt(4);
        switch (val) {
          case 1:
            columns.asc(columnName);
            if (!pks.contains(columnName)) {
              indexColumns++;
            }
            break;
          case 2:
            columns.desc(columnName);
            if (!pks.contains(columnName)) {
              indexColumns++;
            }
            break;
          case 3:
            if (!pks.contains(columnName)) {
              columns.storing(columnName);
            }
            break;
          default:
            // skip this column
        }
      }
      columns.end();
      // index.nullFiltered(rnd.nextBoolean());
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
            ForeignKey.builder()
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
      if (!CHECK_CONSTRAINT_TYPES.contains(column.type())) {
        continue;
      }
      // An expression that won't be trivially optimized away by query optimizer.
      String expr = "TO_HEX(SHA1(CAST(" + column.name() + " AS STRING))) <= '~'";
      String checkName = generateIdentifier(getMaxIdLength());
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
    return Column.builder().name(name).type(type).size(size).notNull(nullable).autoBuild();
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
    return isArray ? Type.array(typeOf(code)) : typeOf(code);
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
    }
    throw new IllegalArgumentException("Arrays and Structs are not supported");
  }

  private Type.Code randomCode(Type.Code[] codes) {
    return codes[getRandom().nextInt(codes.length)];
  }
}
