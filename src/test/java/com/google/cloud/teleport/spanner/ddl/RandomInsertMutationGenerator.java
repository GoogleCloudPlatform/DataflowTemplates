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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;

/**
 * Given a {@link Ddl} Generates a stream of random Cloud Spanner mutations for the specified table.
 */
public class RandomInsertMutationGenerator {

  private final Ddl ddl;

  public RandomInsertMutationGenerator(Ddl ddl) {
    this.ddl = ddl;
  }

  public Stream<Mutation> stream(String tableName) {
    Table table = ddl.table(tableName);
    if (table == null) {
      throw new IllegalArgumentException("Unknown table " + tableName);
    }
    return Stream.generate(new TableSupplier(table));
  }

  public Stream<MutationGroup> stream() {
    Map<String, TableSupplier> suppliers = new HashMap<>();
    for (Table t : ddl.allTables()) {
      suppliers.put(t.name(), new TableSupplier(t));
    }

    Collection<Table> roots = ddl.rootTables();
    List<Supplier<MutationGroup>> groupSuppliers = new ArrayList<>();
    Random rand = new Random();
    for (Table r : roots) {
      groupSuppliers.add(new TableHierarchySupplier(ddl, r, suppliers, rand));
    }

    return Stream.generate(new DbSupplier(groupSuppliers, rand));
  }

  private static class DbSupplier implements Supplier<MutationGroup> {

    private final List<Supplier<MutationGroup>> suppliers;
    private final Random rand;

    private DbSupplier(List<Supplier<MutationGroup>> suppliers, Random rand) {
      this.suppliers = suppliers;
      this.rand = rand;
    }

    @Override
    public MutationGroup get() {
      int i = rand.nextInt(suppliers.size());
      return suppliers.get(i).get();
    }
  }

  private static class TableHierarchySupplier implements Supplier<MutationGroup> {

    private final Ddl ddl;
    private final Table root;
    private final Map<String, TableSupplier> suppliers;
    private final Random rand;

    public TableHierarchySupplier(
        Ddl ddl, Table root, Map<String, TableSupplier> suppliers, Random rand) {
      this.ddl = ddl;
      this.root = root;
      this.suppliers = suppliers;
      this.rand = rand;
    }

    @Override
    public MutationGroup get() {
      LinkedList<Mutation> mutations = generate(root, Collections.emptyMap(), suppliers, 1);
      Mutation primary = mutations.removeFirst();
      return MutationGroup.create(primary, mutations);
    }

    private LinkedList<Mutation> generate(
        Table r, Map<String, Value> overrides, Map<String, TableSupplier> suppliers, int num) {
      LinkedList<Mutation> result = new LinkedList<>();
      for (int i = 0; i < num; i++) {
        TableSupplier tableSupplier = suppliers.get(r.name());

        Mutation mutation = tableSupplier.generateMutation(overrides);
        Map<String, Value> mutationMap = mutation.asMap();

        Map<String, Value> pkMap = new HashMap<>();
        for (IndexColumn pk : r.primaryKeys()) {
          Value value = mutationMap.get(pk.name());
          pkMap.put(pk.name(), value);
        }

        result.add(mutation);
        for (Table c : ddl.childTables(r.name())) {
          LinkedList<Mutation> children = generate(c, pkMap, suppliers, rand.nextInt(3));
          result.addAll(children);
        }
      }
      return result;
    }
  }

  private static class TableSupplier implements Supplier<Mutation> {

    private final Table table;
    private final Map<String, Iterator<Value>> valueGenerators = new LinkedHashMap<>();

    public TableSupplier(Table table) {
      this.table = checkNotNull(table);
      RandomValueGenerator randomValueGenerator = RandomValueGenerator.defaultInstance();

      for (Column column : table.columns()) {
        valueGenerators.put(column.name(), randomValueGenerator.valueStream(column).iterator());
      }
    }

    @Override
    public Mutation get() {
      return generateMutation(Collections.emptyMap());
    }

    public Mutation generateMutation(Map<String, Value> overrides) {
      Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(randomCase(table.name()));
      for (Map.Entry<String, Iterator<Value>> values : valueGenerators.entrySet()) {
        String columnName = values.getKey();
        Value value = overrides.get(columnName);
        if (value == null) {
          value = values.getValue().next();
        }
        switch (value.getType().getCode()) {
          case BOOL:
            Boolean bool = value.isNull() ? null : value.getBool();
            builder.set(columnName).to(bool);
            break;
          case INT64:
            Long l = value.isNull() ? null : value.getInt64();
            builder.set(columnName).to(l);
            break;
          case FLOAT64:
            Double f = value.isNull() ? null : value.getFloat64();
            builder.set(columnName).to(f);
            break;
          case BYTES:
            ByteArray bytes = value.isNull() ? null : value.getBytes();
            builder.set(columnName).to(bytes);
            break;
          case STRING:
            String string = value.isNull() ? null : value.getString();
            builder.set(columnName).to(string);
            break;
          case TIMESTAMP:
            Timestamp timestamp = value.isNull() ? null : value.getTimestamp();
            builder.set(columnName).to(timestamp);
            break;
          case DATE:
            Date date = value.isNull() ? null : value.getDate();
            builder.set(columnName).to(date);
            break;
          case ARRAY:
            switch (value.getType().getArrayElementType().getCode()) {
              case BOOL:
                List<Boolean> bools = value.isNull() ? null : value.getBoolArray();
                builder.set(columnName).toBoolArray(bools);
                break;
              case INT64:
                List<Long> longs = value.isNull() ? null : value.getInt64Array();
                builder.set(columnName).toInt64Array(longs);
                break;
              case FLOAT64:
                List<Double> doubles = value.isNull() ? null : value.getFloat64Array();
                builder.set(columnName).toFloat64Array(doubles);
                break;
              case BYTES:
                List<ByteArray> bytesArray = value.isNull() ? null : value.getBytesArray();
                builder.set(columnName).toBytesArray(bytesArray);
                break;
              case STRING:
                List<String> strings = value.isNull() ? null : value.getStringArray();
                builder.set(columnName).toStringArray(strings);
                break;
              case TIMESTAMP:
                List<Timestamp> timestamps = value.isNull() ? null : value.getTimestampArray();
                builder.set(columnName).toTimestampArray(timestamps);
                break;
              case DATE:
                List<Date> dates = value.isNull() ? null : value.getDateArray();
                builder.set(columnName).toDateArray(dates);
                break;
            }
            break;
          default:
            throw new IllegalArgumentException("Unknown toValue " + value);
        }
      }
      return builder.build();
    }
  }

  private static String randomCase(String value) {
    Random r = new Random();
    char[] chars = value.toCharArray();
    for (int i = 0; i < chars.length; i++) {
      boolean b = r.nextBoolean();
      chars[i] = b ? Character.toLowerCase(chars[i]) : Character.toUpperCase(chars[i]);
    }
    return new String(chars);
  }
}
