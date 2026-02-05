/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.mapper;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.dto.Column;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.avro.GenericRecordTypeConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.visitor.IUnifiedVisitor;
import com.google.cloud.teleport.v2.visitor.UnifiedHasherVisitor;
import com.google.cloud.teleport.v2.visitor.UnifiedStringVisitor;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mapper class to convert various inputs into a {@link ComparisonRecord}. It handles the logic of
 * extracting data, converting types, and computing hashes.
 */
public class ComparisonRecordMapper implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ComparisonRecordMapper.class);
  private final ISchemaMapper schemaMapper;
  private final ISpannerMigrationTransformer transformer;
  private final Ddl ddl;

  public ComparisonRecordMapper(
      ISchemaMapper schemaMapper, ISpannerMigrationTransformer transformer, Ddl ddl) {
    this.schemaMapper = schemaMapper;
    this.transformer = transformer;
    this.ddl = ddl;
  }

  public ComparisonRecord mapFrom(GenericRecord avroRecord) {
    try {
      String tableName = avroRecord.get("tableName").toString();
      String shardId =
          avroRecord.get("shardId") != null ? avroRecord.get("shardId").toString() : "";
      GenericRecord payload = (GenericRecord) avroRecord.get("payload");
      GenericRecordTypeConvertor convertor =
          new GenericRecordTypeConvertor(schemaMapper, "", shardId, transformer);
      Map<String, Value> values = convertor.transformChangeEvent(payload, tableName);

      if (values == null) {
        return null;
      }
      // Map to Spanner table using mapper
      String spannerTableName = schemaMapper.getSpannerTableName("", tableName);
      Table table = ddl.table(spannerTableName);
      if (table == null) {
        throw new RuntimeException("Table not found in DDL: " + spannerTableName);
      }
      List<String> pkNames =
          table.primaryKeys().stream().map(IndexColumn::name).collect(Collectors.toList());
      return buildRecord(spannerTableName, new TreeMap<>(values), pkNames);
    } catch (Exception e) {
      throw new RuntimeException("Error mapping GenericRecord to ComparisonRecord", e);
    }
  }

  public ComparisonRecord mapFrom(Struct spannerStruct) {
    TreeMap<String, Value> values = new TreeMap<>();
    spannerStruct.getType().getStructFields().stream()
        .filter(field -> !field.getName().equals("__tableName__"))
        .forEach(field -> values.put(field.getName(), spannerStruct.getValue(field.getName())));

    String tableName = spannerStruct.getString("__tableName__");
    Table table = ddl.table(tableName);
    if (table == null) {
      throw new RuntimeException("Table not found in DDL: " + tableName);
    }
    List<String> pkNames =
        table.primaryKeys().stream().map(IndexColumn::name).collect(Collectors.toList());

    return buildRecord(tableName, values, pkNames);
  }

  // Accepts a TreeMap of values to enforce strict natural ordering of keys during hashing.
  private ComparisonRecord buildRecord(
      String tableName, TreeMap<String, Value> data, List<String> pkNames) {

    // 1. Use the record data to compute the hash
    Hasher hasher = Hashing.murmur3_128().newHasher();
    UnifiedHasherVisitor hasherVisitor = new UnifiedHasherVisitor(hasher);
    for (Map.Entry<String, Value> entry : data.entrySet()) {
      // Hash the columnNames
      hasher.putString(entry.getKey(), StandardCharsets.UTF_8);
      // Hash the columnValues
      IUnifiedVisitor.dispatch(entry.getValue(), hasherVisitor);
    }
    // Add the tableName to the hasher at the end
    hasher.putString(tableName, StandardCharsets.UTF_8);
    String hash = hasher.hash().toString();

    // 2. Use the pk column names to form the full primary keys from the record data
    UnifiedStringVisitor stringVisitor = new UnifiedStringVisitor();
    List<Column> primaryKeyColumns =
        pkNames.stream()
            .map(
                pkName -> {
                  Value val = data.get(pkName);
                  IUnifiedVisitor.dispatch(val, stringVisitor);
                  return Column.builder()
                      .setColName(pkName)
                      .setColValue(stringVisitor.getResult())
                      .build();
                })
            .collect(Collectors.toList());

    // 3. Build the final record
    return ComparisonRecord.builder()
        .setTableName(tableName)
        .setHash(hash)
        .setPrimaryKeyColumns(primaryKeyColumns)
        .build();
  }
}
