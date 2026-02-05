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
import java.util.HashMap;
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

  public ComparisonRecordMapper(
      ISchemaMapper schemaMapper, ISpannerMigrationTransformer transformer) {
    this.schemaMapper = schemaMapper;
    this.transformer = transformer;
  }

  public ComparisonRecord mapFrom(GenericRecord avroRecord, Ddl ddl) {
    try {
      // 1. Extract metadata from SourceRow (wrapped in GenericRecord)
      String tableName = avroRecord.get("tableName").toString();
      String shardId =
          avroRecord.get("shardId") != null ? avroRecord.get("shardId").toString() : "";
      GenericRecord payload = (GenericRecord) avroRecord.get("payload");

      // 2. Convert payload to Map<String, Value> using GenericRecordTypeConvertor
      GenericRecordTypeConvertor convertor =
          new GenericRecordTypeConvertor(schemaMapper, "", shardId, transformer);
      Map<String, Value> values = convertor.transformChangeEvent(payload, tableName);

      if (values == null) {
        return null; // Transformation filtered out the record
      }

      // 3. Get Primary Key names from Spanner DDL (destination table)
      // Note: GenericRecordTypeConvertor already handles table mapping, so 'values'
      // keys are Spanner column names.
      // We need the Spanner table name to look up PKs.
      String spannerTableName = schemaMapper.getSpannerTableName("", tableName);
      Table table = ddl.table(spannerTableName);
      //
      if (table == null) {
        throw new RuntimeException("Table not found in DDL: " + spannerTableName);
      }
      List<String> pkNames =
          table.primaryKeys().stream().map(IndexColumn::name).collect(Collectors.toList());

      return buildRecord(spannerTableName, values, pkNames);

    } catch (Exception e) {
      throw new RuntimeException("Error mapping GenericRecord to ComparisonRecord", e);
    }
  }

  public ComparisonRecord mapFrom(Struct spannerStruct, Ddl ddl) {
    // 1. Convert Struct to Map<String, Value>
    Map<String, Value> values = new HashMap<>();
    spannerStruct
        .getType()
        .getStructFields()
        .forEach(
            field -> {
              if (!field.getName().equals("__tableName__")) {
                values.put(field.getName(), spannerStruct.getValue(field.getName()));
              }
            });

    String tableName = spannerStruct.getString("__tableName__");

    // 2. Get Primary Key names from DDL
    Table table = ddl.table(tableName);
    if (table == null) {
      throw new RuntimeException("Table not found in DDL: " + tableName);
    }
    List<String> pkNames =
        table.primaryKeys().stream().map(IndexColumn::name).collect(Collectors.toList());

    return buildRecord(tableName, values, pkNames);
  }

  private ComparisonRecord buildRecord(
      String tableName, Map<String, Value> data, List<String> pkNames) {
    Map<String, Value> dataToHash = new TreeMap<>(data);

    dataToHash.put("__tableName__", Value.string(tableName));

    Hasher hasher = Hashing.murmur3_128().newHasher();
    UnifiedHasherVisitor hasherVisitor = new UnifiedHasherVisitor(hasher);
    for (Map.Entry<String, Value> entry : data.entrySet()) {
      // Hash the key
      hasher.putString(entry.getKey(), StandardCharsets.UTF_8);
      // Hash the value
      IUnifiedVisitor.dispatch(entry.getValue(), hasherVisitor);
    }

    String hash = hasher.hash().toString();

    UnifiedStringVisitor stringVisitor = new UnifiedStringVisitor();
    List<Column> primaryKeyColumns =
        pkNames.stream()
            .map(
                pkName -> {
                  Value val = data.get(pkName);
                  if (val == null) {
                    throw new RuntimeException(
                        "Primary key column "
                            + pkName
                            + " not found in data for table "
                            + tableName);
                  }
                  IUnifiedVisitor.dispatch(val, stringVisitor);
                  return Column.builder()
                      .setColName(pkName)
                      .setColValue(stringVisitor.getResult())
                      .build();
                })
            .collect(Collectors.toList());

    return ComparisonRecord.builder()
        .setTableName(tableName)
        .setHash(hash)
        .setPrimaryKeyColumns(primaryKeyColumns)
        .build();
  }
}
