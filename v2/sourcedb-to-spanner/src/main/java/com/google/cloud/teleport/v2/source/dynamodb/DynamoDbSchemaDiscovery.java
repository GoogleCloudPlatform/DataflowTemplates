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
package com.google.cloud.teleport.v2.source.dynamodb;

import com.google.cloud.teleport.v2.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

/**
 * Discovers tables and attribute schemas from Amazon DynamoDB.
 *
 * <p>Because DynamoDB {@code DescribeTable} only returns attribute definitions for key attributes,
 * {@code discoverTableSchema} combines {@code DescribeTable} metadata with a sample {@code Scan} to
 * infer types for non-key attributes present in the table items.
 */
public class DynamoDbSchemaDiscovery {

  private static final Logger LOG = LoggerFactory.getLogger(DynamoDbSchemaDiscovery.class);
  private static final int SAMPLE_SCAN_LIMIT = 1000;

  /** Discovers all table names from DynamoDB using paginated {@code ListTables} calls. */
  public ImmutableList<String> discoverTables(DynamoDbClient client)
      throws SchemaDiscoveryException {
    try {
      ImmutableList.Builder<String> tablesBuilder = ImmutableList.builder();
      String lastEvaluatedTableName = null;
      do {
        ListTablesRequest.Builder requestBuilder = ListTablesRequest.builder();
        if (lastEvaluatedTableName != null) {
          requestBuilder.exclusiveStartTableName(lastEvaluatedTableName);
        }
        ListTablesResponse response = client.listTables(requestBuilder.build());
        if (response.hasTableNames()) {
          tablesBuilder.addAll(response.tableNames());
        }
        lastEvaluatedTableName = response.lastEvaluatedTableName();
      } while (lastEvaluatedTableName != null && !lastEvaluatedTableName.isEmpty());
      ImmutableList<String> tables = tablesBuilder.build();
      LOG.info("Discovered DynamoDB tables: {}", tables);
      return tables;
    } catch (SdkException e) {
      LOG.error("Failed to list DynamoDB tables", e);
      throw new SchemaDiscoveryException(e);
    }
  }

  /** Discovers the column/attribute schema for each specified DynamoDB table. */
  public ImmutableMap<String, ImmutableMap<String, SourceColumnType>> discoverTableSchema(
      DynamoDbClient client, List<String> tables) throws SchemaDiscoveryException {
    try {
      ImmutableMap.Builder<String, ImmutableMap<String, SourceColumnType>> schemaBuilder =
          ImmutableMap.builder();
      for (String table : tables) {
        schemaBuilder.put(table, discoverSingleTableSchema(client, table));
      }
      return schemaBuilder.build();
    } catch (SdkException e) {
      LOG.error("Failed to discover DynamoDB table schemas for tables {}", tables, e);
      throw new SchemaDiscoveryException(e);
    }
  }

  private ImmutableMap<String, SourceColumnType> discoverSingleTableSchema(
      DynamoDbClient client, String tableName) {
    Map<String, String> colToType = new LinkedHashMap<>();

    DescribeTableResponse describeResponse =
        client.describeTable(DescribeTableRequest.builder().tableName(tableName).build());
    TableDescription tableDescription = describeResponse.table();
    if (tableDescription != null && tableDescription.hasAttributeDefinitions()) {
      for (AttributeDefinition attrDef : tableDescription.attributeDefinitions()) {
        if (attrDef.attributeName() != null && attrDef.attributeTypeAsString() != null) {
          colToType.put(attrDef.attributeName(), attrDef.attributeTypeAsString().toUpperCase());
        }
      }
    }

    ScanResponse scanResponse =
        client.scan(ScanRequest.builder().tableName(tableName).limit(SAMPLE_SCAN_LIMIT).build());
    if (scanResponse != null && scanResponse.hasItems()) {
      for (Map<String, AttributeValue> item : scanResponse.items()) {
        for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
          String attrName = entry.getKey();
          String inferredType = inferDynamoDbType(entry.getValue());
          String existingType = colToType.get(attrName);
          if (existingType == null
              || ("NULL".equals(existingType) && !"NULL".equals(inferredType))) {
            colToType.put(attrName, inferredType);
          }
        }
      }
    }

    ImmutableMap.Builder<String, SourceColumnType> tableColumnsBuilder = ImmutableMap.builder();
    for (Map.Entry<String, String> entry : colToType.entrySet()) {
      tableColumnsBuilder.put(
          entry.getKey(), new SourceColumnType(entry.getValue(), new Long[] {}, new Long[] {}));
    }
    return tableColumnsBuilder.build();
  }

  static String inferDynamoDbType(AttributeValue attrValue) {
    if (attrValue == null || attrValue.type() == null) {
      return "NULL";
    }
    switch (attrValue.type()) {
      case S:
        return "S";
      case N:
        return "N";
      case B:
        return "B";
      case BOOL:
        return "BOOL";
      case NUL:
        return "NULL";
      case M:
        return "M";
      case L:
        return "L";
      case SS:
        return "SS";
      case NS:
        return "NS";
      case BS:
        return "BS";
      case UNKNOWN_TO_SDK_VERSION:
      default:
        return "UNSUPPORTED";
    }
  }
}
