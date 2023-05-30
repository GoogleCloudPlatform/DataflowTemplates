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
package com.google.cloud.teleport.v2.astradb.transforms;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.CqlVectorType;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.astra.db.AstraDbIO;
import org.apache.beam.sdk.io.astra.db.CqlSessionHolder;
import org.apache.beam.sdk.transforms.SerializableFunction;

/** Will Convert a Cassandra Row into a Beam Row. */
public class AstraDbToBigQueryMappingFn
    implements SerializableFunction<AstraDbIO.Read<?>, TableSchema> {

  /** Current table. */
  private final String table;

  /** Current keyspace. */
  private final String keyspace;

  /**
   * Access Table Schema.
   *
   * @param keyspace current keyspace
   * @param table current table
   */
  public AstraDbToBigQueryMappingFn(String keyspace, String table) {
    this.keyspace = keyspace;
    this.table = table;
  }

  @Override
  public TableSchema apply(AstraDbIO.Read<?> astraSource) {
    return readTableSchemaFromCassandraTable(
        CqlSessionHolder.getCqlSession(astraSource), keyspace, table);
  }

  /**
   * This function is meant to build a schema for destination table based on cassandra table schema.
   *
   * @param session current session
   * @param keyspace cassandra keyspace
   * @param table cassandra table
   * @return table schema for destination table
   */
  public static TableSchema readTableSchemaFromCassandraTable(
      CqlSession session, String keyspace, String table) {
    Metadata clusterMetadata = session.getMetadata();
    KeyspaceMetadata keyspaceMetadata =
        clusterMetadata
            .getKeyspace(keyspace)
            .orElseThrow(() -> new RuntimeException("Keyspace not found"));
    TableMetadata tableMetadata =
        keyspaceMetadata.getTable(table).orElseThrow(() -> new RuntimeException("Table not found"));
    return new TableSchema()
        .setFields(
            tableMetadata.getColumns().values().stream()
                .map(cd -> mapColumnDefinition(tableMetadata, cd))
                .collect(Collectors.toList()));
  }

  /**
   * Mapping for a column.
   *
   * @param cd current column
   * @return big query column
   */
  private static TableFieldSchema mapColumnDefinition(
      TableMetadata tableMetadata, ColumnMetadata cd) {
    TableFieldSchema tfs = new TableFieldSchema();
    tfs.setName(cd.getName().toString());
    tfs.setType(mapCassandraToBigQueryType(cd.getType()).name());
    if (tableMetadata.getPrimaryKey().contains(cd)) {
      tfs.setMode("REQUIRED");
    }
    if (cd.getType() instanceof ListType || cd.getType() instanceof SetType) {
      tfs.setMode("REPEATED");
    }
    return tfs;
  }

  /**
   * Map DataType to BigQuery StandardSQLTypeName.
   *
   * @param type cassandra type
   * @return SQL Type.
   */
  private static StandardSQLTypeName mapCassandraToBigQueryType(DataType type) {
    switch (type.getProtocolCode()) {
      case ProtocolConstants.DataType.UUID:
      case ProtocolConstants.DataType.VARCHAR:
      case ProtocolConstants.DataType.ASCII:
      case ProtocolConstants.DataType.TIMEUUID:
      case ProtocolConstants.DataType.INET:
        return StandardSQLTypeName.STRING;

      case ProtocolConstants.DataType.VARINT:
      case ProtocolConstants.DataType.DECIMAL:
        return StandardSQLTypeName.NUMERIC;

      case ProtocolConstants.DataType.COUNTER:
      case ProtocolConstants.DataType.BIGINT:
      case ProtocolConstants.DataType.TIME:
      case ProtocolConstants.DataType.INT:
      case ProtocolConstants.DataType.SMALLINT:
      case ProtocolConstants.DataType.TINYINT:
      case ProtocolConstants.DataType.DURATION:
        return StandardSQLTypeName.INT64;

      case ProtocolConstants.DataType.DOUBLE:
      case ProtocolConstants.DataType.FLOAT:
        return StandardSQLTypeName.FLOAT64;

      case ProtocolConstants.DataType.DATE:
        return StandardSQLTypeName.DATETIME;

      case ProtocolConstants.DataType.TIMESTAMP:
        return StandardSQLTypeName.TIMESTAMP;

      case ProtocolConstants.DataType.BLOB:
        return StandardSQLTypeName.BYTES;

      case ProtocolConstants.DataType.BOOLEAN:
        return StandardSQLTypeName.BOOL;

      case ProtocolConstants.DataType.CUSTOM:
        if (type instanceof CqlVectorType) {
          return StandardSQLTypeName.BYTES;
        } else {
          throw new IllegalArgumentException("Invalid custom type " + type.asCql(false, false));
        }
      default:
        throw new IllegalArgumentException(
            "Cannot Map Cassandra Type " + type.getProtocolCode() + " to Beam Type");
    }
  }
}
