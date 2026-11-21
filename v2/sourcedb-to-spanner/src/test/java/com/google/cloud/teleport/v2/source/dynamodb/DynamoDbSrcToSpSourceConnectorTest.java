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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.reader.io.schema.SourceSchema;
import com.google.cloud.teleport.v2.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.reader.io.schema.typemapping.UnifiedTypeMapper;
import com.google.cloud.teleport.v2.spanner.migrations.constants.Constants;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.AstraConnectionConfig;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.DynamoDbConnectionConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.aws2.common.ClientConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

/** Unit tests for the DynamoDB source connector package. */
@RunWith(JUnit4.class)
public class DynamoDbSrcToSpSourceConnectorTest {

  private final DynamoDbSrcToSpSourceConnector connector = new DynamoDbSrcToSpSourceConnector();

  @Test
  public void testGetSourceType() {
    assertThat(connector.getSourceType()).isEqualTo(Constants.DYNAMODB_SOURCE_TYPE);
    assertThat(connector.getDlqSourceType()).isEqualTo(Constants.DYNAMODB_SOURCE_TYPE);
  }

  @Test
  public void testGetTypeMapping() {
    var mappings = connector.getTypeMapping();
    assertThat(mappings).containsKey("S");
    assertThat(mappings).containsKey("STRING");
    assertThat(mappings).containsKey("N");
    assertThat(mappings).containsKey("NUMBER");
    assertThat(mappings).containsKey("B");
    assertThat(mappings).containsKey("BINARY");
    assertThat(mappings).containsKey("BOOL");
    assertThat(mappings).containsKey("BOOLEAN");
    assertThat(mappings).containsKey("NULL");
    assertThat(mappings).containsKey("M");
    assertThat(mappings).containsKey("MAP");
    assertThat(mappings).containsKey("L");
    assertThat(mappings).containsKey("LIST");
    assertThat(mappings).containsKey("SS");
    assertThat(mappings).containsKey("STRING_SET");
    assertThat(mappings).containsKey("NS");
    assertThat(mappings).containsKey("NUMBER_SET");
    assertThat(mappings).containsKey("BS");
    assertThat(mappings).containsKey("BINARY_SET");
    assertThat(mappings).containsKey("UNSUPPORTED");
  }

  @Test
  public void testDynamoDbRowMapper_allDataTypes() {
    SourceSchemaReference schemaRef =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("dynamodb").build());
    SourceTableSchema tableSchema =
        SourceTableSchema.builder(new UnifiedTypeMapper(DynamoDbMappingsProvider.getMapping()))
            .setTableName("Users")
            .addSourceColumnNameToSourceColumnType("id", new SourceColumnType("S", null, null))
            .addSourceColumnNameToSourceColumnType("age", new SourceColumnType("N", null, null))
            .addSourceColumnNameToSourceColumnType(
                "raw_bytes", new SourceColumnType("B", null, null))
            .addSourceColumnNameToSourceColumnType(
                "is_active", new SourceColumnType("BOOL", null, null))
            .addSourceColumnNameToSourceColumnType(
                "null_col", new SourceColumnType("NULL", null, null))
            .addSourceColumnNameToSourceColumnType(
                "metadata_map", new SourceColumnType("M", null, null))
            .addSourceColumnNameToSourceColumnType(
                "items_list", new SourceColumnType("L", null, null))
            .addSourceColumnNameToSourceColumnType(
                "tags_ss", new SourceColumnType("SS", null, null))
            .addSourceColumnNameToSourceColumnType(
                "scores_ns", new SourceColumnType("NS", null, null))
            .addSourceColumnNameToSourceColumnType(
                "blobs_bs", new SourceColumnType("BS", null, null))
            .build();

    DynamoDbRowMapper rowMapper = new DynamoDbRowMapper(schemaRef, tableSchema);

    byte[] sampleBytes = "hello".getBytes(StandardCharsets.UTF_8);
    Map<String, AttributeValue> item =
        ImmutableMap.<String, AttributeValue>builder()
            .put("id", AttributeValue.builder().s("user-1").build())
            .put("age", AttributeValue.builder().n("42.5").build())
            .put(
                "raw_bytes",
                AttributeValue.builder().b(SdkBytes.fromByteArray(sampleBytes)).build())
            .put("is_active", AttributeValue.builder().bool(true).build())
            .put("null_col", AttributeValue.builder().nul(true).build())
            .put(
                "metadata_map",
                AttributeValue.builder()
                    .m(
                        ImmutableMap.of(
                            "city", AttributeValue.builder().s("Seattle").build(),
                            "zip", AttributeValue.builder().n("98101").build(),
                            "flag", AttributeValue.builder().bool(false).build()))
                    .build())
            .put(
                "items_list",
                AttributeValue.builder()
                    .l(
                        AttributeValue.builder().s("elem1").build(),
                        AttributeValue.builder().n("100").build(),
                        AttributeValue.builder().nul(true).build())
                    .build())
            .put("tags_ss", AttributeValue.builder().ss("tagA", "tagB").build())
            .put("scores_ns", AttributeValue.builder().ns("10", "20.5").build())
            .put(
                "blobs_bs",
                AttributeValue.builder().bs(SdkBytes.fromByteArray(new byte[] {1, 2})).build())
            .build();

    SourceRow row = rowMapper.mapRow(item);
    assertEquals("Users", row.tableName());
    assertEquals("user-1", row.getPayload().get("id"));
    assertEquals("42.5", row.getPayload().get("age"));
    assertEquals(ByteBuffer.wrap(sampleBytes), row.getPayload().get("raw_bytes"));
    assertEquals(true, row.getPayload().get("is_active"));
    assertNull(row.getPayload().get("null_col"));
    assertEquals(
        "{\"city\":\"Seattle\",\"zip\":98101,\"flag\":false}",
        row.getPayload().get("metadata_map"));
    assertEquals("[\"elem1\",100,null]", row.getPayload().get("items_list"));
    assertEquals(List.of("tagA", "tagB"), row.getPayload().get("tags_ss"));
    assertEquals(List.of("10", "20.5"), row.getPayload().get("scores_ns"));
    assertEquals(List.of(ByteBuffer.wrap(new byte[] {1, 2})), row.getPayload().get("blobs_bs"));
  }

  @Test
  public void testDynamoDbSchemaDiscovery() {
    DynamoDbClient mockClient = mock(DynamoDbClient.class);

    when(mockClient.listTables(any(ListTablesRequest.class)))
        .thenReturn(
            ListTablesResponse.builder()
                .tableNames("Table1")
                .lastEvaluatedTableName("Table1")
                .build())
        .thenReturn(
            ListTablesResponse.builder().tableNames("Table2").lastEvaluatedTableName(null).build());

    DynamoDbSchemaDiscovery discovery = new DynamoDbSchemaDiscovery();
    ImmutableList<String> tables = discovery.discoverTables(mockClient);
    assertThat(tables).containsExactly("Table1", "Table2").inOrder();

    when(mockClient.describeTable(any(DescribeTableRequest.class)))
        .thenReturn(
            DescribeTableResponse.builder()
                .table(
                    TableDescription.builder()
                        .tableName("Table1")
                        .attributeDefinitions(
                            AttributeDefinition.builder()
                                .attributeName("pk")
                                .attributeType(ScalarAttributeType.S)
                                .build(),
                            AttributeDefinition.builder()
                                .attributeName("sk")
                                .attributeType(ScalarAttributeType.N)
                                .build())
                        .build())
                .build());

    when(mockClient.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder()
                .items(
                    ImmutableMap.of(
                        "pk", AttributeValue.builder().s("p1").build(),
                        "sk", AttributeValue.builder().n("1").build(),
                        "opt_col", AttributeValue.builder().nul(true).build(),
                        "map_col",
                            AttributeValue.builder()
                                .m(Map.of("k", AttributeValue.builder().s("v").build()))
                                .build()),
                    ImmutableMap.of(
                        "pk", AttributeValue.builder().s("p2").build(),
                        "sk", AttributeValue.builder().n("2").build(),
                        "opt_col", AttributeValue.builder().bool(true).build()))
                .build());

    ImmutableMap<String, ImmutableMap<String, SourceColumnType>> schema =
        discovery.discoverTableSchema(mockClient, ImmutableList.of("Table1"));

    assertThat(schema).containsKey("Table1");
    ImmutableMap<String, SourceColumnType> table1Cols = schema.get("Table1");
    assertEquals("S", table1Cols.get("pk").getName());
    assertEquals("N", table1Cols.get("sk").getName());
    // Should upgrade from NULL in item 1 to BOOL in item 2
    assertEquals("BOOL", table1Cols.get("opt_col").getName());
    assertEquals("M", table1Cols.get("map_col").getName());
  }

  @Test
  public void testDynamoDbIoWrapperAndFactory() {
    DynamoDbConnectionConfig config = new DynamoDbConnectionConfig();
    config.setEndpoint("http://localhost:8000");
    config.setRegion("us-west-2");
    config.setAccessKey("testKey");
    config.setSecretKey("testSecret");
    config.setTotalSegments(2);

    DynamoDbIoWrapperFactory factory = DynamoDbIoWrapperFactory.fromConfig(config, 4);
    assertEquals(Integer.valueOf(4), factory.numPartitions());
    assertEquals(config, factory.dynamoDbConnectionConfig());

    assertThrows(
        IllegalArgumentException.class,
        () -> DynamoDbIoWrapperFactory.fromConfig(new AstraConnectionConfig(), 1));

    DynamoDbClient mockClient = mock(DynamoDbClient.class);
    when(mockClient.listTables(any(ListTablesRequest.class)))
        .thenReturn(ListTablesResponse.builder().tableNames("T1", "T2").build());
    when(mockClient.describeTable(any(DescribeTableRequest.class)))
        .thenReturn(
            DescribeTableResponse.builder()
                .table(
                    TableDescription.builder()
                        .tableName("T1")
                        .attributeDefinitions(
                            AttributeDefinition.builder()
                                .attributeName("id")
                                .attributeType(ScalarAttributeType.S)
                                .build())
                        .build())
                .build());
    when(mockClient.scan(any(ScanRequest.class)))
        .thenReturn(ScanResponse.builder().items(List.of()).build());

    DynamoDbIoWrapper wrapper =
        new DynamoDbIoWrapper(config, List.of("T1"), 4, mockClient, new DynamoDbSchemaDiscovery());

    ImmutableList<SourceSchema> schemas = wrapper.discoverTableSchema();
    assertThat(schemas).hasSize(1);
    assertThat(schemas.get(0).tableSchemas()).hasSize(1);
    assertEquals("T1", schemas.get(0).tableSchemas().get(0).tableName());
    assertThat(wrapper.getTableReaders()).hasSize(1);

    ClientConfiguration clientConfig = DynamoDbClientProvider.createBeamClientConfiguration(config);
    assertNotNull(clientConfig);
  }
}
