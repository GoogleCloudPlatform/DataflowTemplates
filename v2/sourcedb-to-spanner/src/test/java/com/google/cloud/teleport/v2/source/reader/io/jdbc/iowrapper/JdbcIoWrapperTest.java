/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.source.reader.auth.dbauth.LocalCredentialsProvider;
import com.google.cloud.teleport.v2.source.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.exception.SuitableIndexNotFoundException;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.DialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo.IndexType;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchema;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.SQLException;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link JdbcIoWrapper}. */
@RunWith(MockitoJUnitRunner.class)
public class JdbcIoWrapperTest {
  @Mock DialectAdapter mockDialectAdapter;

  @BeforeClass
  public static void beforeClass() {
    // by default, derby uses a lock timeout of 60 seconds. In order to speed up the test
    // and detect the lock faster, we decrease this timeout
    System.setProperty("derby.locks.waitTimeout", "2");
    System.setProperty("derby.stream.error.file", "build/derby.log");
  }

  @Before
  public void initDerby() throws SQLException, ClassNotFoundException {
    // Creating testDB database
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
  }

  @Test
  public void testJdbcIoWrapperBasic() throws RetriableSchemaDiscoveryException {
    SourceSchemaReference testSourceSchemaReference =
        SourceSchemaReference.builder().setDbName("testDB").build();
    String testCol = "ID";
    SourceColumnType testColType = new SourceColumnType("INTEGER", new Long[] {}, null);
    when(mockDialectAdapter.discoverTables(any(), any())).thenReturn(ImmutableList.of("testTable"));
    when(mockDialectAdapter.discoverTableIndexes(any(), any(), any()))
        .thenReturn(
            ImmutableMap.of(
                "testTable",
                ImmutableList.of(
                    SourceColumnIndexInfo.builder()
                        .setIndexType(IndexType.NUMERIC)
                        .setIndexName("PRIMARY")
                        .setIsPrimary(true)
                        .setCardinality(42L)
                        .setColumnName(testCol)
                        .setIsUnique(true)
                        .setOrdinalPosition(1)
                        .build())));
    when(mockDialectAdapter.discoverTableSchema(any(), any(), any()))
        .thenReturn(ImmutableMap.of("testTable", ImmutableMap.of(testCol, testColType)));
    JdbcIoWrapper jdbcIoWrapper =
        JdbcIoWrapper.of(
            JdbcIOWrapperConfig.builderWithMySqlDefaults()
                .setSourceDbURL("jdbc:derby://myhost/memory:TestingDB;create=true")
                .setSourceSchemaReference(testSourceSchemaReference)
                .setShardID("test")
                .setDbAuth(
                    LocalCredentialsProvider.builder()
                        .setUserName("testUser")
                        .setPassword("testPassword")
                        .build())
                .setJdbcDriverJars("")
                .setJdbcDriverClassName("org.apache.derby.jdbc.EmbeddedDriver")
                .setDialectAdapter(mockDialectAdapter)
                .build());
    SourceSchema sourceSchema = jdbcIoWrapper.discoverTableSchema();
    assertThat(sourceSchema.schemaReference()).isEqualTo(testSourceSchemaReference);
    assertThat(sourceSchema.tableSchemas().size()).isEqualTo(1);
    SourceTableSchema tableSchema = sourceSchema.tableSchemas().get(0);
    assertThat(tableSchema.tableName()).isEqualTo("testTable");
    assertThat(tableSchema.sourceColumnNameToSourceColumnType())
        .isEqualTo(ImmutableMap.of(testCol, testColType));
    ImmutableMap<SourceTableReference, PTransform<PBegin, PCollection<SourceRow>>> tableReaders =
        jdbcIoWrapper.getTableReaders();
    assertThat(tableReaders.size()).isEqualTo(1);
  }

  @Test
  public void testJdbcIoWrapperWithoutInference() throws RetriableSchemaDiscoveryException {
    SourceSchemaReference testSourceSchemaReference =
        SourceSchemaReference.builder().setDbName("testDB").build();
    String testCol = "ID";
    SourceColumnType testColType = new SourceColumnType("INTEGER", new Long[] {}, null);
    when(mockDialectAdapter.discoverTables(any(), any())).thenReturn(ImmutableList.of("testTable"));
    when(mockDialectAdapter.discoverTableIndexes(any(), any(), any()))
        .thenReturn(
            ImmutableMap.of(
                "testTable",
                ImmutableList.of(
                    SourceColumnIndexInfo.builder()
                        .setIndexType(IndexType.NUMERIC)
                        .setIndexName("PRIMARY")
                        .setIsPrimary(true)
                        .setCardinality(42L)
                        .setColumnName(testCol)
                        .setIsUnique(true)
                        .setOrdinalPosition(1)
                        .build())));
    when(mockDialectAdapter.discoverTableSchema(any(), any(), any()))
        .thenReturn(ImmutableMap.of("testTable", ImmutableMap.of(testCol, testColType)));
    JdbcIoWrapper jdbcIoWrapper =
        JdbcIoWrapper.of(
            JdbcIOWrapperConfig.builderWithMySqlDefaults()
                .setSourceDbURL("jdbc:derby://myhost/memory:TestingDB;create=true")
                .setSourceSchemaReference(testSourceSchemaReference)
                .setShardID("test")
                .setTableVsPartitionColumns(ImmutableMap.of("testTable", ImmutableList.of("ID")))
                .setDbAuth(
                    LocalCredentialsProvider.builder()
                        .setUserName("testUser")
                        .setPassword("testPassword")
                        .build())
                .setJdbcDriverJars("")
                .setJdbcDriverClassName("org.apache.derby.jdbc.EmbeddedDriver")
                .setDialectAdapter(mockDialectAdapter)
                .build());
    SourceSchema sourceSchema = jdbcIoWrapper.discoverTableSchema();
    assertThat(sourceSchema.schemaReference()).isEqualTo(testSourceSchemaReference);
    assertThat(sourceSchema.tableSchemas().size()).isEqualTo(1);
    SourceTableSchema tableSchema = sourceSchema.tableSchemas().get(0);
    assertThat(tableSchema.tableName()).isEqualTo("testTable");
    assertThat(tableSchema.sourceColumnNameToSourceColumnType())
        .isEqualTo(ImmutableMap.of(testCol, testColType));
    ImmutableMap<SourceTableReference, PTransform<PBegin, PCollection<SourceRow>>> tableReaders =
        jdbcIoWrapper.getTableReaders();
    assertThat(tableReaders.size()).isEqualTo(1);
  }

  @Test
  public void testJdbcIoWrapperNoIndexException() throws RetriableSchemaDiscoveryException {
    SourceSchemaReference testSourceSchemaReference =
        SourceSchemaReference.builder().setDbName("testDB").build();
    String testCol = "ID";
    SourceColumnType testColType = new SourceColumnType("INTEGER", new Long[] {}, null);
    when(mockDialectAdapter.discoverTables(any(), any())).thenReturn(ImmutableList.of("testTable"));
    when(mockDialectAdapter.discoverTableIndexes(any(), any(), any()))
        .thenReturn(ImmutableMap.of(/* No Index on testTable */ ))
        .thenReturn(
            ImmutableMap.of(
                /* No Numeric Index on testTable */
                "testTable",
                ImmutableList.of(
                    SourceColumnIndexInfo.builder()
                        .setIndexType(IndexType.OTHER)
                        .setIndexName("PRIMARY")
                        .setIsPrimary(true)
                        .setCardinality(42L)
                        .setColumnName(testCol)
                        .setIsUnique(true)
                        .setOrdinalPosition(1)
                        .build())));

    /* No Index on table */
    assertThrows(
        SuitableIndexNotFoundException.class,
        () ->
            JdbcIoWrapper.of(
                JdbcIOWrapperConfig.builderWithMySqlDefaults()
                    .setSourceDbURL("jdbc:derby://myhost/memory:TestingDB;create=true")
                    .setSourceSchemaReference(testSourceSchemaReference)
                    .setShardID("test")
                    .setDbAuth(
                        LocalCredentialsProvider.builder()
                            .setUserName("testUser")
                            .setPassword("testPassword")
                            .build())
                    .setJdbcDriverJars("")
                    .setJdbcDriverClassName("org.apache.derby.jdbc.EmbeddedDriver")
                    .setDialectAdapter(mockDialectAdapter)
                    .build()));
    /* No Numeric Index on table */
    assertThrows(
        SuitableIndexNotFoundException.class,
        () ->
            JdbcIoWrapper.of(
                JdbcIOWrapperConfig.builderWithMySqlDefaults()
                    .setSourceDbURL("jdbc:derby://myhost/memory:TestingDB;create=true")
                    .setSourceSchemaReference(testSourceSchemaReference)
                    .setShardID("test")
                    .setDbAuth(
                        LocalCredentialsProvider.builder()
                            .setUserName("testUser")
                            .setPassword("testPassword")
                            .build())
                    .setJdbcDriverJars("")
                    .setJdbcDriverClassName("org.apache.derby.jdbc.EmbeddedDriver")
                    .setDialectAdapter(mockDialectAdapter)
                    .build()));
  }

  @Test
  public void testGetTablesToMigrate() {
    ImmutableList<String> tablesToMigrate =
        JdbcIoWrapper.getTablesToMigrate(ImmutableList.of("a", "b"), ImmutableList.of("a"));
    assertEquals(1, tablesToMigrate.size());
    assertTrue(tablesToMigrate.contains("a"));

    ImmutableList<String> tablesToMigrate2 =
        JdbcIoWrapper.getTablesToMigrate(ImmutableList.of(), ImmutableList.of("x", "y"));
    assertEquals(2, tablesToMigrate2.size());
    assertTrue(tablesToMigrate2.contains("x"));
    assertTrue(tablesToMigrate2.contains("y"));

    ImmutableList<String> tablesToMigrate3 =
        JdbcIoWrapper.getTablesToMigrate(
            ImmutableList.of("p", "q", "r"), ImmutableList.of("p", "q", "r"));
    assertEquals(3, tablesToMigrate3.size());
    assertTrue(tablesToMigrate3.contains("p"));
    assertTrue(tablesToMigrate3.contains("q"));
    assertTrue(tablesToMigrate3.contains("r"));
  }
}
