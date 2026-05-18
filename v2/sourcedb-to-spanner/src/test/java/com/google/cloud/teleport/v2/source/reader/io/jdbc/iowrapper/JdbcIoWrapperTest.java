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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.source.reader.auth.dbauth.LocalCredentialsProvider;
import com.google.cloud.teleport.v2.source.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.exception.SuitableIndexNotFoundException;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.DialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.JdbcIoWrapperConfigGroup;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.TableConfig;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.DataSourceProvider;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableIdentifier;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableReadSpecification;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableSplitSpecification;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms.ReadWithUniformPartitions;
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
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link JdbcIoWrapper}. */
@RunWith(MockitoJUnitRunner.class)
public class JdbcIoWrapperTest {
  @Mock DialectAdapter mockDialectAdapter;

  @Mock BasicDataSource mockBasicDataSource;

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
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("testDB").build());
    String testCol = "ID";
    SourceColumnType testColType = new SourceColumnType("INTEGER", new Long[] {}, null);
    when(mockDialectAdapter.discoverTables(any(), (SourceSchemaReference) any()))
        .thenReturn(ImmutableList.of("testTable"));
    when(mockDialectAdapter.discoverTableIndexes(any(), (SourceSchemaReference) any(), any()))
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
    when(mockDialectAdapter.discoverTableSchema(any(), (SourceSchemaReference) any(), any()))
        .thenReturn(ImmutableMap.of("testTable", ImmutableMap.of(testCol, testColType)));
    JdbcIoWrapper jdbcIoWrapper =
        JdbcIoWrapper.of(
            JdbcIoWrapperConfigGroup.builder()
                .addShardConfig(
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
                        .build())
                .build());
    SourceSchema sourceSchema = jdbcIoWrapper.discoverTableSchema().get(0);
    assertThat(sourceSchema.schemaReference()).isEqualTo(testSourceSchemaReference);
    assertThat(sourceSchema.tableSchemas().size()).isEqualTo(1);
    SourceTableSchema tableSchema = sourceSchema.tableSchemas().get(0);
    assertThat(tableSchema.tableName()).isEqualTo("testTable");
    assertThat(tableSchema.sourceColumnNameToSourceColumnType())
        .isEqualTo(ImmutableMap.of(testCol, testColType));
    ImmutableMap<ImmutableList<SourceTableReference>, PTransform<PBegin, PCollection<SourceRow>>>
        tableReaders = jdbcIoWrapper.getTableReaders();
    assertThat(tableReaders.size()).isEqualTo(1);
  }

  @Test
  public void testJdbcIoWrapperWithoutInference() throws RetriableSchemaDiscoveryException {
    SourceSchemaReference testSourceSchemaReference =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("testDB").build());
    String testCol = "ID";
    SourceColumnType testColType = new SourceColumnType("INTEGER", new Long[] {}, null);
    when(mockDialectAdapter.discoverTables(any(), (SourceSchemaReference) any()))
        .thenReturn(ImmutableList.of("testTable"));
    when(mockDialectAdapter.discoverTableIndexes(any(), (SourceSchemaReference) any(), any()))
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
    when(mockDialectAdapter.discoverTableSchema(any(), (SourceSchemaReference) any(), any()))
        .thenReturn(ImmutableMap.of("testTable", ImmutableMap.of(testCol, testColType)));
    JdbcIoWrapper jdbcIoWrapper =
        JdbcIoWrapper.of(
            JdbcIoWrapperConfigGroup.builder()
                .addShardConfig(
                    JdbcIOWrapperConfig.builderWithMySqlDefaults()
                        .setSourceDbURL("jdbc:derby://myhost/memory:TestingDB;create=true")
                        .setSourceSchemaReference(testSourceSchemaReference)
                        .setShardID("test")
                        .setTableVsPartitionColumns(
                            ImmutableMap.of("testTable", ImmutableList.of("ID")))
                        .setDbAuth(
                            LocalCredentialsProvider.builder()
                                .setUserName("testUser")
                                .setPassword("testPassword")
                                .build())
                        .setJdbcDriverJars("")
                        .setJdbcDriverClassName("org.apache.derby.jdbc.EmbeddedDriver")
                        .setDialectAdapter(mockDialectAdapter)
                        .build())
                .build());
    SourceSchema sourceSchema = jdbcIoWrapper.discoverTableSchema().get(0);
    assertThat(sourceSchema.schemaReference()).isEqualTo(testSourceSchemaReference);
    assertThat(sourceSchema.tableSchemas().size()).isEqualTo(1);
    SourceTableSchema tableSchema = sourceSchema.tableSchemas().get(0);
    assertThat(tableSchema.tableName()).isEqualTo("testTable");
    assertThat(tableSchema.sourceColumnNameToSourceColumnType())
        .isEqualTo(ImmutableMap.of(testCol, testColType));
    assertThat(tableSchema.primaryKeyColumns()).isEqualTo(ImmutableList.of(testCol));
    ImmutableMap<ImmutableList<SourceTableReference>, PTransform<PBegin, PCollection<SourceRow>>>
        tableReaders = jdbcIoWrapper.getTableReaders();
    assertThat(tableReaders.size()).isEqualTo(1);
  }

  @Test
  public void testJdbcIoWrapperNoPrimaryKeyExistsOnTable()
      throws RetriableSchemaDiscoveryException {
    SourceSchemaReference testSourceSchemaReference =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("testDB").build());
    String testCol = "ID";
    SourceColumnType testColType = new SourceColumnType("INTEGER", new Long[] {}, null);
    when(mockDialectAdapter.discoverTables(any(), (SourceSchemaReference) any()))
        .thenReturn(ImmutableList.of("testTable"));
    when(mockDialectAdapter.discoverTableIndexes(any(), (SourceSchemaReference) any(), any()))
        .thenReturn(
            ImmutableMap.of(
                "testTable",
                ImmutableList.of(
                    SourceColumnIndexInfo.builder()
                        .setIndexType(IndexType.NUMERIC)
                        .setIndexName("SECONDARY")
                        .setIsPrimary(false)
                        .setCardinality(42L)
                        .setColumnName(testCol)
                        .setIsUnique(true)
                        .setOrdinalPosition(2)
                        .build())));
    lenient()
        .when(mockDialectAdapter.discoverTableSchema(any(), (SourceSchemaReference) any(), any()))
        .thenReturn(ImmutableMap.of("testTable", ImmutableMap.of(testCol, testColType)));

    assertThrows(
        SuitableIndexNotFoundException.class,
        () ->
            JdbcIoWrapper.of(
                JdbcIoWrapperConfigGroup.builder()
                    .addShardConfig(
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
                            .build())
                    .build()));
  }

  @Test
  public void testJdbcIoWrapperNoIndexException() throws RetriableSchemaDiscoveryException {
    SourceSchemaReference testSourceSchemaReference =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("testDB").build());
    String testCol = "ID";
    SourceColumnType testColType = new SourceColumnType("INTEGER", new Long[] {}, null);
    when(mockDialectAdapter.discoverTables(any(), (SourceSchemaReference) any()))
        .thenReturn(ImmutableList.of("testTable"));
    when(mockDialectAdapter.discoverTableIndexes(any(), (SourceSchemaReference) any(), any()))
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
                JdbcIoWrapperConfigGroup.builder()
                    .addShardConfig(
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
                            .build())
                    .build()));
    /* No Numeric Index on table */
    assertThrows(
        SuitableIndexNotFoundException.class,
        () ->
            JdbcIoWrapper.of(
                JdbcIoWrapperConfigGroup.builder()
                    .addShardConfig(
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
                            .build())
                    .build()));
  }

  @Test
  public void testJdbcIoWrapperDifferentTables() throws RetriableSchemaDiscoveryException {
    // Test to check what happens if config passes tables not present in source
    SourceSchemaReference testSourceSchemaReference =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("testDB").build());
    String testCol = "ID";
    SourceColumnType testColType = new SourceColumnType("INTEGER", new Long[] {}, null);
    when(mockDialectAdapter.discoverTables(any(), (SourceSchemaReference) any()))
        .thenReturn(ImmutableList.of("testTable"));
    lenient()
        .when(mockDialectAdapter.discoverTableIndexes(any(), (SourceSchemaReference) any(), any()))
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
    lenient()
        .when(mockDialectAdapter.discoverTableSchema(any(), (SourceSchemaReference) any(), any()))
        .thenReturn(ImmutableMap.of("testTable", ImmutableMap.of(testCol, testColType)));
    JdbcIoWrapper jdbcIoWrapper =
        JdbcIoWrapper.of(
            JdbcIoWrapperConfigGroup.builder()
                .addShardConfig(
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
                        .setTables(ImmutableList.of("spanner_table"))
                        .build())
                .build());
    ImmutableMap<ImmutableList<SourceTableReference>, PTransform<PBegin, PCollection<SourceRow>>>
        tableReaders = jdbcIoWrapper.getTableReaders();
    assertThat(tableReaders.size()).isEqualTo(0);
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

    // To handle scenarios where the configured tables are different from the tables at source
    ImmutableList<String> tablesToMigrateDiffTables =
        JdbcIoWrapper.getTablesToMigrate(ImmutableList.of("a"), ImmutableList.of("x", "y"));
    assertEquals(0, tablesToMigrateDiffTables.size());
  }

  @Test
  public void testReadWithUniformPartitionFeatureFlag() throws RetriableSchemaDiscoveryException {

    String testCol = "ID";
    SourceColumnType testColType = new SourceColumnType("INTEGER", new Long[] {}, null);
    when(mockDialectAdapter.discoverTables(any(), (SourceSchemaReference) any()))
        .thenReturn(ImmutableList.of("testTable"));
    when(mockDialectAdapter.discoverTableIndexes(any(), (SourceSchemaReference) any(), any()))
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
    when(mockDialectAdapter.discoverTableSchema(any(), (SourceSchemaReference) any(), any()))
        .thenReturn(ImmutableMap.of("testTable", ImmutableMap.of(testCol, testColType)));

    SourceSchemaReference testSourceSchemaReference =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("testDB").build());

    JdbcIOWrapperConfig configWithFeatureEnabled =
        JdbcIOWrapperConfig.builderWithMySqlDefaults()
            .setSourceDbURL("jdbc:derby://myhost/memory:TestingDB;create=true")
            .setSourceSchemaReference(testSourceSchemaReference)
            .setShardID("test")
            .setTableVsPartitionColumns(ImmutableMap.of("testTable", ImmutableList.of("ID")))
            .setReadWithUniformPartitionsFeatureEnabled(true)
            .setDbAuth(
                LocalCredentialsProvider.builder()
                    .setUserName("testUser")
                    .setPassword("testPassword")
                    .build())
            .setJdbcDriverJars("")
            .setJdbcDriverClassName("org.apache.derby.jdbc.EmbeddedDriver")
            .setDialectAdapter(mockDialectAdapter)
            .build();
    JdbcIOWrapperConfig configWithFeatureDisabled =
        configWithFeatureEnabled.toBuilder()
            .setReadWithUniformPartitionsFeatureEnabled(false)
            .build();
    JdbcIoWrapper jdbcIOWrapperWithFeatureEnabled =
        JdbcIoWrapper.of(
            JdbcIoWrapperConfigGroup.builder().addShardConfig(configWithFeatureEnabled).build());
    JdbcIoWrapper jdbcIOWrapperWithFeatureDisabled =
        JdbcIoWrapper.of(
            JdbcIoWrapperConfigGroup.builder().addShardConfig(configWithFeatureDisabled).build());
    assertThat(
            jdbcIOWrapperWithFeatureDisabled.getTableReaders().values().stream().findFirst().get())
        .isInstanceOf(JdbcIO.ReadWithPartitions.class);
    assertThat(
            jdbcIOWrapperWithFeatureEnabled.getTableReaders().values().stream().findFirst().get())
        .isInstanceOf(ReadWithUniformPartitions.class);
    // We test that setting the fetch size works for both modes. The more detailed testing of the
    // fetch size getting applied to JdbcIO is covered in {@link ReadWithUniformPartitionTest}
    assertThat(
            JdbcIoWrapper.of(
                    JdbcIoWrapperConfigGroup.builder()
                        .addShardConfig(
                            configWithFeatureEnabled.toBuilder().setMaxFetchSize(42).build())
                        .build())
                .getTableReaders())
        .hasSize(1);
    assertThat(
            JdbcIoWrapper.of(
                    JdbcIoWrapperConfigGroup.builder()
                        .addShardConfig(
                            configWithFeatureDisabled.toBuilder().setMaxFetchSize(42).build())
                        .build())
                .getTableReaders())
        .hasSize(1);
  }

  @Test
  public void testReadWithUniformPartitionMultiTable() throws RetriableSchemaDiscoveryException {
    SourceSchemaReference testSourceSchemaReference =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("testDB").build());
    String testCol = "ID";
    SourceColumnType testColType = new SourceColumnType("INTEGER", new Long[] {}, null);

    when(mockDialectAdapter.discoverTables(any(), (SourceSchemaReference) any()))
        .thenReturn(ImmutableList.of("table1", "table2"));
    when(mockDialectAdapter.discoverTableIndexes(any(), (SourceSchemaReference) any(), any()))
        .thenReturn(
            ImmutableMap.of(
                "table1",
                ImmutableList.of(
                    SourceColumnIndexInfo.builder()
                        .setIndexType(IndexType.NUMERIC)
                        .setIndexName("PRIMARY")
                        .setIsPrimary(true)
                        .setCardinality(42L)
                        .setColumnName(testCol)
                        .setIsUnique(true)
                        .setOrdinalPosition(1)
                        .build()),
                "table2",
                ImmutableList.of(
                    SourceColumnIndexInfo.builder()
                        .setIndexType(IndexType.NUMERIC)
                        .setIndexName("PRIMARY")
                        .setIsPrimary(true)
                        .setCardinality(100L)
                        .setColumnName(testCol)
                        .setIsUnique(true)
                        .setOrdinalPosition(1)
                        .build())));
    when(mockDialectAdapter.discoverTableSchema(any(), (SourceSchemaReference) any(), any()))
        .thenReturn(
            ImmutableMap.of(
                "table1", ImmutableMap.of(testCol, testColType),
                "table2", ImmutableMap.of(testCol, testColType)));

    JdbcIOWrapperConfig config =
        JdbcIOWrapperConfig.builderWithMySqlDefaults()
            .setSourceDbURL("jdbc:derby://myhost/memory:TestingDB;create=true")
            .setSourceSchemaReference(testSourceSchemaReference)
            .setShardID("test")
            .setReadWithUniformPartitionsFeatureEnabled(true)
            .setDbAuth(
                LocalCredentialsProvider.builder()
                    .setUserName("testUser")
                    .setPassword("testPassword")
                    .build())
            .setJdbcDriverJars("")
            .setJdbcDriverClassName("org.apache.derby.jdbc.EmbeddedDriver")
            .setDialectAdapter(mockDialectAdapter)
            .setTables(ImmutableList.of("table1", "table2"))
            .build();

    JdbcIoWrapper jdbcIoWrapper =
        JdbcIoWrapper.of(JdbcIoWrapperConfigGroup.builder().addShardConfig(config).build());
    ImmutableMap<ImmutableList<SourceTableReference>, PTransform<PBegin, PCollection<SourceRow>>>
        tableReaders = jdbcIoWrapper.getTableReaders();

    assertThat(tableReaders).hasSize(1);
    ImmutableList<SourceTableReference> key = tableReaders.keySet().iterator().next();
    assertThat(key).hasSize(2);
    assertThat(key.stream().map(SourceTableReference::sourceTableName).collect(Collectors.toList()))
        .containsExactly("\"table1\"", "\"table2\"");
    assertThat(tableReaders.values().iterator().next())
        .isInstanceOf(ReadWithUniformPartitions.class);
  }

  @Test
  public void testReadWithUniformPartitionConfigOverrides()
      throws RetriableSchemaDiscoveryException {
    SourceSchemaReference testSourceSchemaReference =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("testDB").build());
    String testCol = "ID";
    SourceColumnType testColType = new SourceColumnType("INTEGER", new Long[] {}, null);

    when(mockDialectAdapter.discoverTables(any(), (SourceSchemaReference) any()))
        .thenReturn(ImmutableList.of("table1"));
    when(mockDialectAdapter.discoverTableIndexes(any(), (SourceSchemaReference) any(), any()))
        .thenReturn(
            ImmutableMap.of(
                "table1",
                ImmutableList.of(
                    SourceColumnIndexInfo.builder()
                        .setIndexType(IndexType.NUMERIC)
                        .setIndexName("PRIMARY")
                        .setIsPrimary(true)
                        .setCardinality(100L)
                        .setColumnName(testCol)
                        .setIsUnique(true)
                        .setOrdinalPosition(1)
                        .build())));
    when(mockDialectAdapter.discoverTableSchema(any(), (SourceSchemaReference) any(), any()))
        .thenReturn(ImmutableMap.of("table1", ImmutableMap.of(testCol, testColType)));

    JdbcIOWrapperConfig config =
        JdbcIOWrapperConfig.builderWithMySqlDefaults()
            .setSourceDbURL("jdbc:derby://myhost/memory:TestingDB;create=true")
            .setSourceSchemaReference(testSourceSchemaReference)
            .setShardID("test")
            .setReadWithUniformPartitionsFeatureEnabled(true)
            .setMaxPartitions(10) // Triggers line 487 in getMultiTableReadWithUniformPartitionIO
            .setSplitStageCountHint(
                5L) // Triggers line 492 in getMultiTableReadWithUniformPartitionIO
            .setDbAuth(
                LocalCredentialsProvider.builder()
                    .setUserName("testUser")
                    .setPassword("testPassword")
                    .build())
            .setJdbcDriverJars("")
            .setJdbcDriverClassName("org.apache.derby.jdbc.EmbeddedDriver")
            .setDialectAdapter(mockDialectAdapter)
            .build();

    JdbcIoWrapper jdbcIoWrapper =
        JdbcIoWrapper.of(JdbcIoWrapperConfigGroup.builder().addShardConfig(config).build());
    assertThat(jdbcIoWrapper.getTableReaders()).hasSize(1);
  }

  @Test
  public void testGetJdbcIOWithMaxPartitions() throws RetriableSchemaDiscoveryException {
    SourceSchemaReference testSourceSchemaReference =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("testDB").build());
    String testCol = "ID";
    SourceColumnType testColType = new SourceColumnType("INTEGER", new Long[] {}, null);

    when(mockDialectAdapter.discoverTables(any(), (SourceSchemaReference) any()))
        .thenReturn(ImmutableList.of("testTable"));
    when(mockDialectAdapter.discoverTableIndexes(any(), (SourceSchemaReference) any(), any()))
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
    when(mockDialectAdapter.discoverTableSchema(any(), (SourceSchemaReference) any(), any()))
        .thenReturn(ImmutableMap.of("testTable", ImmutableMap.of(testCol, testColType)));

    JdbcIOWrapperConfig config =
        JdbcIOWrapperConfig.builderWithMySqlDefaults()
            .setSourceDbURL("jdbc:derby://myhost/memory:TestingDB;create=true")
            .setSourceSchemaReference(testSourceSchemaReference)
            .setShardID("test")
            .setReadWithUniformPartitionsFeatureEnabled(false)
            .setMaxPartitions(10) // Triggers line 439 in getJdbcIO
            .setDbAuth(
                LocalCredentialsProvider.builder()
                    .setUserName("testUser")
                    .setPassword("testPassword")
                    .build())
            .setJdbcDriverJars("")
            .setJdbcDriverClassName("org.apache.derby.jdbc.EmbeddedDriver")
            .setDialectAdapter(mockDialectAdapter)
            .build();

    JdbcIoWrapper jdbcIoWrapper =
        JdbcIoWrapper.of(JdbcIoWrapperConfigGroup.builder().addShardConfig(config).build());
    assertThat(jdbcIoWrapper.getTableReaders()).hasSize(1);
    assertThat(jdbcIoWrapper.getTableReaders().values().iterator().next())
        .isInstanceOf(JdbcIO.ReadWithPartitions.class);
  }

  @Test
  public void testLoginTimeout() throws RetriableSchemaDiscoveryException {

    int testLoginTimeoutMilliseconds = 1000;
    int testLoginTimeoutSeconds = 1;

    doNothing().when(mockBasicDataSource).setMaxWaitMillis(testLoginTimeoutMilliseconds);
    when(mockBasicDataSource.getUrl())
        .thenReturn("jdbc://testIp:3306/testDB")
        .thenReturn("jdbc://testIp:3306/testDB")
        .thenReturn("jdbc://testIp:3306/testDB?connectTimeout=2000&socketTimeout=2000")
        .thenReturn("jdbc://testIp:3306/testDB?connectTimeout=2000&socketTimeout=2000")
        .thenReturn("jdbc://testIp:3306/testDB?connectTimeout=2000&socketTimeout=2000");
    doNothing()
        .when(mockBasicDataSource)
        .addConnectionProperty("connectTimeout", String.valueOf(testLoginTimeoutMilliseconds));
    doNothing()
        .when(mockBasicDataSource)
        .addConnectionProperty("socketTimeout", String.valueOf(testLoginTimeoutMilliseconds));
    doNothing()
        .when(mockBasicDataSource)
        .addConnectionProperty("loginTimeout", String.valueOf(testLoginTimeoutSeconds));

    SourceSchemaReference testSourceSchemaReference =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("testDB").build());

    JdbcIOWrapperConfig config =
        JdbcIOWrapperConfig.builderWithMySqlDefaults()
            .setSourceDbURL("jdbc:derby://myhost/memory:TestingDB;create=true")
            .setSourceSchemaReference(testSourceSchemaReference)
            .setSchemaDiscoveryConnectivityTimeoutMilliSeconds(testLoginTimeoutMilliseconds)
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
            .build();
    JdbcIOWrapperConfig configWithTimeoutSet =
        config.toBuilder()
            .setSourceDbDialect(SQLDialect.MYSQL)
            .setSchemaDiscoveryConnectivityTimeoutMilliSeconds(testLoginTimeoutMilliseconds)
            .build();
    JdbcIOWrapperConfig configWithUrlTimeout =
        config.toBuilder()
            .setSourceDbDialect(SQLDialect.POSTGRESQL)
            .setSourceDbURL(
                "jdbc:derby://myhost/memory:TestingDB;create=true?socketTimeout=10&connectTimeout=10")
            .setSchemaDiscoveryConnectivityTimeoutMilliSeconds(testLoginTimeoutMilliseconds)
            .build();

    JdbcIoWrapper.setDataSourceLoginTimeout(mockBasicDataSource, configWithTimeoutSet);
    JdbcIoWrapper.setDataSourceLoginTimeout(mockBasicDataSource, configWithUrlTimeout);

    assertThat(configWithTimeoutSet.schemaDiscoveryConnectivityTimeoutMilliSeconds())
        .isEqualTo(testLoginTimeoutMilliseconds);
    verify(mockBasicDataSource, times(2)).setMaxWaitMillis(testLoginTimeoutMilliseconds);
    verify(mockBasicDataSource, times(1))
        .addConnectionProperty("connectTimeout", String.valueOf(testLoginTimeoutMilliseconds));
    verify(mockBasicDataSource, times(1))
        .addConnectionProperty("socketTimeout", String.valueOf(testLoginTimeoutMilliseconds));
    verify(mockBasicDataSource, times(1))
        .addConnectionProperty("loginTimeout", String.valueOf(testLoginTimeoutSeconds));
  }

  @Test
  public void testIndexTypeToColumnClass() {

    assertThat(
            JdbcIoWrapper.indexTypeToColumnClass(
                SourceColumnIndexInfo.builder()
                    .setColumnName("col1")
                    .setIndexType(IndexType.BIG_INT_UNSIGNED)
                    .setOrdinalPosition(1)
                    .setIndexName("PRIMARY")
                    .setIsPrimary(true)
                    .setCardinality(42L)
                    .setIsUnique(true)
                    .build()))
        .isEqualTo(BigDecimal.class);
    assertThrows(
        SuitableIndexNotFoundException.class,
        () ->
            JdbcIoWrapper.indexTypeToColumnClass(
                SourceColumnIndexInfo.builder()
                    .setColumnName("col1")
                    .setIndexType(IndexType.OTHER)
                    .setOrdinalPosition(1)
                    .setIndexName("PRIMARY")
                    .setIsPrimary(true)
                    .setCardinality(42L)
                    .setIsUnique(true)
                    .build()));
  }

  @Test
  public void testIdentifierEscaping() {
    assertThat(JdbcIoWrapper.delimitIdentifier("key")).isEqualTo("\"key\"");
    assertThat(JdbcIoWrapper.delimitIdentifier("ke\"y")).isEqualTo("\"ke\"\"y\"");
  }

  @Test
  public void testIdGeneration() {
    assertThat(JdbcIOWrapperConfig.generateId()).isNotEqualTo(JdbcIOWrapperConfig.generateId());
  }

  @Test
  public void testGetTableConfig() {

    SourceSchemaReference testSourceSchemaReference =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("testDB").build());

    JdbcIOWrapperConfig config =
        JdbcIOWrapperConfig.builderWithMySqlDefaults()
            .setSourceDbURL("jdbc:derby://myhost/memory:TestingDB;create=true")
            .setSourceSchemaReference(testSourceSchemaReference)
            .setShardID("test")
            .setTableVsPartitionColumns(ImmutableMap.of("testTable", ImmutableList.of("id")))
            .setDbAuth(
                LocalCredentialsProvider.builder()
                    .setUserName("testUser")
                    .setPassword("testPassword")
                    .build())
            .setMaxPartitions(10)
            .setMaxFetchSize(1000)
            .setJdbcDriverJars("")
            .setJdbcDriverClassName("org.apache.derby.jdbc.EmbeddedDriver")
            .setDialectAdapter(mockDialectAdapter)
            .build();
    String tableName = "testTable";
    SourceColumnIndexInfo indexInfo =
        SourceColumnIndexInfo.builder()
            .setColumnName("id")
            .setIndexType(IndexType.NUMERIC)
            .setIndexName("PRIMARY")
            .setIsPrimary(true)
            .setCardinality(100L)
            .setIsUnique(true)
            .setOrdinalPosition(1)
            .build();
    ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> indexInfoMap =
        ImmutableMap.of(tableName, ImmutableList.of(indexInfo));

    TableConfig tableConfig = JdbcIoWrapper.getTableConfig(tableName, config, indexInfoMap);

    assertThat(tableConfig.tableName()).isEqualTo(tableName);
    assertThat(tableConfig.dataSourceId()).isEqualTo(config.id());
    assertThat(tableConfig.approxRowCount()).isEqualTo(100L);
    assertThat(tableConfig.maxPartitions()).isEqualTo(10);
    assertThat(tableConfig.fetchSize()).isEqualTo(1000);
    assertThat(tableConfig.partitionColumns()).hasSize(1);
    assertThat(tableConfig.partitionColumns().get(0).columnName()).isEqualTo("\"id\"");
  }

  @Test
  public void testGetTableIdentifier() {
    TableConfig tableConfig =
        TableConfig.builder("testTable").setDataSourceId("testDataSource").build();

    TableIdentifier tableIdentifier = JdbcIoWrapper.getTableIdentifier(tableConfig);

    assertThat(tableIdentifier.tableName()).isEqualTo("\"testTable\"");
    assertThat(tableIdentifier.dataSourceId()).isEqualTo("testDataSource");
  }

  @Test
  public void testBuildTableReaders() {
    SourceSchemaReference schemaRef =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("testDB").build());
    String colName = "ID";
    SourceColumnType colType = new SourceColumnType("INTEGER", new Long[] {}, null);

    // 1. Legacy Source 1: 0 table configs
    JdbcIOWrapperConfig legacyConfig1 =
        JdbcIOWrapperConfig.builderWithMySqlDefaults()
            .setSourceDbURL("jdbc:mysql://localhost/test")
            .setDbAuth(
                LocalCredentialsProvider.builder().setUserName("user").setPassword("pass").build())
            .setJdbcDriverJars("")
            .setJdbcDriverClassName("com.mysql.cj.jdbc.Driver")
            .setReadWithUniformPartitionsFeatureEnabled(false)
            .setSourceSchemaReference(schemaRef)
            .build();
    JdbcIoWrapper.PerSourceDiscovery legacyDiscovery1 =
        JdbcIoWrapper.PerSourceDiscovery.builder()
            .setConfig(legacyConfig1)
            .setDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("driver", "url"))
            .setTableConfigs(ImmutableList.of())
            .setSourceSchema(SourceSchema.builder().setSchemaReference(schemaRef).build())
            .build();

    // 2. Legacy Source 2: 1 table config
    JdbcIOWrapperConfig legacyConfig2 =
        legacyConfig1.toBuilder()
            .setId(JdbcIOWrapperConfig.generateId())
            .setShardID("shard2")
            .build();
    TableConfig tableConfig2 =
        TableConfig.builder("t2")
            .setDataSourceId(legacyConfig2.id())
            .withPartitionColum(
                PartitionColumn.builder().setColumnName("ID").setColumnClass(Long.class).build())
            .build();
    SourceTableSchema tableSchema2 =
        SourceTableSchema.builder(SQLDialect.MYSQL)
            .setTableName("t2")
            .setEstimatedRowSize(100L)
            .addSourceColumnNameToSourceColumnType(colName, colType)
            .build();
    JdbcIoWrapper.PerSourceDiscovery legacyDiscovery2 =
        JdbcIoWrapper.PerSourceDiscovery.builder()
            .setConfig(legacyConfig2)
            .setDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("driver", "url"))
            .setTableConfigs(ImmutableList.of(tableConfig2))
            .setSourceSchema(
                SourceSchema.builder()
                    .setSchemaReference(schemaRef)
                    .addTableSchema(tableSchema2)
                    .build())
            .build();

    // 3. Legacy Source 3: 2 table configs
    JdbcIOWrapperConfig legacyConfig3 =
        legacyConfig1.toBuilder()
            .setId(JdbcIOWrapperConfig.generateId())
            .setShardID("shard3")
            .build();
    TableConfig tableConfig3a =
        TableConfig.builder("t3a")
            .setDataSourceId(legacyConfig3.id())
            .withPartitionColum(
                PartitionColumn.builder().setColumnName("ID").setColumnClass(Long.class).build())
            .build();
    TableConfig tableConfig3b =
        TableConfig.builder("t3b")
            .setDataSourceId(legacyConfig3.id())
            .withPartitionColum(
                PartitionColumn.builder().setColumnName("ID").setColumnClass(Long.class).build())
            .build();
    SourceTableSchema tableSchema3a =
        SourceTableSchema.builder(SQLDialect.MYSQL)
            .setTableName("t3a")
            .setEstimatedRowSize(100L)
            .addSourceColumnNameToSourceColumnType(colName, colType)
            .build();
    SourceTableSchema tableSchema3b =
        SourceTableSchema.builder(SQLDialect.MYSQL)
            .setTableName("t3b")
            .setEstimatedRowSize(100L)
            .addSourceColumnNameToSourceColumnType(colName, colType)
            .build();
    JdbcIoWrapper.PerSourceDiscovery legacyDiscovery3 =
        JdbcIoWrapper.PerSourceDiscovery.builder()
            .setConfig(legacyConfig3)
            .setDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("driver", "url"))
            .setTableConfigs(ImmutableList.of(tableConfig3a, tableConfig3b))
            .setSourceSchema(
                SourceSchema.builder()
                    .setSchemaReference(schemaRef)
                    .addTableSchema(tableSchema3a)
                    .addTableSchema(tableSchema3b)
                    .build())
            .build();

    // 4. Uniform Source 4: 0 table configs
    JdbcIOWrapperConfig uniformConfig4 =
        legacyConfig1.toBuilder()
            .setId(JdbcIOWrapperConfig.generateId())
            .setReadWithUniformPartitionsFeatureEnabled(true)
            .build();
    JdbcIoWrapper.PerSourceDiscovery uniformDiscovery4 =
        JdbcIoWrapper.PerSourceDiscovery.builder()
            .setConfig(uniformConfig4)
            .setDataSourceConfiguration(legacyDiscovery1.dataSourceConfiguration())
            .setTableConfigs(legacyDiscovery1.tableConfigs())
            .setSourceSchema(legacyDiscovery1.sourceSchema())
            .build();

    // 5. Uniform Source 5: 1 table config
    JdbcIOWrapperConfig uniformConfig5 =
        uniformConfig4.toBuilder()
            .setId(JdbcIOWrapperConfig.generateId())
            .setShardID("shard5")
            .build();
    TableConfig tableConfig5 =
        TableConfig.builder("t5").setDataSourceId(uniformConfig5.id()).build();
    SourceTableSchema tableSchema5 =
        SourceTableSchema.builder(SQLDialect.MYSQL)
            .setTableName("t5")
            .setEstimatedRowSize(100L)
            .addSourceColumnNameToSourceColumnType(colName, colType)
            .build();
    JdbcIoWrapper.PerSourceDiscovery uniformDiscovery5 =
        JdbcIoWrapper.PerSourceDiscovery.builder()
            .setConfig(uniformConfig5)
            .setDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("driver", "url"))
            .setTableConfigs(ImmutableList.of(tableConfig5))
            .setSourceSchema(
                SourceSchema.builder()
                    .setSchemaReference(schemaRef)
                    .addTableSchema(tableSchema5)
                    .build())
            .build();

    // 6. Uniform Source 6: 2 table configs
    JdbcIOWrapperConfig uniformConfig6 =
        uniformConfig4.toBuilder()
            .setId(JdbcIOWrapperConfig.generateId())
            .setShardID("shard6")
            .build();
    TableConfig tableConfig6a =
        TableConfig.builder("t6a").setDataSourceId(uniformConfig6.id()).build();
    TableConfig tableConfig6b =
        TableConfig.builder("t6b").setDataSourceId(uniformConfig6.id()).build();
    SourceTableSchema tableSchema6a =
        SourceTableSchema.builder(SQLDialect.MYSQL)
            .setTableName("t6a")
            .setEstimatedRowSize(100L)
            .addSourceColumnNameToSourceColumnType(colName, colType)
            .build();
    SourceTableSchema tableSchema6b =
        SourceTableSchema.builder(SQLDialect.MYSQL)
            .setTableName("t6b")
            .setEstimatedRowSize(100L)
            .addSourceColumnNameToSourceColumnType(colName, colType)
            .build();
    JdbcIoWrapper.PerSourceDiscovery uniformDiscovery6 =
        JdbcIoWrapper.PerSourceDiscovery.builder()
            .setConfig(uniformConfig6)
            .setDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("driver", "url"))
            .setTableConfigs(ImmutableList.of(tableConfig6a, tableConfig6b))
            .setSourceSchema(
                SourceSchema.builder()
                    .setSchemaReference(schemaRef)
                    .addTableSchema(tableSchema6a)
                    .addTableSchema(tableSchema6b)
                    .build())
            .build();

    ImmutableList<JdbcIoWrapper.PerSourceDiscovery> discoveries =
        ImmutableList.of(
            legacyDiscovery1,
            legacyDiscovery2,
            legacyDiscovery3,
            uniformDiscovery4,
            uniformDiscovery5,
            uniformDiscovery6);

    // Act
    ImmutableMap<ImmutableList<SourceTableReference>, PTransform<PBegin, PCollection<SourceRow>>>
        tableReaders = JdbcIoWrapper.buildTableReaders(discoveries);

    // Assert
    // Legacy: shard2.t2 (1), shard3.t3a (1), shard3.t3b (1) = 3 transforms
    // Uniform: shard5 (1), shard6 (1) = 1 transform (combined)
    // Total = 4 entries
    assertThat(tableReaders).hasSize(4);

    // Verify Legacy readers
    long legacyCount =
        tableReaders.values().stream().filter(v -> v instanceof JdbcIO.ReadWithPartitions).count();
    assertThat(legacyCount).isEqualTo(3);

    // Verify that we have one combined Uniform reader
    long uniformCount =
        tableReaders.values().stream().filter(v -> v instanceof ReadWithUniformPartitions).count();
    assertThat(uniformCount).isEqualTo(1);

    // Verify table names in keys
    java.util.List<String> allTableNames =
        tableReaders.keySet().stream()
            .flatMap(java.util.List::stream)
            .map(SourceTableReference::sourceTableName)
            .collect(Collectors.toList());
    assertThat(allTableNames)
        .containsExactly("\"t2\"", "\"t3a\"", "\"t3b\"", "\"t5\"", "\"t6a\"", "\"t6b\"");
  }

  @Test
  public void testGetPerSourceDiscoveries() throws RetriableSchemaDiscoveryException {
    SourceSchemaReference schemaRef =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("testDB").build());
    JdbcIOWrapperConfig shardConfig1 =
        JdbcIOWrapperConfig.builderWithMySqlDefaults()
            .setSourceDbURL("jdbc:derby://myhost/memory:db1;create=true")
            .setSourceSchemaReference(schemaRef)
            .setShardID("shard1")
            .setDbAuth(LocalCredentialsProvider.builder().setUserName("u").setPassword("p").build())
            .setJdbcDriverJars("")
            .setJdbcDriverClassName("org.apache.derby.jdbc.EmbeddedDriver")
            .setDialectAdapter(mockDialectAdapter)
            .setTableVsPartitionColumns(ImmutableMap.of("t1", ImmutableList.of("id")))
            .build();
    JdbcIOWrapperConfig shardConfig2 =
        shardConfig1.toBuilder()
            .setSourceDbURL("jdbc:derby://myhost/memory:db2;create=true")
            .setShardID("shard2")
            .build();

    JdbcIoWrapperConfigGroup group =
        JdbcIoWrapperConfigGroup.builder()
            .setSourceDbDialect(SQLDialect.MYSQL)
            .addShardConfig(shardConfig1)
            .addShardConfig(shardConfig2)
            .build();

    // Mock discovery for both shards
    when(mockDialectAdapter.discoverTables(any(), (SourceSchemaReference) any()))
        .thenReturn(ImmutableList.of("t1"));
    when(mockDialectAdapter.discoverTableIndexes(any(), (SourceSchemaReference) any(), any()))
        .thenReturn(
            ImmutableMap.of(
                "t1",
                ImmutableList.of(
                    SourceColumnIndexInfo.builder()
                        .setColumnName("id")
                        .setIndexType(IndexType.NUMERIC)
                        .setOrdinalPosition(1)
                        .setIndexName("PRIMARY")
                        .setIsPrimary(true)
                        .setCardinality(10L)
                        .setIsUnique(true)
                        .build())));
    when(mockDialectAdapter.discoverTableSchema(any(), (SourceSchemaReference) any(), any()))
        .thenReturn(
            ImmutableMap.of(
                "t1", ImmutableMap.of("id", new SourceColumnType("INT", new Long[] {}, null))));

    ImmutableList<JdbcIoWrapper.PerSourceDiscovery> discoveries =
        JdbcIoWrapper.getPerSourceDiscoveries(group);

    assertThat(discoveries).hasSize(2);
    // Note: parallelStream order is not guaranteed.
    // We check containment to be safe.
    assertThat(discoveries.stream().map(d -> d.config().shardID()).collect(Collectors.toList()))
        .containsAnyIn(ImmutableList.of("shard1", "shard2"));
    assertThat(discoveries.stream().map(d -> d.config().shardID()).collect(Collectors.toList()))
        .containsNoDuplicates();
  }

  /**
   * Tests that {@link JdbcIoWrapper#getPerSourceDiscoveries} correctly propagates exceptions when
   * schema discovery fails for a shard.
   */
  @Test
  public void testGetPerSourceDiscoveries_Fails() throws RetriableSchemaDiscoveryException {
    SourceSchemaReference schemaRef =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("testDB").build());
    JdbcIOWrapperConfig shardConfig1 =
        JdbcIOWrapperConfig.builderWithMySqlDefaults()
            .setSourceDbURL("jdbc:derby://myhost/memory:db1;create=true")
            .setSourceSchemaReference(schemaRef)
            .setShardID("shard1")
            .setDbAuth(LocalCredentialsProvider.builder().setUserName("u").setPassword("p").build())
            .setJdbcDriverJars("")
            .setJdbcDriverClassName("org.apache.derby.jdbc.EmbeddedDriver")
            .setDialectAdapter(mockDialectAdapter)
            .build();

    JdbcIoWrapperConfigGroup group =
        JdbcIoWrapperConfigGroup.builder()
            .setSourceDbDialect(SQLDialect.MYSQL)
            .addShardConfig(shardConfig1)
            .build();

    when(mockDialectAdapter.discoverTables(any(), (SourceSchemaReference) any()))
        .thenThrow(new RuntimeException("Test Exception"));

    assertThrows(RuntimeException.class, () -> JdbcIoWrapper.getPerSourceDiscoveries(group));
  }

  /**
   * Tests that {@link JdbcIoWrapper#getPerSourceDiscoveries} logs a warning and continues when
   * closing a data source fails after discovery.
   */
  @Test
  public void testGetPerSourceDiscovery_LogsWarning_WhenCloseFails() throws Exception {
    SourceSchemaReference schemaRef =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("testDB").build());
    JdbcIOWrapperConfig shardConfig1 =
        JdbcIOWrapperConfig.builderWithMySqlDefaults()
            .setSourceDbURL("jdbc:derby://myhost/memory:db1;create=true")
            .setSourceSchemaReference(schemaRef)
            .setShardID("shard1")
            .setDbAuth(LocalCredentialsProvider.builder().setUserName("u").setPassword("p").build())
            .setJdbcDriverJars("")
            .setJdbcDriverClassName("org.apache.derby.jdbc.EmbeddedDriver")
            .setDialectAdapter(mockDialectAdapter)
            .build();

    JdbcIoWrapperConfigGroup group =
        JdbcIoWrapperConfigGroup.builder()
            .setSourceDbDialect(SQLDialect.MYSQL)
            .addShardConfig(shardConfig1)
            .build();

    // Mock successful discovery so it reaches the finally block
    when(mockDialectAdapter.discoverTables(any(), (SourceSchemaReference) any()))
        .thenReturn(ImmutableList.of("t1"));
    when(mockDialectAdapter.discoverTableIndexes(any(), (SourceSchemaReference) any(), any()))
        .thenReturn(
            ImmutableMap.of(
                "t1",
                ImmutableList.of(
                    SourceColumnIndexInfo.builder()
                        .setIndexType(IndexType.NUMERIC)
                        .setIndexName("PRIMARY")
                        .setIsPrimary(true)
                        .setCardinality(42L)
                        .setColumnName("ID")
                        .setIsUnique(true)
                        .setOrdinalPosition(1)
                        .build())));
    when(mockDialectAdapter.discoverTableSchema(any(), (SourceSchemaReference) any(), any()))
        .thenReturn(
            ImmutableMap.of(
                "t1", ImmutableMap.of("ID", new SourceColumnType("INTEGER", new Long[] {}, null))));

    try (MockedConstruction<BasicDataSource> mocked =
        mockConstruction(
            BasicDataSource.class,
            (mock, context) -> {
              doThrow(new SQLException("Close Failure")).when(mock).close();
            })) {
      // Act
      ImmutableList<JdbcIoWrapper.PerSourceDiscovery> result =
          JdbcIoWrapper.getPerSourceDiscoveries(group);

      // Assert
      assertThat(result).hasSize(1);
      // The SQLException is caught and logged, so it shouldn't propagate.
    }
  }

  @Test
  public void testGetPerSourceDiscovery_SuitableIndexNotFoundException()
      throws RetriableSchemaDiscoveryException {
    SourceSchemaReference schemaRef =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("testDB").build());
    JdbcIOWrapperConfig shardConfig =
        JdbcIOWrapperConfig.builderWithMySqlDefaults()
            .setSourceDbURL("jdbc:derby://myhost/memory:db1;create=true")
            .setSourceSchemaReference(schemaRef)
            .setShardID("shard1")
            .setDbAuth(LocalCredentialsProvider.builder().setUserName("u").setPassword("p").build())
            .setJdbcDriverJars("")
            .setJdbcDriverClassName("org.apache.derby.jdbc.EmbeddedDriver")
            .setDialectAdapter(mockDialectAdapter)
            .build();

    JdbcIoWrapperConfigGroup group =
        JdbcIoWrapperConfigGroup.builder()
            .setSourceDbDialect(SQLDialect.MYSQL)
            .addShardConfig(shardConfig)
            .build();

    // Force SuitableIndexNotFoundException during autoInferTableConfigs
    when(mockDialectAdapter.discoverTables(any(), (SourceSchemaReference) any()))
        .thenReturn(ImmutableList.of("t1"));
    when(mockDialectAdapter.discoverTableIndexes(any(), (SourceSchemaReference) any(), any()))
        .thenReturn(ImmutableMap.of()); // Empty indexes triggers exception

    assertThrows(
        SuitableIndexNotFoundException.class, () -> JdbcIoWrapper.getPerSourceDiscoveries(group));
  }

  /**
   * Tests that {@link JdbcIoWrapper#getPerSourceDiscoveries} handles InterruptedException during
   * parallel discovery.
   */
  @Test
  public void testGetPerSourceDiscovery_InterruptedException() throws Exception {
    JdbcIoWrapperConfigGroup mockGroup = Mockito.mock(JdbcIoWrapperConfigGroup.class);
    JdbcIOWrapperConfig mockConfig = Mockito.mock(JdbcIOWrapperConfig.class);
    when(mockGroup.shardConfigs()).thenReturn(ImmutableList.of(mockConfig));

    try (org.mockito.MockedStatic<java.util.concurrent.Executors> mockedExecutors =
        Mockito.mockStatic(java.util.concurrent.Executors.class)) {
      java.util.concurrent.ExecutorService mockExecutor =
          Mockito.mock(java.util.concurrent.ExecutorService.class);
      mockedExecutors
          .when(() -> java.util.concurrent.Executors.newFixedThreadPool(Mockito.anyInt()))
          .thenReturn(mockExecutor);

      java.util.concurrent.Future<JdbcIoWrapper.PerSourceDiscovery> mockFuture =
          Mockito.mock(java.util.concurrent.Future.class);
      when(mockFuture.get()).thenThrow(new InterruptedException("Interrupted"));
      when(mockExecutor.submit(any(java.util.concurrent.Callable.class))).thenReturn(mockFuture);

      assertThrows(
          SchemaDiscoveryException.class, () -> JdbcIoWrapper.getPerSourceDiscoveries(mockGroup));
    }
  }

  @Test
  public void testAccumulateSpecs() {
    SourceSchemaReference schemaRef =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("testDB").build());
    JdbcIOWrapperConfig config =
        JdbcIOWrapperConfig.builderWithMySqlDefaults()
            .setSourceDbURL("jdbc:mysql://localhost/test")
            .setDbAuth(
                LocalCredentialsProvider.builder().setUserName("user").setPassword("pass").build())
            .setJdbcDriverJars("")
            .setJdbcDriverClassName("com.mysql.cj.jdbc.Driver")
            .setSourceSchemaReference(schemaRef)
            .setMaxPartitions(10)
            .setSplitStageCountHint(5L)
            .setMaxFetchSize(42)
            .build();

    SourceColumnType colType = new SourceColumnType("INTEGER", new Long[] {}, null);
    SourceTableSchema tableSchema1 =
        SourceTableSchema.builder(SQLDialect.MYSQL)
            .setTableName("t1")
            .setTableSchemaUUID("uuid1")
            .setEstimatedRowSize(100L)
            .addSourceColumnNameToSourceColumnType("ID", colType)
            .build();
    TableConfig tableConfig1 =
        TableConfig.builder("t1")
            .setDataSourceId(config.id())
            .setApproxRowCount(1000L)
            .setMaxPartitions(10)
            .withPartitionColum(
                PartitionColumn.builder().setColumnName("ID").setColumnClass(Long.class).build())
            .build();

    SourceTableSchema tableSchema2 =
        SourceTableSchema.builder(SQLDialect.MYSQL)
            .setTableName("t2")
            .setTableSchemaUUID("uuid2")
            .setEstimatedRowSize(200L)
            .addSourceColumnNameToSourceColumnType("ID", colType)
            .build();
    TableConfig tableConfig2 =
        TableConfig.builder("t2").setDataSourceId(config.id()).setApproxRowCount(2000L).build();

    JdbcIoWrapper.PerSourceDiscovery discovery =
        JdbcIoWrapper.PerSourceDiscovery.builder()
            .setConfig(config)
            .setDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("driver", "url"))
            .setTableConfigs(ImmutableList.of(tableConfig1, tableConfig2))
            .setSourceSchema(
                SourceSchema.builder()
                    .setSchemaReference(schemaRef)
                    .addTableSchema(tableSchema1)
                    .addTableSchema(tableSchema2)
                    .build())
            .build();

    ImmutableList.Builder<SourceTableReference> tableReferencesBuilder = ImmutableList.builder();
    ImmutableList.Builder<TableSplitSpecification> splitSpecsBuilder = ImmutableList.builder();
    ImmutableMap.Builder<TableIdentifier, TableReadSpecification<SourceRow>> readSpecsBuilder =
        ImmutableMap.builder();

    JdbcIoWrapper.accumulateSpecs(
        ImmutableList.of(discovery), tableReferencesBuilder, splitSpecsBuilder, readSpecsBuilder);

    ImmutableList<SourceTableReference> tableRefs = tableReferencesBuilder.build();
    ImmutableList<TableSplitSpecification> splitSpecs = splitSpecsBuilder.build();
    ImmutableMap<TableIdentifier, TableReadSpecification<SourceRow>> readSpecs =
        readSpecsBuilder.build();

    assertThat(tableRefs).hasSize(2);
    assertThat(tableRefs.get(0).sourceTableName()).isEqualTo("\"t1\"");
    assertThat(tableRefs.get(0).sourceTableSchemaUUID()).isEqualTo("uuid1");
    assertThat(tableRefs.get(1).sourceTableName()).isEqualTo("\"t2\"");
    assertThat(tableRefs.get(1).sourceTableSchemaUUID()).isEqualTo("uuid2");

    assertThat(splitSpecs).hasSize(2);
    assertThat(splitSpecs.get(0).tableIdentifier().tableName()).isEqualTo("\"t1\"");
    assertThat(splitSpecs.get(0).approxRowCount()).isEqualTo(1000L);
    assertThat(splitSpecs.get(0).maxPartitionsHint()).isEqualTo(10L);
    assertThat(splitSpecs.get(0).splitStagesCount()).isEqualTo(5L);

    assertThat(readSpecs).hasSize(2);
    TableIdentifier id1 =
        TableIdentifier.builder().setTableName("\"t1\"").setDataSourceId(config.id()).build();
    assertThat(readSpecs.get(id1).fetchSize()).isEqualTo(42);
  }

  @Test
  public void testAccumulateSpecs_Empty() {
    SourceSchemaReference schemaRef =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("testDB").build());
    JdbcIOWrapperConfig config =
        JdbcIOWrapperConfig.builderWithMySqlDefaults()
            .setSourceDbURL("jdbc:mysql://localhost/test")
            .setDbAuth(
                LocalCredentialsProvider.builder().setUserName("user").setPassword("pass").build())
            .setJdbcDriverJars("")
            .setJdbcDriverClassName("com.mysql.cj.jdbc.Driver")
            .setSourceSchemaReference(schemaRef)
            .build();

    JdbcIoWrapper.PerSourceDiscovery discovery =
        JdbcIoWrapper.PerSourceDiscovery.builder()
            .setConfig(config)
            .setDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("driver", "url"))
            .setTableConfigs(ImmutableList.of())
            .setSourceSchema(SourceSchema.builder().setSchemaReference(schemaRef).build())
            .build();

    ImmutableList.Builder<SourceTableReference> tableReferencesBuilder = ImmutableList.builder();
    ImmutableList.Builder<TableSplitSpecification> splitSpecsBuilder = ImmutableList.builder();
    ImmutableMap.Builder<TableIdentifier, TableReadSpecification<SourceRow>> readSpecsBuilder =
        ImmutableMap.builder();

    JdbcIoWrapper.accumulateSpecs(
        ImmutableList.of(discovery), tableReferencesBuilder, splitSpecsBuilder, readSpecsBuilder);

    assertThat(tableReferencesBuilder.build()).isEmpty();
    assertThat(splitSpecsBuilder.build()).isEmpty();
    assertThat(readSpecsBuilder.build()).isEmpty();
  }

  /**
   * Tests that {@link JdbcIoWrapper#accumulateSpecs} does not generate split specifications when
   * the uniform partitions feature is disabled.
   */
  @Test
  public void testAccumulateSpecs_FeatureDisabled() {
    SourceSchemaReference schemaRef =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("testDB").build());
    JdbcIOWrapperConfig config =
        JdbcIOWrapperConfig.builderWithMySqlDefaults()
            .setReadWithUniformPartitionsFeatureEnabled(false)
            .setSourceDbURL("jdbc:mysql://localhost/test")
            .setDbAuth(
                LocalCredentialsProvider.builder().setUserName("user").setPassword("pass").build())
            .setJdbcDriverJars("")
            .setJdbcDriverClassName("com.mysql.cj.jdbc.Driver")
            .setSourceSchemaReference(schemaRef)
            .build();
    TableConfig tableConfig = TableConfig.builder("testTable").setDataSourceId("shard1").build();
    JdbcIoWrapper.PerSourceDiscovery discovery =
        JdbcIoWrapper.PerSourceDiscovery.builder()
            .setConfig(config)
            .setDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("c", "u"))
            .setTableConfigs(ImmutableList.of(tableConfig))
            .setSourceSchema(SourceSchema.builder().setSchemaReference(schemaRef).build())
            .build();

    ImmutableList.Builder<SourceTableReference> tableReferencesBuilder = ImmutableList.builder();
    ImmutableList.Builder<TableSplitSpecification> splitSpecsBuilder = ImmutableList.builder();
    ImmutableMap.Builder<TableIdentifier, TableReadSpecification<SourceRow>> readSpecsBuilder =
        ImmutableMap.builder();

    JdbcIoWrapper.accumulateSpecs(
        ImmutableList.of(discovery), tableReferencesBuilder, splitSpecsBuilder, readSpecsBuilder);

    assertThat(splitSpecsBuilder.build()).isEmpty();
    assertThat(readSpecsBuilder.build()).isEmpty();
  }

  @Test
  public void testGetDataSourceProvider() {
    JdbcIOWrapperConfig config1 =
        JdbcIOWrapperConfig.builderWithMySqlDefaults()
            .setSourceDbURL("jdbc:mysql://host1/test")
            .setDbAuth(LocalCredentialsProvider.builder().setUserName("u").setPassword("p").build())
            .setJdbcDriverJars("")
            .setJdbcDriverClassName("c")
            .setSourceSchemaReference(JdbcSchemaReference.builder().setDbName("db1").build())
            .build();
    JdbcIOWrapperConfig config2 =
        JdbcIOWrapperConfig.builderWithMySqlDefaults()
            .setSourceDbURL("jdbc:mysql://host2/test")
            .setDbAuth(LocalCredentialsProvider.builder().setUserName("u").setPassword("p").build())
            .setJdbcDriverJars("")
            .setJdbcDriverClassName("c")
            .setSourceSchemaReference(JdbcSchemaReference.builder().setDbName("db2").build())
            .build();

    JdbcIoWrapper.PerSourceDiscovery discovery1 =
        JdbcIoWrapper.PerSourceDiscovery.builder()
            .setConfig(config1)
            .setDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("c", "u1"))
            .setTableConfigs(ImmutableList.of())
            .setSourceSchema(
                SourceSchema.builder().setSchemaReference(config1.sourceSchemaReference()).build())
            .build();
    JdbcIoWrapper.PerSourceDiscovery discovery2 =
        JdbcIoWrapper.PerSourceDiscovery.builder()
            .setConfig(config2)
            .setDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("c", "u2"))
            .setTableConfigs(ImmutableList.of())
            .setSourceSchema(
                SourceSchema.builder().setSchemaReference(config2.sourceSchemaReference()).build())
            .build();

    DataSourceProvider provider =
        JdbcIoWrapper.getDataSourceProvider(ImmutableList.of(discovery1, discovery2));

    assertThat(provider.getDataSourceIds()).containsExactly(config1.id(), config2.id());
  }

  @Test
  public void testGetDataSourceProvider_Empty() {
    DataSourceProvider provider = JdbcIoWrapper.getDataSourceProvider(ImmutableList.of());
    assertThat(provider.getDataSourceIds()).isEmpty();
  }

  @Test
  public void testGetMultiTableReadWithUniformPartitionIO_Empty() {
    assertThat(JdbcIoWrapper.getMultiTableReadWithUniformPartitionIO(ImmutableList.of())).isEmpty();
  }

  @Test
  public void testGetMultiTableReadWithUniformPartitionIO_FeatureDisabled() {
    JdbcIOWrapperConfig config =
        JdbcIOWrapperConfig.builderWithMySqlDefaults()
            .setSourceDbURL("jdbc:mysql://host1/test")
            .setDbAuth(LocalCredentialsProvider.builder().setUserName("u").setPassword("p").build())
            .setJdbcDriverJars("")
            .setJdbcDriverClassName("c")
            .setSourceSchemaReference(JdbcSchemaReference.builder().setDbName("db1").build())
            .setReadWithUniformPartitionsFeatureEnabled(false)
            .setDialectAdapter(mockDialectAdapter)
            .build();

    JdbcIoWrapper.PerSourceDiscovery discovery =
        JdbcIoWrapper.PerSourceDiscovery.builder()
            .setConfig(config)
            .setDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("c", "u1"))
            .setTableConfigs(
                ImmutableList.of(TableConfig.builder("table1").setDataSourceId("ds1").build()))
            .setSourceSchema(
                SourceSchema.builder().setSchemaReference(config.sourceSchemaReference()).build())
            .build();

    assertThat(JdbcIoWrapper.getMultiTableReadWithUniformPartitionIO(ImmutableList.of(discovery)))
        .isEmpty();
  }

  @Test
  public void testGetMultiTableReadWithUniformPartitionIO_EmptyTableConfigs() {
    JdbcIOWrapperConfig config =
        JdbcIOWrapperConfig.builderWithMySqlDefaults()
            .setSourceDbURL("jdbc:mysql://host1/test")
            .setDbAuth(LocalCredentialsProvider.builder().setUserName("u").setPassword("p").build())
            .setJdbcDriverJars("")
            .setJdbcDriverClassName("c")
            .setSourceSchemaReference(JdbcSchemaReference.builder().setDbName("db1").build())
            .setReadWithUniformPartitionsFeatureEnabled(true)
            .setDialectAdapter(mockDialectAdapter)
            .build();

    JdbcIoWrapper.PerSourceDiscovery discovery =
        JdbcIoWrapper.PerSourceDiscovery.builder()
            .setConfig(config)
            .setDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("c", "u1"))
            .setTableConfigs(ImmutableList.of())
            .setSourceSchema(
                SourceSchema.builder().setSchemaReference(config.sourceSchemaReference()).build())
            .build();

    assertThat(JdbcIoWrapper.getMultiTableReadWithUniformPartitionIO(ImmutableList.of(discovery)))
        .isEmpty();
  }

  @Test
  public void testGetMultiTableReadWithUniformPartitionIO_Success() {
    JdbcIOWrapperConfig config =
        JdbcIOWrapperConfig.builderWithMySqlDefaults()
            .setSourceDbURL("jdbc:mysql://host1/test")
            .setDbAuth(LocalCredentialsProvider.builder().setUserName("u").setPassword("p").build())
            .setJdbcDriverJars("")
            .setJdbcDriverClassName("c")
            .setSourceSchemaReference(JdbcSchemaReference.builder().setDbName("db1").build())
            .setReadWithUniformPartitionsFeatureEnabled(true)
            .setDialectAdapter(mockDialectAdapter)
            .build();

    String tableName = "table1";
    TableConfig tableConfig = TableConfig.builder(tableName).setDataSourceId("ds1").build();
    SourceSchema sourceSchema =
        SourceSchema.builder()
            .setSchemaReference(config.sourceSchemaReference())
            .addTableSchema(
                SourceTableSchema.builder(SQLDialect.MYSQL)
                    .setTableName(tableName)
                    .setTableSchemaUUID("uuid")
                    .setEstimatedRowSize(100L)
                    .addSourceColumnNameToSourceColumnType(
                        "col1", new SourceColumnType("INTEGER", new Long[] {}, null))
                    .build())
            .build();

    JdbcIoWrapper.PerSourceDiscovery discovery =
        JdbcIoWrapper.PerSourceDiscovery.builder()
            .setConfig(config)
            .setDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("c", "u1"))
            .setTableConfigs(ImmutableList.of(tableConfig))
            .setSourceSchema(sourceSchema)
            .build();

    ImmutableMap<ImmutableList<SourceTableReference>, PTransform<PBegin, PCollection<SourceRow>>>
        result = JdbcIoWrapper.getMultiTableReadWithUniformPartitionIO(ImmutableList.of(discovery));

    assertThat(result).hasSize(1);
    assertThat(result.values().iterator().next()).isInstanceOf(ReadWithUniformPartitions.class);
  }

  @Test
  public void testGetMultiTableReadWithUniformPartitionIO_Mix() {
    JdbcIOWrapperConfig configUniform =
        JdbcIOWrapperConfig.builderWithMySqlDefaults()
            .setSourceDbURL("jdbc:mysql://host1/test")
            .setDbAuth(LocalCredentialsProvider.builder().setUserName("u").setPassword("p").build())
            .setJdbcDriverJars("")
            .setJdbcDriverClassName("c")
            .setSourceSchemaReference(JdbcSchemaReference.builder().setDbName("db1").build())
            .setReadWithUniformPartitionsFeatureEnabled(true)
            .setDialectAdapter(mockDialectAdapter)
            .build();

    JdbcIOWrapperConfig configLegacy =
        configUniform.toBuilder()
            .setId(JdbcIOWrapperConfig.generateId())
            .setSourceDbURL("jdbc:mysql://host2/test")
            .setReadWithUniformPartitionsFeatureEnabled(false)
            .build();

    String tableName = "table1";
    TableConfig tableConfig = TableConfig.builder(tableName).setDataSourceId("ds1").build();
    SourceSchema sourceSchema =
        SourceSchema.builder()
            .setSchemaReference(configUniform.sourceSchemaReference())
            .addTableSchema(
                SourceTableSchema.builder(SQLDialect.MYSQL)
                    .setTableName(tableName)
                    .setTableSchemaUUID("uuid")
                    .setEstimatedRowSize(100L)
                    .addSourceColumnNameToSourceColumnType(
                        "col1", new SourceColumnType("INTEGER", new Long[] {}, null))
                    .build())
            .build();

    JdbcIoWrapper.PerSourceDiscovery discoveryUniform =
        JdbcIoWrapper.PerSourceDiscovery.builder()
            .setConfig(configUniform)
            .setDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("c", "u1"))
            .setTableConfigs(ImmutableList.of(tableConfig))
            .setSourceSchema(sourceSchema)
            .build();

    JdbcIoWrapper.PerSourceDiscovery discoveryLegacy =
        JdbcIoWrapper.PerSourceDiscovery.builder()
            .setConfig(configLegacy)
            .setDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("c", "u1"))
            .setTableConfigs(ImmutableList.of(tableConfig))
            .setSourceSchema(sourceSchema)
            .build();

    ImmutableMap<ImmutableList<SourceTableReference>, PTransform<PBegin, PCollection<SourceRow>>>
        result =
            JdbcIoWrapper.getMultiTableReadWithUniformPartitionIO(
                ImmutableList.of(discoveryUniform, discoveryLegacy));

    assertThat(result).hasSize(1);
    ImmutableList<SourceTableReference> tableRefs = result.keySet().iterator().next();
    assertThat(tableRefs).hasSize(1);
    assertThat(tableRefs.get(0).sourceTableName()).isEqualTo("\"table1\"");
  }
}
