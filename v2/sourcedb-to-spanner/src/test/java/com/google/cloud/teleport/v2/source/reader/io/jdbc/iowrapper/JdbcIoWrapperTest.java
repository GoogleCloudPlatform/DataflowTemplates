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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.source.reader.auth.dbauth.LocalCredentialsProvider;
import com.google.cloud.teleport.v2.source.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.exception.SuitableIndexNotFoundException;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.DialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.JdbcIOWrapperConfig;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.SQLDialect;
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
import java.math.BigInteger;
import java.sql.SQLException;
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
  public void testJdbcIoWrapperDifferentTables() throws RetriableSchemaDiscoveryException {
    // Test to check what happens if config passes tables not present in source
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
                .build());
    ImmutableMap<SourceTableReference, PTransform<PBegin, PCollection<SourceRow>>> tableReaders =
        jdbcIoWrapper.getTableReaders();
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
    JdbcIoWrapper jdbcIOWrapperWithFeatureEnabled = JdbcIoWrapper.of(configWithFeatureEnabled);
    JdbcIoWrapper jdbcIOWrapperWithFeatureDisabled = JdbcIoWrapper.of(configWithFeatureDisabled);
    assertThat(
            jdbcIOWrapperWithFeatureDisabled.getTableReaders().values().stream().findFirst().get())
        .isInstanceOf(JdbcIO.ReadWithPartitions.class);
    assertThat(
            jdbcIOWrapperWithFeatureEnabled.getTableReaders().values().stream().findFirst().get())
        .isInstanceOf(ReadWithUniformPartitions.class);
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
        .isEqualTo(BigInteger.class);
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
}
