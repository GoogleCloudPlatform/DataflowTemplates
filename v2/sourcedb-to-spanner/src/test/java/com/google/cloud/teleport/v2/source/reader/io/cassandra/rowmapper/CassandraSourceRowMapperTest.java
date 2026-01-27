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
package com.google.cloud.teleport.v2.source.reader.io.cassandra.rowmapper;

import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.LIST_TYPES_TABLE;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.LIST_TYPES_TABLE_AVRO_ROWS;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.MAP_TYPES_TABLE;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.MAP_TYPES_TABLE_AVRO_ROWS;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.PRIMITIVE_TYPES_TABLE;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.PRIMITIVE_TYPES_TABLE_AVRO_ROWS;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.SET_TYPES_TABLE;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.SET_TYPES_TABLE_AVRO_ROWS;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_CONFIG;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_CONFIG_SSL;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_CQLSH;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_KEYSPACE;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.JdkSSLOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper.CassandraConnector;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper.CassandraDataSource;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper.CassandraDataSourceOss;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper.SslContextFactory;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.schema.CassandraSchemaDiscovery;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.schema.CassandraSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.SharedEmbeddedCassandra;
import com.google.cloud.teleport.v2.source.reader.io.datasource.DataSource;
import com.google.cloud.teleport.v2.source.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.UnifiedTypeMapper.MapperType;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import javax.net.ssl.SSLContext;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link CassandraSourceRowMapper}. */
@RunWith(MockitoJUnitRunner.class)
public class CassandraSourceRowMapperTest {

  private static SharedEmbeddedCassandra sharedEmbeddedCassandra = null;
  private static SharedEmbeddedCassandra sharedEmbeddedCassandraWithSsl = null;

  @BeforeClass
  public static void startEmbeddedCassandra() throws IOException {
    if (sharedEmbeddedCassandra == null) {
      sharedEmbeddedCassandra = new SharedEmbeddedCassandra(TEST_CONFIG, TEST_CQLSH);
    }
    if (sharedEmbeddedCassandraWithSsl == null) {
      sharedEmbeddedCassandraWithSsl =
          new SharedEmbeddedCassandra(TEST_CONFIG_SSL, TEST_CQLSH, true);
    }
  }

  @AfterClass
  public static void stopEmbeddedCassandra() throws Exception {
    if (sharedEmbeddedCassandra != null) {
      sharedEmbeddedCassandra.close();
      sharedEmbeddedCassandra = null;
    }
    if (sharedEmbeddedCassandraWithSsl != null) {
      sharedEmbeddedCassandraWithSsl.close();
      sharedEmbeddedCassandraWithSsl = null;
    }
  }

  @Test
  public void testCassandraSourceRowMapperBasic() throws RetriableSchemaDiscoveryException {
    cassandraSourceRowMapperTestHelper(PRIMITIVE_TYPES_TABLE, PRIMITIVE_TYPES_TABLE_AVRO_ROWS);
  }

  @Test
  public void testCassandraSourceRowMapperList() throws RetriableSchemaDiscoveryException {
    cassandraSourceRowMapperTestHelper(LIST_TYPES_TABLE, LIST_TYPES_TABLE_AVRO_ROWS);
    astraDbSourceRowMapperTestHelper(LIST_TYPES_TABLE, LIST_TYPES_TABLE_AVRO_ROWS);
  }

  @Test
  public void testCassandraSourceRowMapperSet() throws RetriableSchemaDiscoveryException {
    cassandraSourceRowMapperTestHelper(SET_TYPES_TABLE, SET_TYPES_TABLE_AVRO_ROWS);
    astraDbSourceRowMapperTestHelper(SET_TYPES_TABLE, SET_TYPES_TABLE_AVRO_ROWS);
  }

  @Test
  public void testCassandraSourceRowMapperMap() throws RetriableSchemaDiscoveryException {
    cassandraSourceRowMapperTestHelper(MAP_TYPES_TABLE, MAP_TYPES_TABLE_AVRO_ROWS);
    astraDbSourceRowMapperTestHelper(MAP_TYPES_TABLE, MAP_TYPES_TABLE_AVRO_ROWS);
  }

  @Test
  public void testCassandraSourceRowMapperWithSsl()
      throws IOException, RetriableSchemaDiscoveryException {
    cassandraSourceRowMapperSslTestHelper(
        PRIMITIVE_TYPES_TABLE, PRIMITIVE_TYPES_TABLE_AVRO_ROWS, sharedEmbeddedCassandraWithSsl);
  }

  private void cassandraSourceRowMapperTestHelper(String tableName, List<String> expectedRows)
      throws RetriableSchemaDiscoveryException {

    SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.ofCassandra(
            CassandraSchemaReference.builder().setKeyspaceName(TEST_KEYSPACE).build());

    DataSource dataSource =
        DataSource.ofCassandra(
            CassandraDataSource.ofOss(
                CassandraDataSourceOss.builder()
                    .setClusterName(sharedEmbeddedCassandra.getInstance().getClusterName())
                    .setOptionsMap(OptionsMap.driverDefaults())
                    .setContactPoints(sharedEmbeddedCassandra.getInstance().getContactPoints())
                    .setLocalDataCenter(sharedEmbeddedCassandra.getInstance().getLocalDataCenter())
                    .build()));

    SourceTableSchema.Builder sourceTableSchemaBuilder =
        SourceTableSchema.builder(MapperType.CASSANDRA).setTableName(tableName);
    new CassandraSchemaDiscovery()
        .discoverTableSchema(dataSource, sourceSchemaReference, ImmutableList.of(tableName))
        .get(tableName)
        .forEach(sourceTableSchemaBuilder::addSourceColumnNameToSourceColumnType);

    CassandraSourceRowMapper cassandraSourceRowMapper =
        CassandraSourceRowMapper.builder()
            .setSourceSchemaReference(sourceSchemaReference)
            .setSourceTableSchema(sourceTableSchemaBuilder.build())
            .build();

    String query = "SELECT * FROM " + tableName;
    Cluster cluster = null;
    try {
      cluster =
          Cluster.builder()
              .addContactPointsWithPorts(dataSource.cassandra().oss().contactPoints())
              .withClusterName(dataSource.cassandra().oss().clusterName())
              .withoutJMXReporting()
              .withLoadBalancingPolicy(
                  new DCAwareRoundRobinPolicy.Builder()
                      .withLocalDc(dataSource.cassandra().oss().localDataCenter())
                      .build())
              .build();
      ResultSet resultSet = cluster.connect(TEST_KEYSPACE).execute(query);
      ImmutableList.Builder<SourceRow> readRowsBuilder = ImmutableList.builder();
      cassandraSourceRowMapper.map(resultSet).forEachRemaining(row -> readRowsBuilder.add(row));
      ImmutableList<SourceRow> readRows = readRowsBuilder.build();

      readRows.forEach(r -> assertThat(r.tableName() == tableName));
      readRows.forEach(r -> assertThat(r.sourceSchemaReference() == sourceSchemaReference));
      assertThat(
              readRows.stream()
                  .map(r -> r.getPayload().toString())
                  .sorted()
                  .collect(ImmutableList.toImmutableList()))
          .isEqualTo(expectedRows.stream().sorted().collect(ImmutableList.toImmutableList()));

      // Since we will use CassandraIO only for reads, we don't need to support the `deleteAsync`
      // and `saveAsync` functions of the CassandraIO mapper interface.
      assertThrows(
          UnsupportedOperationException.class,
          () -> cassandraSourceRowMapper.deleteAsync(readRows.get(0)));
      assertThrows(
          UnsupportedOperationException.class,
          () -> cassandraSourceRowMapper.saveAsync(readRows.get(0)));
    } finally {
      if (cluster != null) {
        cluster.close();
      }
    }
  }

  private void cassandraSourceRowMapperSslTestHelper(
      String tableName, List<String> expectedRows, SharedEmbeddedCassandra cassandra)
      throws RetriableSchemaDiscoveryException {

    SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.ofCassandra(
            CassandraSchemaReference.builder().setKeyspaceName(TEST_KEYSPACE).build());

    OptionsMap optionsMap = OptionsMap.driverDefaults();
    optionsMap.put(
        DriverExecutionProfile.DEFAULT_NAME,
        TypedDriverOption.SSL_TRUSTSTORE_PATH,
        cassandra.getInstance().getTrustStorePath().toString());
    optionsMap.put(
        DriverExecutionProfile.DEFAULT_NAME,
        TypedDriverOption.SSL_TRUSTSTORE_PASSWORD,
        "cassandra");

    DataSource dataSource =
        DataSource.ofCassandra(
            CassandraDataSource.ofOss(
                CassandraDataSourceOss.builder()
                    .setClusterName(cassandra.getInstance().getClusterName())
                    .setOptionsMap(optionsMap)
                    .setContactPoints(cassandra.getInstance().getContactPoints())
                    .setLocalDataCenter(cassandra.getInstance().getLocalDataCenter())
                    .build()));

    SourceTableSchema.Builder sourceTableSchemaBuilder =
        SourceTableSchema.builder(MapperType.CASSANDRA).setTableName(tableName);
    new CassandraSchemaDiscovery()
        .discoverTableSchema(dataSource, sourceSchemaReference, ImmutableList.of(tableName))
        .get(tableName)
        .forEach(sourceTableSchemaBuilder::addSourceColumnNameToSourceColumnType);

    CassandraSourceRowMapper cassandraSourceRowMapper =
        CassandraSourceRowMapper.builder()
            .setSourceSchemaReference(sourceSchemaReference)
            .setSourceTableSchema(sourceTableSchemaBuilder.build())
            .build();

    Cluster cluster = null;
    try {
      SSLContext sslContext =
          SslContextFactory.createSslContext(
              cassandra.getInstance().getTrustStorePath().toString(), "cassandra", null, null);
      SSLOptions sslOptions = JdkSSLOptions.builder().withSSLContext(sslContext).build();

      cluster =
          Cluster.builder()
              .addContactPointsWithPorts(dataSource.cassandra().oss().contactPoints())
              .withClusterName(dataSource.cassandra().oss().clusterName())
              .withSSL(sslOptions)
              .withoutJMXReporting()
              .withLoadBalancingPolicy(
                  new DCAwareRoundRobinPolicy.Builder()
                      .withLocalDc(dataSource.cassandra().oss().localDataCenter())
                      .build())
              .build();

      String query = "SELECT * FROM " + tableName;
      ResultSet resultSet = cluster.connect(TEST_KEYSPACE).execute(query);
      ImmutableList.Builder<SourceRow> readRowsBuilder = ImmutableList.builder();
      cassandraSourceRowMapper.map(resultSet).forEachRemaining(row -> readRowsBuilder.add(row));
      ImmutableList<SourceRow> readRows = readRowsBuilder.build();

      readRows.forEach(r -> assertThat(r.tableName() == tableName));
      readRows.forEach(r -> assertThat(r.sourceSchemaReference() == sourceSchemaReference));
      assertThat(
              readRows.stream()
                  .map(r -> r.getPayload().toString())
                  .sorted()
                  .collect(ImmutableList.toImmutableList()))
          .isEqualTo(expectedRows.stream().sorted().collect(ImmutableList.toImmutableList()));

    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (cluster != null) {
        cluster.close();
      }
    }
  }

  private void astraDbSourceRowMapperTestHelper(String tableName, List<String> expectedRows)
      throws RetriableSchemaDiscoveryException {

    SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.ofCassandra(
            CassandraSchemaReference.builder().setKeyspaceName(TEST_KEYSPACE).build());

    DataSource dataSource =
        DataSource.ofCassandra(
            CassandraDataSource.ofOss(
                CassandraDataSourceOss.builder()
                    .setClusterName(sharedEmbeddedCassandra.getInstance().getClusterName())
                    .setOptionsMap(OptionsMap.driverDefaults())
                    .setContactPoints(sharedEmbeddedCassandra.getInstance().getContactPoints())
                    .setLocalDataCenter(sharedEmbeddedCassandra.getInstance().getLocalDataCenter())
                    .build()));

    SourceTableSchema.Builder sourceTableSchemaBuilder =
        SourceTableSchema.builder(MapperType.CASSANDRA).setTableName(tableName);
    new CassandraSchemaDiscovery()
        .discoverTableSchema(dataSource, sourceSchemaReference, ImmutableList.of(tableName))
        .get(tableName)
        .forEach(sourceTableSchemaBuilder::addSourceColumnNameToSourceColumnType);

    AstraDbSourceRowMapper astraDbSourceRowMapper =
        AstraDbSourceRowMapper.builder()
            .setSourceSchemaReference(sourceSchemaReference)
            .setSourceTableSchema(sourceTableSchemaBuilder.build())
            .build();
    com.datastax.oss.driver.api.core.cql.ResultSet resultSet;
    String query = "SELECT * FROM " + tableName;
    var statement = SimpleStatement.newInstance(query);
    try (CassandraConnector cassandraConnectorWithSchemaReference =
        new CassandraConnector(dataSource.cassandra(), sourceSchemaReference.cassandra())) {
      resultSet = cassandraConnectorWithSchemaReference.getSession().execute(statement);
      ImmutableList.Builder<SourceRow> readRowsBuilder = ImmutableList.builder();
      resultSet
          .iterator()
          .forEachRemaining(row -> readRowsBuilder.add((astraDbSourceRowMapper.mapRow(row))));
      ImmutableList<SourceRow> readRows = readRowsBuilder.build();

      readRows.forEach(r -> assertThat(r.tableName() == tableName));
      readRows.forEach(r -> assertThat(r.sourceSchemaReference() == sourceSchemaReference));
      assertThat(
              readRows.stream()
                  .map(r -> r.getPayload().toString())
                  .sorted()
                  .collect(ImmutableList.toImmutableList()))
          .isEqualTo(expectedRows.stream().sorted().collect(ImmutableList.toImmutableList()));

      // Since we will use CassandraIO only for reads, we don't need to support the `deleteAsync`
      // and `saveAsync` functions of the CassandraIO mapper interface.
      assertThrows(
          UnsupportedOperationException.class,
          () -> astraDbSourceRowMapper.deleteAsync(readRows.get(0)));
      assertThrows(
          UnsupportedOperationException.class,
          () -> astraDbSourceRowMapper.saveAsync(readRows.get(0)));
    }
  }

  @Test
  public void testCassandraSourceRowForNullValues() {
    ResultSet mockResultSet = Mockito.mock(ResultSet.class);
    Row mockRow = Mockito.mock(Row.class);
    final String testIntCol = "testIntCol";
    final String nullCol = "nullCol";
    final String extractedNullCol = "extractedNullCol";

    when(mockRow.getInt(testIntCol)).thenReturn(42);

    when(mockRow.isNull(nullCol)).thenReturn(true);

    when(mockRow.isNull(extractedNullCol)).thenReturn(false);
    when(mockRow.getString(extractedNullCol)).thenReturn(null);

    when(mockResultSet.iterator()).thenReturn(ImmutableList.of(mockRow).stream().iterator());

    SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.ofCassandra(
            CassandraSchemaReference.builder().setKeyspaceName(TEST_KEYSPACE).build());

    SourceTableSchema sourceTableSchema =
        SourceTableSchema.builder(MapperType.CASSANDRA)
            .setTableName("testTable")
            .addSourceColumnNameToSourceColumnType(
                testIntCol, new SourceColumnType("int", null, null))
            .addSourceColumnNameToSourceColumnType(nullCol, new SourceColumnType("int", null, null))
            .addSourceColumnNameToSourceColumnType(
                extractedNullCol, new SourceColumnType("varchar", null, null))
            .build();

    CassandraSourceRowMapper cassandraSourceRowMapper =
        CassandraSourceRowMapper.builder()
            .setSourceSchemaReference(sourceSchemaReference)
            .setSourceTableSchema(sourceTableSchema)
            .build();

    ImmutableList.Builder<SourceRow> readRowsBuilder = ImmutableList.builder();
    cassandraSourceRowMapper.map(mockResultSet).forEachRemaining(row -> readRowsBuilder.add(row));
    ImmutableList<SourceRow> readRows = readRowsBuilder.build();

    assertThat(
            readRows.stream()
                .map(r -> r.getPayload().toString())
                .sorted()
                .collect(ImmutableList.toImmutableList()))
        .isEqualTo(
            ImmutableList.of(
                "{\"testIntCol\": 42, \"nullCol\": null, \"extractedNullCol\": null}"));
  }

  @Test
  public void testAstraDbSourceRowForNullValues() {
    com.datastax.oss.driver.api.core.cql.Row mockRow =
        Mockito.mock(com.datastax.oss.driver.api.core.cql.Row.class);
    com.datastax.oss.driver.api.core.cql.ResultSet mockResultSet =
        new MockV4Resultset(ImmutableList.of(mockRow));
    final String testIntCol = "testIntCol";
    final String nullCol = "nullCol";
    final String extractedNullCol = "extractedNullCol";

    when(mockRow.getInt(testIntCol)).thenReturn(42);

    when(mockRow.isNull(nullCol)).thenReturn(true);

    when(mockRow.isNull(extractedNullCol)).thenReturn(false);
    when(mockRow.getString(extractedNullCol)).thenReturn(null);

    SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.ofCassandra(
            CassandraSchemaReference.builder().setKeyspaceName(TEST_KEYSPACE).build());

    SourceTableSchema sourceTableSchema =
        SourceTableSchema.builder(MapperType.CASSANDRA)
            .setTableName("testTable")
            .addSourceColumnNameToSourceColumnType(
                testIntCol, new SourceColumnType("int", null, null))
            .addSourceColumnNameToSourceColumnType(nullCol, new SourceColumnType("int", null, null))
            .addSourceColumnNameToSourceColumnType(
                extractedNullCol, new SourceColumnType("varchar", null, null))
            .build();

    AstraDbSourceRowMapper astraDbSourceRowMapper =
        AstraDbSourceRowMapper.builder()
            .setSourceSchemaReference(sourceSchemaReference)
            .setSourceTableSchema(sourceTableSchema)
            .build();

    ImmutableList.Builder<SourceRow> readRowsBuilder = ImmutableList.builder();
    astraDbSourceRowMapper.map(mockResultSet).forEachRemaining(row -> readRowsBuilder.add(row));
    ImmutableList<SourceRow> readRows = readRowsBuilder.build();

    assertThat(
            readRows.stream()
                .map(r -> r.getPayload().toString())
                .sorted()
                .collect(ImmutableList.toImmutableList()))
        .isEqualTo(
            ImmutableList.of(
                "{\"testIntCol\": 42, \"nullCol\": null, \"extractedNullCol\": null}"));
  }

  @Test
  public void testCassandraSourceRowForUnsupportedType() {
    ResultSet mockResultSet = Mockito.mock(ResultSet.class);
    Row mockRow = Mockito.mock(Row.class);
    final String testIntCol = "testIntCol";
    when(mockRow.getInt(testIntCol)).thenReturn(42);
    when(mockResultSet.iterator()).thenReturn(ImmutableList.of(mockRow).stream().iterator());

    SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.ofCassandra(
            CassandraSchemaReference.builder().setKeyspaceName(TEST_KEYSPACE).build());

    SourceTableSchema sourceTableSchema =
        SourceTableSchema.builder(MapperType.CASSANDRA)
            .setTableName("testTable")
            .addSourceColumnNameToSourceColumnType(
                testIntCol, new SourceColumnType("int", null, null))
            .addSourceColumnNameToSourceColumnType(
                "UnSupportedCol1", new SourceColumnType("UnseenColumnType", null, null))
            .addSourceColumnNameToSourceColumnType(
                "UnSupportedCol2", new SourceColumnType("UNSUPPORTED", null, null))
            .build();

    CassandraSourceRowMapper cassandraSourceRowMapper =
        CassandraSourceRowMapper.builder()
            .setSourceSchemaReference(sourceSchemaReference)
            .setSourceTableSchema(sourceTableSchema)
            .build();

    ImmutableList.Builder<SourceRow> readRowsBuilder = ImmutableList.builder();
    cassandraSourceRowMapper.map(mockResultSet).forEachRemaining(row -> readRowsBuilder.add(row));
    ImmutableList<SourceRow> readRows = readRowsBuilder.build();

    assertThat(
            readRows.stream()
                .map(r -> r.getPayload().toString())
                .sorted()
                .collect(ImmutableList.toImmutableList()))
        .isEqualTo(
            ImmutableList.of(
                "{\"testIntCol\": 42, \"UnSupportedCol1\": null, \"UnSupportedCol2\": null}"));
  }

  @Test
  public void testAstraDbSourceRowForUnsupportedType() {
    com.datastax.oss.driver.api.core.cql.Row mockRow =
        Mockito.mock(com.datastax.oss.driver.api.core.cql.Row.class);
    com.datastax.oss.driver.api.core.cql.ResultSet mockResultSet =
        new MockV4Resultset(ImmutableList.of(mockRow));
    final String testIntCol = "testIntCol";
    when(mockRow.getInt(testIntCol)).thenReturn(42);

    SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.ofCassandra(
            CassandraSchemaReference.builder().setKeyspaceName(TEST_KEYSPACE).build());

    SourceTableSchema sourceTableSchema =
        SourceTableSchema.builder(MapperType.CASSANDRA)
            .setTableName("testTable")
            .addSourceColumnNameToSourceColumnType(
                testIntCol, new SourceColumnType("int", null, null))
            .addSourceColumnNameToSourceColumnType(
                "UnSupportedCol1", new SourceColumnType("UnseenColumnType", null, null))
            .addSourceColumnNameToSourceColumnType(
                "UnSupportedCol2", new SourceColumnType("UNSUPPORTED", null, null))
            .build();

    AstraDbSourceRowMapper astraDbSourceRowMapper =
        AstraDbSourceRowMapper.builder()
            .setSourceSchemaReference(sourceSchemaReference)
            .setSourceTableSchema(sourceTableSchema)
            .build();

    ImmutableList.Builder<SourceRow> readRowsBuilder = ImmutableList.builder();
    astraDbSourceRowMapper.map(mockResultSet).forEachRemaining(row -> readRowsBuilder.add(row));
    ImmutableList<SourceRow> readRows = readRowsBuilder.build();

    assertThat(
            readRows.stream()
                .map(r -> r.getPayload().toString())
                .sorted()
                .collect(ImmutableList.toImmutableList()))
        .isEqualTo(
            ImmutableList.of(
                "{\"testIntCol\": 42, \"UnSupportedCol1\": null, \"UnSupportedCol2\": null}"));
  }

  private class MockV4Resultset implements com.datastax.oss.driver.api.core.cql.ResultSet {
    private ImmutableList<com.datastax.oss.driver.api.core.cql.Row> mockRows;

    private MockV4Resultset(ImmutableList<com.datastax.oss.driver.api.core.cql.Row> mockRows) {
      this.mockRows = mockRows;
    }

    @NotNull
    @Override
    public ColumnDefinitions getColumnDefinitions() {
      return null;
    }

    @NotNull
    @Override
    public List<ExecutionInfo> getExecutionInfos() {
      return null;
    }

    @Override
    public boolean isFullyFetched() {
      return false;
    }

    @Override
    public int getAvailableWithoutFetching() {
      return 0;
    }

    @Override
    public boolean wasApplied() {
      return false;
    }

    /**
     * Returns an iterator over elements of type {@code T}.
     *
     * @return an Iterator.
     */
    @NotNull
    @Override
    public Iterator<com.datastax.oss.driver.api.core.cql.Row> iterator() {
      return this.mockRows.iterator();
    }
  }
}
