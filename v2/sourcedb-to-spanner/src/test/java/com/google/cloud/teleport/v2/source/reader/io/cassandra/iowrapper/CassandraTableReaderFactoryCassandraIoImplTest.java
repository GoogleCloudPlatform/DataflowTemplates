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
package com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper;

import static com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper.CassandraTableReaderFactoryCassandraIoImpl.DEFAULT_CONNECTION_TIMEOUT_MILLIS;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper.CassandraTableReaderFactoryCassandraIoImpl.DEFAULT_CONSISTENCY;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper.CassandraTableReaderFactoryCassandraIoImpl.DEFAULT_READ_TIMEOUT_MILLIS;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.PRIMITIVE_TYPES_TABLE;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.PRIMITIVE_TYPES_TABLE_ROW_COUNT;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_CONFIG;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_CQLSH;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_KEYSPACE;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.dtsx.astra.sdk.db.DatabaseClient;
import com.google.cloud.teleport.v2.source.reader.auth.dbauth.GuardedStringValueProvider;
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
import com.google.cloud.teleport.v2.spanner.migrations.utils.JarFileReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import org.apache.beam.sdk.io.astra.db.AstraDbIO;
import org.apache.beam.sdk.io.astra.db.CqlSessionHolder;
import org.apache.beam.sdk.io.localcassandra.CassandraIO;
import org.apache.beam.sdk.io.localcassandra.CassandraIO.Read;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link CassandraTableReaderFactoryCassandraIoImpl}. */
@RunWith(MockitoJUnitRunner.class)
public class CassandraTableReaderFactoryCassandraIoImplTest {

  private static SharedEmbeddedCassandra sharedEmbeddedCassandra = null;

  @Rule public final transient TestPipeline testPipeline = TestPipeline.create();

  @BeforeClass
  public static void startEmbeddedCassandra() throws IOException {
    if (sharedEmbeddedCassandra == null) {
      sharedEmbeddedCassandra = new SharedEmbeddedCassandra(TEST_CONFIG, TEST_CQLSH);
    }
  }

  @AfterClass
  public static void stopEmbeddedCassandra() throws Exception {
    if (sharedEmbeddedCassandra != null) {
      sharedEmbeddedCassandra.close();
      sharedEmbeddedCassandra = null;
    }
  }

  @Test
  public void testCassandraTableReaderFactoryOssBasic() throws RetriableSchemaDiscoveryException {

    SourceSchemaReference cassandraSchemaReference =
        SourceSchemaReference.ofCassandra(
            CassandraSchemaReference.builder().setKeyspaceName(TEST_KEYSPACE).build());

    DataSource dataSource =
        DataSource.ofCassandra(
            CassandraDataSource.ofOss(
                CassandraDataSourceOss.builder()
                    .setOptionsMap(OptionsMap.driverDefaults())
                    .setClusterName(sharedEmbeddedCassandra.getInstance().getClusterName())
                    .setContactPoints(sharedEmbeddedCassandra.getInstance().getContactPoints())
                    .setLocalDataCenter(sharedEmbeddedCassandra.getInstance().getLocalDataCenter())
                    .overrideOptionInOptionsMap(TypedDriverOption.SESSION_KEYSPACE, TEST_KEYSPACE)
                    .build()));
    CassandraSchemaDiscovery cassandraSchemaDiscovery = new CassandraSchemaDiscovery();
    ImmutableMap<String, ImmutableMap<String, SourceColumnType>> discoverTableSchema =
        cassandraSchemaDiscovery.discoverTableSchema(
            dataSource, cassandraSchemaReference, ImmutableList.of(PRIMITIVE_TYPES_TABLE));

    SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.ofCassandra(
            CassandraSchemaReference.builder()
                .setKeyspaceName(dataSource.cassandra().loggedKeySpace())
                .build());
    SourceTableSchema.Builder sourceTableSchemaBuilder =
        SourceTableSchema.builder(MapperType.CASSANDRA).setTableName(PRIMITIVE_TYPES_TABLE);
    discoverTableSchema
        .get(PRIMITIVE_TYPES_TABLE)
        .forEach(
            (colName, colType) ->
                sourceTableSchemaBuilder.addSourceColumnNameToSourceColumnType(colName, colType));
    SourceTableSchema sourceTableSchema = sourceTableSchemaBuilder.build();

    PTransform<PBegin, PCollection<SourceRow>> tableReader =
        new CassandraTableReaderFactoryCassandraIoImpl()
            .getTableReader(dataSource.cassandra(), sourceSchemaReference, sourceTableSchema);
    PCollection<SourceRow> output = testPipeline.apply(tableReader);
    PAssert.that(output.apply(Count.globally()))
        .containsInAnyOrder(PRIMITIVE_TYPES_TABLE_ROW_COUNT);
    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void testSetCredentialsOss() throws FileNotFoundException {

    String testGcsPath = "gs://smt-test-bucket/cassandraConfig.conf";
    URL testUrl = Resources.getResource("CassandraUT/test-cassandra-config.conf");

    CassandraIO.Read<SourceRow> mockCassandraIORead = mock(CassandraIO.Read.class);
    try (MockedStatic mockFileReader = mockStatic(JarFileReader.class)) {
      String testUserName = "testUserName";
      String testPassword = "testPassword1234@";

      mockFileReader
          .when(() -> JarFileReader.saveFilesLocally(testGcsPath))
          .thenReturn(new URL[] {testUrl});
      when(mockCassandraIORead.withUsername(testUserName)).thenReturn(mockCassandraIORead);
      when(mockCassandraIORead.withPassword(testPassword)).thenReturn(mockCassandraIORead);

      CassandraDataSource cassandraDataSource =
          CassandraDataSource.ofOss(
              CassandraDataSourceOss.builder().setOptionsMapFromGcsFile(testGcsPath).build());
      new CassandraTableReaderFactoryCassandraIoImpl()
          .setCredentials(
              mockCassandraIORead,
              cassandraDataSource
                  .oss()
                  .driverConfigLoader()
                  .getInitialConfig()
                  .getDefaultProfile());
      verify(mockCassandraIORead, times(1)).withUsername(testUserName);
      verify(mockCassandraIORead, times(1)).withPassword(testPassword);
    }
  }

  @Test
  public void testDefaultsOss() {
    final Duration testTimeout = Duration.ofMillis(42);
    final String testConsistency = "ONE";
    DataSource dataSourceWithoutDefaults =
        DataSource.ofCassandra(
            CassandraDataSource.ofOss(
                CassandraDataSourceOss.builder()
                    .setOptionsMap(OptionsMap.driverDefaults())
                    .setClusterName(sharedEmbeddedCassandra.getInstance().getClusterName())
                    .setContactPoints(sharedEmbeddedCassandra.getInstance().getContactPoints())
                    .setLocalDataCenter(sharedEmbeddedCassandra.getInstance().getLocalDataCenter())
                    .overrideOptionInOptionsMap(TypedDriverOption.SESSION_KEYSPACE, TEST_KEYSPACE)
                    .overrideOptionInOptionsMap(
                        TypedDriverOption.CONNECTION_CONNECT_TIMEOUT, testTimeout)
                    .overrideOptionInOptionsMap(TypedDriverOption.REQUEST_TIMEOUT, testTimeout)
                    .overrideOptionInOptionsMap(
                        TypedDriverOption.REQUEST_CONSISTENCY, testConsistency)
                    .build()));
    DriverExecutionProfile profileWithoutDefaults =
        dataSourceWithoutDefaults
            .cassandra()
            .oss()
            .driverConfigLoader()
            .getInitialConfig()
            .getDefaultProfile();
    DriverExecutionProfile profileWithDefaults =
        dataSourceWithoutDefaults
            .cassandra()
            .oss()
            .driverConfigLoader()
            .getInitialConfig()
            .getDefaultProfile()
            .without(TypedDriverOption.REQUEST_CONSISTENCY.getRawOption())
            .without(TypedDriverOption.CONNECTION_CONNECT_TIMEOUT.getRawOption())
            .without(TypedDriverOption.REQUEST_TIMEOUT.getRawOption());

    assertThat(
            CassandraTableReaderFactoryCassandraIoImpl.getConnectionTimeout(profileWithoutDefaults))
        .isEqualTo((int) testTimeout.toMillis());
    assertThat(CassandraTableReaderFactoryCassandraIoImpl.getReadTimeout(profileWithoutDefaults))
        .isEqualTo((int) testTimeout.toMillis());
    assertThat(
            CassandraTableReaderFactoryCassandraIoImpl.getConsistencyLevel(profileWithoutDefaults))
        .isEqualTo(testConsistency);
    assertThat(CassandraTableReaderFactoryCassandraIoImpl.getConnectionTimeout(profileWithDefaults))
        .isEqualTo(DEFAULT_CONNECTION_TIMEOUT_MILLIS);
    assertThat(CassandraTableReaderFactoryCassandraIoImpl.getReadTimeout(profileWithDefaults))
        .isEqualTo(DEFAULT_READ_TIMEOUT_MILLIS);
    assertThat(CassandraTableReaderFactoryCassandraIoImpl.getConsistencyLevel(profileWithDefaults))
        .isEqualTo(DEFAULT_CONSISTENCY);
  }

  @Test
  public void testSetNumPartitionsOss() {

    Integer testNumberOfSplits = 42;
    CassandraIO.Read<SourceRow> mockCassandraIORead = mock(CassandraIO.Read.class);
    when(mockCassandraIORead.withMinNumberOfSplits(testNumberOfSplits))
        .thenReturn(mockCassandraIORead);

    CassandraDataSource cassandraDataSource =
        CassandraDataSource.ofOss(
            CassandraDataSourceOss.builder()
                .setClusterName("testCluster")
                .setOptionsMap(OptionsMap.driverDefaults())
                .build());
    Read<SourceRow> retWithoutPartitions =
        CassandraTableReaderFactoryCassandraIoImpl.setNumPartitionsOss(
            mockCassandraIORead, cassandraDataSource.oss(), "testTable");
    assertThat(retWithoutPartitions).isEqualTo(mockCassandraIORead);
    Read<SourceRow> retWithZeroPartitions =
        CassandraTableReaderFactoryCassandraIoImpl.setNumPartitionsOss(
            mockCassandraIORead,
            cassandraDataSource.oss().toBuilder().setNumPartitions(0).build(),
            "testTable");
    assertThat(retWithZeroPartitions).isEqualTo(mockCassandraIORead);
    Read<SourceRow> retWithPartitions =
        CassandraTableReaderFactoryCassandraIoImpl.setNumPartitionsOss(
            mockCassandraIORead,
            cassandraDataSource.oss().toBuilder().setNumPartitions(testNumberOfSplits).build(),
            "testTable");
    assertThat(retWithPartitions).isEqualTo(mockCassandraIORead);
    verify(mockCassandraIORead, times(1)).withMinNumberOfSplits(testNumberOfSplits);
  }

  @Test
  public void testCassandraTableReaderFactoryAstraBasic() throws RetriableSchemaDiscoveryException {

    SourceSchemaReference cassandraSchemaReference =
        SourceSchemaReference.ofCassandra(
            CassandraSchemaReference.builder().setKeyspaceName(TEST_KEYSPACE).build());
    final Duration testTimeout = Duration.ofMillis(42);
    final String testConsistency = "ONE";

    CqlSession cqlSession =
        new CassandraConnector(
                CassandraDataSource.ofOss(
                    CassandraDataSourceOss.builder()
                        .setOptionsMap(OptionsMap.driverDefaults())
                        .setClusterName(sharedEmbeddedCassandra.getInstance().getClusterName())
                        .setContactPoints(sharedEmbeddedCassandra.getInstance().getContactPoints())
                        .setLocalDataCenter(
                            sharedEmbeddedCassandra.getInstance().getLocalDataCenter())
                        .overrideOptionInOptionsMap(
                            TypedDriverOption.SESSION_KEYSPACE, TEST_KEYSPACE)
                        .overrideOptionInOptionsMap(
                            TypedDriverOption.CONNECTION_CONNECT_TIMEOUT, testTimeout)
                        .overrideOptionInOptionsMap(TypedDriverOption.REQUEST_TIMEOUT, testTimeout)
                        .overrideOptionInOptionsMap(
                            TypedDriverOption.REQUEST_CONSISTENCY, testConsistency)
                        .build()),
                cassandraSchemaReference.cassandra())
            .getSession();
    ValueProvider<String> testAstraDbToken = GuardedStringValueProvider.create("AstraCS:testToken");
    ValueProvider<byte[]> testAstraDbSecureBundle =
        ValueProvider.StaticValueProvider.of(new byte[] {});
    ValueProvider<String> testAstraDbKeySpace =
        ValueProvider.StaticValueProvider.of("testKeySpace");
    String testAstraDbRegion = "testRegion";
    String testAstraDBID = "testID";
    SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.ofCassandra(
            CassandraSchemaReference.builder().setKeyspaceName(TEST_KEYSPACE).build());
    try (MockedConstruction<DatabaseClient> mockedConstruction =
        mockConstruction(
            DatabaseClient.class,
            (mock, context) -> {
              when(mock.exist()).thenReturn(true);
              when(mock.downloadSecureConnectBundle(testAstraDbRegion))
                  .thenReturn(testAstraDbSecureBundle.get());
            })) {
      try (MockedStatic<CqlSessionHolder> mockedStatic =
          Mockito.mockStatic(CqlSessionHolder.class)) {
        CassandraDataSource cassandraDataSource =
            CassandraDataSource.ofAstra(
                AstraDbDataSource.builder()
                    .setAstraDbRegion(testAstraDbRegion)
                    .setKeySpace(testAstraDbKeySpace.get())
                    .setDatabaseId(testAstraDBID)
                    .setAstraToken(testAstraDbToken.get())
                    .build());
        mockedStatic
            .when(
                () ->
                    CqlSessionHolder.getCqlSession(
                        testAstraDbToken, testAstraDbSecureBundle, testAstraDbKeySpace))
            .thenReturn(cqlSession);
        CassandraSchemaDiscovery cassandraSchemaDiscovery = new CassandraSchemaDiscovery();
        ImmutableMap<String, ImmutableMap<String, SourceColumnType>> discoverTableSchema =
            cassandraSchemaDiscovery.discoverTableSchema(
                DataSource.ofCassandra(cassandraDataSource),
                cassandraSchemaReference,
                ImmutableList.of(PRIMITIVE_TYPES_TABLE));

        SourceTableSchema.Builder sourceTableSchemaBuilder =
            SourceTableSchema.builder(MapperType.CASSANDRA).setTableName(PRIMITIVE_TYPES_TABLE);
        discoverTableSchema
            .get(PRIMITIVE_TYPES_TABLE)
            .forEach(
                (colName, colType) ->
                    sourceTableSchemaBuilder.addSourceColumnNameToSourceColumnType(
                        colName, colType));
        SourceTableSchema sourceTableSchema = sourceTableSchemaBuilder.build();
        PTransform<PBegin, PCollection<SourceRow>> tableReader =
            new CassandraTableReaderFactoryCassandraIoImpl()
                .getTableReader(cassandraDataSource, sourceSchemaReference, sourceTableSchema);
        assertThat(tableReader).isInstanceOf(AstraDbIO.Read.class);
      }
    }
  }

  @Test
  public void testSetNumPartitionsAstra() {

    Integer testNumberOfSplits = 42;
    AstraDbIO.Read<SourceRow> mockAstraDbIORead = mock(AstraDbIO.Read.class);
    when(mockAstraDbIORead.withMinNumberOfSplits(testNumberOfSplits)).thenReturn(mockAstraDbIORead);

    ValueProvider<String> testAstraDbToken = GuardedStringValueProvider.create("AstraCS:testToken");
    ValueProvider<byte[]> testAstraDbSecureBundle =
        ValueProvider.StaticValueProvider.of(new byte[] {});
    ValueProvider<String> testAstraDbKeySpace =
        ValueProvider.StaticValueProvider.of("testKeySpace");
    String testAstraDbRegion = "testRegion";
    String testAstraDBID = "testID";

    CassandraDataSource cassandraDataSource =
        CassandraDataSource.ofAstra(
            AstraDbDataSource.builder()
                .setAstraDbRegion(testAstraDbRegion)
                .setKeySpace(testAstraDbKeySpace.get())
                .setDatabaseId(testAstraDBID)
                .setAstraToken(testAstraDbToken.get())
                .build());
    AstraDbIO.Read<SourceRow> retWithoutPartitions =
        CassandraTableReaderFactoryCassandraIoImpl.setNumPartitionsAstra(
            mockAstraDbIORead, cassandraDataSource.astra(), "testTable");
    assertThat(retWithoutPartitions).isEqualTo(mockAstraDbIORead);

    AstraDbIO.Read<SourceRow> retWithZeroPartitions =
        CassandraTableReaderFactoryCassandraIoImpl.setNumPartitionsAstra(
            mockAstraDbIORead,
            cassandraDataSource.astra().toBuilder().setNumPartitions(0).build(),
            "testTable");
    assertThat(retWithZeroPartitions).isEqualTo(mockAstraDbIORead);
    AstraDbIO.Read<SourceRow> retWithPartitions =
        CassandraTableReaderFactoryCassandraIoImpl.setNumPartitionsAstra(
            mockAstraDbIORead,
            cassandraDataSource.astra().toBuilder().setNumPartitions(testNumberOfSplits).build(),
            "testTable");
    assertThat(retWithPartitions).isEqualTo(mockAstraDbIORead);
    verify(mockAstraDbIORead, times(1)).withMinNumberOfSplits(testNumberOfSplits);
  }
}
