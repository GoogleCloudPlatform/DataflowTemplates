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

import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.BASIC_TEST_TABLE;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.PRIMITIVE_TYPES_TABLE;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_KEYSPACE;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.schema.CassandraSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.datasource.DataSource;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SchemaDiscovery;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchema;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import org.apache.beam.sdk.io.cassandra.CassandraIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link CassandraIOWrapperFactory}. */
@RunWith(MockitoJUnitRunner.class)
public class CassandraIOWrapperFactoryTest {
  private MockedStatic<CassandraIOWrapperHelper> mockCassandraIoWrapperHelper;
  private static final String TEST_BUCKET_CASSANDRA_CONFIG_CONF =
      "gs://smt-test-bucket/cassandraConfig.conf";
  private static final ImmutableList<String> TABLES_TO_READ =
      ImmutableList.of(BASIC_TEST_TABLE, PRIMITIVE_TYPES_TABLE);
  @Mock SourceSchema mockSourceSchema;

  @Before
  public void setup() {
    mockCassandraIoWrapperHelper = mockStatic(CassandraIOWrapperHelper.class);

    String testClusterName = "testCluster";
    InetSocketAddress testHost = new InetSocketAddress("127.0.0.1", 9042);
    String testLocalDC = "datacenter1";
    DataSource dataSource =
        DataSource.ofCassandra(
            CassandraDataSource.builder()
                .setOptionsMap(OptionsMap.driverDefaults())
                .setClusterName(testClusterName)
                .setContactPoints(ImmutableList.of(testHost))
                .setLocalDataCenter(testLocalDC)
                .overrideOptionInOptionsMap(TypedDriverOption.SESSION_KEYSPACE, TEST_KEYSPACE)
                .build());

    SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.ofCassandra(
            CassandraSchemaReference.builder()
                .setKeyspaceName(dataSource.cassandra().loggedKeySpace())
                .build());

    SchemaDiscovery schemaDiscovery = CassandraIOWrapperHelper.buildSchemaDiscovery();
    SourceTableReference mockSourceTableReference = Mockito.mock(SourceTableReference.class);
    CassandraIO.Read<SourceRow> mockTableReader = Mockito.mock(CassandraIO.Read.class);
    ImmutableMap<SourceTableReference, PTransform<PBegin, PCollection<SourceRow>>>
        mockTableReaders = ImmutableMap.of(mockSourceTableReference, mockTableReader);

    mockCassandraIoWrapperHelper
        .when(
            () -> CassandraIOWrapperHelper.buildDataSource(TEST_BUCKET_CASSANDRA_CONFIG_CONF, null))
        .thenReturn(dataSource);
    mockCassandraIoWrapperHelper
        .when(() -> CassandraIOWrapperHelper.buildSchemaDiscovery())
        .thenReturn(schemaDiscovery);
    mockCassandraIoWrapperHelper
        .when(
            () ->
                CassandraIOWrapperHelper.getTablesToRead(
                    TABLES_TO_READ, dataSource, schemaDiscovery, sourceSchemaReference))
        .thenReturn(TABLES_TO_READ);
    mockCassandraIoWrapperHelper
        .when(
            () ->
                CassandraIOWrapperHelper.getSourceSchema(
                    schemaDiscovery, dataSource, sourceSchemaReference, TABLES_TO_READ))
        .thenReturn(mockSourceSchema);
    mockCassandraIoWrapperHelper
        .when(() -> CassandraIOWrapperHelper.getTableReaders(dataSource, mockSourceSchema))
        .thenReturn(mockTableReaders);
  }

  @After
  public void cleanup() {
    mockCassandraIoWrapperHelper.close();
    mockCassandraIoWrapperHelper = null;
  }

  @Test
  public void testCassandraIoWrapperFactoryBasic() {
    String testConfigPath = TEST_BUCKET_CASSANDRA_CONFIG_CONF;
    SourceDbToSpannerOptions mockOptions =
        mock(SourceDbToSpannerOptions.class, Mockito.withSettings().serializable());
    when(mockOptions.getSourceDbDialect()).thenReturn("CASSANDRA");
    when(mockOptions.getSourceConfigURL()).thenReturn(testConfigPath);
    when(mockOptions.getNumPartitions()).thenReturn(null);
    CassandraIOWrapperFactory cassandraIOWrapperFactory =
        CassandraIOWrapperFactory.fromPipelineOptions(mockOptions);
    assertThat(cassandraIOWrapperFactory.gcsConfigPath()).isEqualTo(testConfigPath);
    assertThat(cassandraIOWrapperFactory.getIOWrapper(TABLES_TO_READ, null).discoverTableSchema())
        .isEqualTo(mockSourceSchema);
  }

  @Test
  public void testCassandraIoWrapperFactoryExceptions() {
    String testConfigPath = "smt-test-bucket/test-conf.conf";
    SourceDbToSpannerOptions mockOptions =
        mock(SourceDbToSpannerOptions.class, Mockito.withSettings().serializable());
    when(mockOptions.getSourceDbDialect()).thenReturn("MYSQL").thenReturn("CASSANDRA");
    when(mockOptions.getSourceConfigURL()).thenReturn(testConfigPath);
    assertThrows(
        IllegalArgumentException.class,
        () -> CassandraIOWrapperFactory.fromPipelineOptions(mockOptions));
    assertThrows(
        IllegalArgumentException.class,
        () -> CassandraIOWrapperFactory.fromPipelineOptions(mockOptions));
  }
}
