/*
 * Copyright (C) 2025 Google LLC
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
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.BASIC_TEST_TABLE;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.PRIMITIVE_TYPES_TABLE;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_CONFIG;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_CQLSH;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_KEYSPACE;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_TABLES;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.schema.CassandraSchemaDiscovery;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.schema.CassandraSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.SharedEmbeddedCassandra;
import com.google.cloud.teleport.v2.source.reader.io.datasource.DataSource;
import com.google.cloud.teleport.v2.source.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchema;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableReference;
import com.google.cloud.teleport.v2.spanner.migrations.utils.JarFileReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.cassandra.CassandraIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link CassandraIOWrapperHelper}. */
@RunWith(MockitoJUnitRunner.class)
public class CassandraIOWrapperHelperTest {

  private static SharedEmbeddedCassandra sharedEmbeddedCassandra = null;

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
  public void testBuildDataSource() {

    String testGcsPath = "gs://smt-test-bucket/cassandraConfig.conf";
    URL testUrl = Resources.getResource("CassandraUT/test-cassandra-config.conf");

    CassandraIO.Read<SourceRow> mockCassandraIORead = mock(CassandraIO.Read.class);
    try (MockedStatic mockFileReader = mockStatic(JarFileReader.class)) {

      mockFileReader
          .when(() -> JarFileReader.saveFilesLocally(testGcsPath))
          .thenReturn(new URL[] {testUrl})
          .thenReturn(new URL[] {testUrl})
          /* Empty URL List to test FileNotFoundException handling. */
          .thenReturn(new URL[] {});

      DataSource dataSource = CassandraIOWrapperHelper.buildDataSource(testGcsPath, null);
      assertThat(dataSource.cassandra().loggedKeySpace()).isEqualTo("test-keyspace");
      assertThat(dataSource.cassandra().localDataCenter()).isEqualTo("datacenter1");
      assertThat(dataSource.cassandra().numPartitions()).isEqualTo(null);
      assertThat(
              CassandraIOWrapperHelper.buildDataSource(testGcsPath, 42).cassandra().numPartitions())
          .isEqualTo(42);
      assertThrows(
          SchemaDiscoveryException.class,
          () -> CassandraIOWrapperHelper.buildDataSource(testGcsPath, null));
    }
  }

  @Test
  public void testTablesToRead() {

    SourceSchemaReference cassandraSchemaReference =
        SourceSchemaReference.ofCassandra(
            CassandraSchemaReference.builder().setKeyspaceName(TEST_KEYSPACE).build());

    DataSource dataSource =
        DataSource.ofCassandra(
            CassandraDataSource.builder()
                .setOptionsMap(OptionsMap.driverDefaults())
                .setClusterName(sharedEmbeddedCassandra.getInstance().getClusterName())
                .setContactPoints(sharedEmbeddedCassandra.getInstance().getContactPoints())
                .setLocalDataCenter(sharedEmbeddedCassandra.getInstance().getLocalDataCenter())
                .overrideOptionInOptionsMap(TypedDriverOption.SESSION_KEYSPACE, TEST_KEYSPACE)
                .build());
    CassandraSchemaDiscovery cassandraSchemaDiscovery = new CassandraSchemaDiscovery();
    assertThat(
            CassandraIOWrapperHelper.getTablesToRead(
                List.of(),
                dataSource,
                CassandraIOWrapperHelper.buildSchemaDiscovery(),
                cassandraSchemaReference))
        .isEqualTo(TEST_TABLES);
    assertThat(
            CassandraIOWrapperHelper.getTablesToRead(
                List.of(BASIC_TEST_TABLE),
                dataSource,
                CassandraIOWrapperHelper.buildSchemaDiscovery(),
                cassandraSchemaReference))
        .isEqualTo(List.of(BASIC_TEST_TABLE));

    assertThat(
            CassandraIOWrapperHelper.getTablesToRead(
                List.of(BASIC_TEST_TABLE, "Non-existing-table"),
                dataSource,
                CassandraIOWrapperHelper.buildSchemaDiscovery(),
                cassandraSchemaReference))
        .isEqualTo(List.of(BASIC_TEST_TABLE));
  }

  @Test
  public void testSourceSchema() {

    SourceSchemaReference cassandraSchemaReference =
        SourceSchemaReference.ofCassandra(
            CassandraSchemaReference.builder().setKeyspaceName(TEST_KEYSPACE).build());

    DataSource dataSource =
        DataSource.ofCassandra(
            CassandraDataSource.builder()
                .setOptionsMap(OptionsMap.driverDefaults())
                .setClusterName(sharedEmbeddedCassandra.getInstance().getClusterName())
                .setContactPoints(sharedEmbeddedCassandra.getInstance().getContactPoints())
                .setLocalDataCenter(sharedEmbeddedCassandra.getInstance().getLocalDataCenter())
                .overrideOptionInOptionsMap(TypedDriverOption.SESSION_KEYSPACE, TEST_KEYSPACE)
                .build());
    CassandraSchemaDiscovery cassandraSchemaDiscovery = new CassandraSchemaDiscovery();
    SourceSchema sourceSchema =
        CassandraIOWrapperHelper.getSourceSchema(
            CassandraIOWrapperHelper.buildSchemaDiscovery(),
            dataSource,
            cassandraSchemaReference,
            ImmutableList.of(BASIC_TEST_TABLE, PRIMITIVE_TYPES_TABLE));
    assertThat(sourceSchema.schemaReference()).isEqualTo(cassandraSchemaReference);
    assertThat(sourceSchema.tableSchemas().asList().stream().count()).isEqualTo(2);
  }

  @Test
  public void testTableReaders() {

    SourceSchemaReference cassandraSchemaReference =
        SourceSchemaReference.ofCassandra(
            CassandraSchemaReference.builder().setKeyspaceName(TEST_KEYSPACE).build());

    DataSource dataSource =
        DataSource.ofCassandra(
            CassandraDataSource.builder()
                .setOptionsMap(OptionsMap.driverDefaults())
                .setClusterName(sharedEmbeddedCassandra.getInstance().getClusterName())
                .setContactPoints(sharedEmbeddedCassandra.getInstance().getContactPoints())
                .setLocalDataCenter(sharedEmbeddedCassandra.getInstance().getLocalDataCenter())
                .overrideOptionInOptionsMap(TypedDriverOption.SESSION_KEYSPACE, TEST_KEYSPACE)
                .build());
    CassandraSchemaDiscovery cassandraSchemaDiscovery = new CassandraSchemaDiscovery();
    SourceSchema sourceSchema =
        CassandraIOWrapperHelper.getSourceSchema(
            CassandraIOWrapperHelper.buildSchemaDiscovery(),
            dataSource,
            cassandraSchemaReference,
            ImmutableList.of(BASIC_TEST_TABLE, PRIMITIVE_TYPES_TABLE));
    ImmutableMap<SourceTableReference, PTransform<PBegin, PCollection<SourceRow>>> tableReraders =
        CassandraIOWrapperHelper.getTableReaders(dataSource, sourceSchema);
    assertThat(
            tableReraders.keySet().stream()
                .map(t -> t.sourceTableName())
                .collect(Collectors.toList()))
        .isEqualTo(List.of(BASIC_TEST_TABLE, PRIMITIVE_TYPES_TABLE));
  }
}
