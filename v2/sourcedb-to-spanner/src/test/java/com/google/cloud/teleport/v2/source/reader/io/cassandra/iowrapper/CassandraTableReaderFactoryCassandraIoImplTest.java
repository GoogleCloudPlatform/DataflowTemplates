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

import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.PRIMITIVE_TYPES_TABLE;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.PRIMITIVE_TYPES_TABLE_ROW_COUNT;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_CONFIG;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_CQLSH;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_KEYSPACE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
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
import org.apache.beam.sdk.io.localcassandra.CassandraIO;
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
import org.mockito.MockedStatic;
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
  public void testCassandraTableReaderFactoryBasic() throws RetriableSchemaDiscoveryException {

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
  public void testSetCredentials() throws FileNotFoundException {

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
          CassandraDataSource.builder().setOptionsMapFromGcsFile(testGcsPath).build();
      new CassandraTableReaderFactoryCassandraIoImpl()
          .setCredentials(
              mockCassandraIORead,
              cassandraDataSource.driverConfigLoader().getInitialConfig().getDefaultProfile());
      verify(mockCassandraIORead, times(1)).withUsername(testUserName);
      verify(mockCassandraIORead, times(1)).withPassword(testPassword);
    }
  }
}
