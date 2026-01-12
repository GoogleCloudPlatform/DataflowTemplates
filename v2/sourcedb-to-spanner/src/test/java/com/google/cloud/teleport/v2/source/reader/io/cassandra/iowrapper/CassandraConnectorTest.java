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

import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_CONFIG;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_CQLSH;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_KEYSPACE;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

import com.datastax.astra.sdk.db.DatabaseClient;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.google.cloud.teleport.v2.source.reader.auth.dbauth.GuardedStringValueProvider;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.schema.CassandraSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.SharedEmbeddedCassandra;
import java.io.IOException;
import org.apache.beam.sdk.io.astra.db.CqlSessionHolder;
import org.apache.beam.sdk.options.ValueProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link CassandraConnector}. */
@RunWith(MockitoJUnitRunner.class)
public class CassandraConnectorTest {

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
  public void testOss() {
    CassandraSchemaReference testSchemaReference =
        CassandraSchemaReference.builder().setKeyspaceName(TEST_KEYSPACE).build();

    CassandraSchemaReference testSchemaReferenceNullKeySpace =
        CassandraSchemaReference.builder().build();

    CassandraDataSource cassandraDataSource =
        CassandraDataSource.ofOss(
            CassandraDataSourceOss.builder()
                .setClusterName(sharedEmbeddedCassandra.getInstance().getClusterName())
                .setOptionsMap(OptionsMap.driverDefaults())
                .setContactPoints(sharedEmbeddedCassandra.getInstance().getContactPoints())
                .setLocalDataCenter(sharedEmbeddedCassandra.getInstance().getLocalDataCenter())
                .build());

    try (CassandraConnector cassandraConnectorWithSchemaReference =
        new CassandraConnector(cassandraDataSource, testSchemaReference)) {
      assertThat(
              cassandraConnectorWithSchemaReference
                  .getSession()
                  .getMetadata()
                  .getClusterName()
                  .get())
          .isEqualTo(sharedEmbeddedCassandra.getInstance().getClusterName());
      assertThat(cassandraConnectorWithSchemaReference.getSession().getKeyspace().get().asCql(true))
          .isEqualTo(TEST_KEYSPACE);
    }
    try (CassandraConnector cassandraConnectorWithNullKeySpace =
        new CassandraConnector(cassandraDataSource, testSchemaReferenceNullKeySpace)) {
      assertThat(
              cassandraConnectorWithNullKeySpace.getSession().getMetadata().getClusterName().get())
          .isEqualTo(sharedEmbeddedCassandra.getInstance().getClusterName());
      assertThat(cassandraConnectorWithNullKeySpace.getSession().getKeyspace()).isEmpty();
    }
  }

  @Test
  public void testAstra() {
    CqlSession mockCqlSession = Mockito.mock(CqlSession.class);
    ValueProvider<String> testAstraDbToken = GuardedStringValueProvider.create("AstraCS:testToken");
    ValueProvider<byte[]> testAstraDbSecureBundle =
        ValueProvider.StaticValueProvider.of(new byte[] {});
    ValueProvider<String> testAstraDbKeySpace =
        ValueProvider.StaticValueProvider.of("testKeySpace");
    String testAstraDbRegion = "testRegion";
    String testAstraDBID = "testID";
    try (MockedConstruction<DatabaseClient> mockedConstruction =
        mockConstruction(
            DatabaseClient.class,
            (mock, context) -> {
              when(mock.exists()).thenReturn(true);
              when(mock.downloadSecureConnectBundle(testAstraDbRegion))
                  .thenReturn(testAstraDbSecureBundle.get());
            })) {
      try (MockedStatic<CqlSessionHolder> mockedStatic =
          Mockito.mockStatic(CqlSessionHolder.class)) {
        mockedStatic
            .when(
                () ->
                    CqlSessionHolder.getCqlSession(
                        testAstraDbToken, testAstraDbSecureBundle, testAstraDbKeySpace))
            .thenReturn(mockCqlSession);
        CassandraDataSource cassandraDataSource =
            CassandraDataSource.ofAstra(
                AstraDbDataSource.builder()
                    .setAstraDbRegion(testAstraDbRegion)
                    .setKeySpace(testAstraDbKeySpace.get())
                    .setDatabaseId(testAstraDBID)
                    .setAstraToken(testAstraDbToken.get())
                    .build());
        CassandraSchemaReference cassandraSchemaReference =
            CassandraSchemaReference.builder().setKeyspaceName(testAstraDbKeySpace.get()).build();
        CassandraConnector cassandraConnector =
            new CassandraConnector(cassandraDataSource, cassandraSchemaReference);
        assertThat(cassandraConnector.getSession()).isEqualTo(mockCqlSession);
      }
    }
  }
}
