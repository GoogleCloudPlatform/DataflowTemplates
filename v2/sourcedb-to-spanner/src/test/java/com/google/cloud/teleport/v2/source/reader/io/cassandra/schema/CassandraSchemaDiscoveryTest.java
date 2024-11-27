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
package com.google.cloud.teleport.v2.source.reader.io.cassandra.schema;

import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_CONFIG;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_CQLSH;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_KEYSPACE;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_TABLES;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_TABLE_SCHEMA;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper.CassandraDataSource;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.SharedEmbeddedCassandra;
import com.google.cloud.teleport.v2.source.reader.io.datasource.DataSource;
import com.google.cloud.teleport.v2.source.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.JdbcDataSource;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link CassandraSchemaDiscovery}. */
@RunWith(MockitoJUnitRunner.class)
public class CassandraSchemaDiscoveryTest {

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
  public void testDiscoverTablesBasic() throws IOException, RetriableSchemaDiscoveryException {

    SourceSchemaReference cassandraSchemaReference =
        SourceSchemaReference.ofCassandra(
            CassandraSchemaReference.builder().setKeyspaceName(TEST_KEYSPACE).build());

    DataSource cassandraDataSource =
        DataSource.ofCassandra(
            CassandraDataSource.builder()
                .setClusterName(sharedEmbeddedCassandra.getInstance().getClusterName())
                .setContactPoints(sharedEmbeddedCassandra.getInstance().getContactPoints())
                .setLocalDataCenter(sharedEmbeddedCassandra.getInstance().getLocalDataCenter())
                .build());

    CassandraSchemaDiscovery cassandraSchemaDiscovery = new CassandraSchemaDiscovery();
    ImmutableList<String> tables =
        cassandraSchemaDiscovery.discoverTables(cassandraDataSource, cassandraSchemaReference);
    assertThat(tables).isEqualTo(TEST_TABLES);
  }

  @Test
  public void testDiscoverTableSchemaBasic() throws IOException, RetriableSchemaDiscoveryException {

    SourceSchemaReference cassandraSchemaReference =
        SourceSchemaReference.ofCassandra(
            CassandraSchemaReference.builder().setKeyspaceName(TEST_KEYSPACE).build());

    DataSource cassandraDataSource =
        DataSource.ofCassandra(
            CassandraDataSource.builder()
                .setClusterName(sharedEmbeddedCassandra.getInstance().getClusterName())
                .setContactPoints(sharedEmbeddedCassandra.getInstance().getContactPoints())
                .setLocalDataCenter(sharedEmbeddedCassandra.getInstance().getLocalDataCenter())
                .build());
    CassandraSchemaDiscovery cassandraSchemaDiscovery = new CassandraSchemaDiscovery();
    ImmutableMap<String, ImmutableMap<String, SourceColumnType>> schema =
        cassandraSchemaDiscovery.discoverTableSchema(
            cassandraDataSource, cassandraSchemaReference, TEST_TABLES);
    assertThat(schema).isEqualTo(TEST_TABLE_SCHEMA);
  }

  @Test
  public void testCassandraSchemaDiscoveryDriverException() {

    SourceSchemaReference cassandraSchemaReference =
        SourceSchemaReference.ofCassandra(
            CassandraSchemaReference.builder().setKeyspaceName(TEST_KEYSPACE).build());
    SourceSchemaReference jdbcSourceSchemaReference =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("jdbc").build());

    /** Using reserved port throws connectionTimeout Exception from Cassandra Layer. */
    DataSource cassandraDataSource =
        DataSource.ofCassandra(
            CassandraDataSource.builder()
                .setClusterName(sharedEmbeddedCassandra.getInstance().getClusterName())
                .setContactPoints(
                    sharedEmbeddedCassandra.getInstance().getContactPoints().stream()
                        .map(
                            addr -> new InetSocketAddress(addr.getAddress(), /* Reserved Port */ 0))
                        .collect(ImmutableList.toImmutableList()))
                .setLocalDataCenter(sharedEmbeddedCassandra.getInstance().getLocalDataCenter())
                .build());

    CassandraSchemaDiscovery cassandraSchemaDiscovery = new CassandraSchemaDiscovery();
    assertThrows(
        SchemaDiscoveryException.class,
        () ->
            cassandraSchemaDiscovery.discoverTables(cassandraDataSource, cassandraSchemaReference));
    assertThrows(
        SchemaDiscoveryException.class,
        () ->
            cassandraSchemaDiscovery.discoverTableSchema(
                cassandraDataSource, cassandraSchemaReference, TEST_TABLES));
  }

  @Test
  public void testCassandraSchemaDiscoveryArgumentExceptions() {

    SourceSchemaReference cassandraSchemaReference =
        SourceSchemaReference.ofCassandra(
            CassandraSchemaReference.builder().setKeyspaceName(TEST_KEYSPACE).build());
    SourceSchemaReference jdbcSourceSchemaReference =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("jdbc").build());

    DataSource cassandraDataSource =
        DataSource.ofCassandra(
            CassandraDataSource.builder()
                .setClusterName(sharedEmbeddedCassandra.getInstance().getClusterName())
                .setContactPoints(sharedEmbeddedCassandra.getInstance().getContactPoints())
                .setLocalDataCenter(sharedEmbeddedCassandra.getInstance().getLocalDataCenter())
                .build());
    JdbcDataSource mockJdbcDataSource = Mockito.mock(JdbcDataSource.class);
    DataSource jdbcDataSource = DataSource.ofJdbc(mockJdbcDataSource);

    CassandraSchemaDiscovery cassandraSchemaDiscovery = new CassandraSchemaDiscovery();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            cassandraSchemaDiscovery.discoverTables(
                cassandraDataSource, jdbcSourceSchemaReference));
    assertThrows(
        IllegalArgumentException.class,
        () -> cassandraSchemaDiscovery.discoverTables(jdbcDataSource, cassandraSchemaReference));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            cassandraSchemaDiscovery.discoverTableSchema(
                cassandraDataSource, jdbcSourceSchemaReference, ImmutableList.of()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            cassandraSchemaDiscovery.discoverTableSchema(
                jdbcDataSource, cassandraSchemaReference, ImmutableList.of()));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            cassandraSchemaDiscovery.discoverTableIndexes(
                cassandraDataSource, jdbcSourceSchemaReference, ImmutableList.of()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            cassandraSchemaDiscovery.discoverTableIndexes(
                jdbcDataSource, cassandraSchemaReference, ImmutableList.of()));
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            cassandraSchemaDiscovery.discoverTableIndexes(
                cassandraDataSource, cassandraSchemaReference, TEST_TABLES));
  }
}
