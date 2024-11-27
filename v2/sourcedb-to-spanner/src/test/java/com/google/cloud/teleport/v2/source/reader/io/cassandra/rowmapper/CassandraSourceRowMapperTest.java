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

import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.PRIMITIVE_TYPES_TABLE;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.PRIMITIVE_TYPES_TABLE_AVRO_ROWS;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_CONFIG;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_CQLSH;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_KEYSPACE;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper.CassandraConnector;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper.CassandraDataSource;
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
  public void testCassandraSourceRowMapperBasic() throws RetriableSchemaDiscoveryException {

    SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.ofCassandra(
            CassandraSchemaReference.builder().setKeyspaceName(TEST_KEYSPACE).build());

    DataSource dataSource =
        DataSource.ofCassandra(
            CassandraDataSource.builder()
                .setClusterName(sharedEmbeddedCassandra.getInstance().getClusterName())
                .setContactPoints(sharedEmbeddedCassandra.getInstance().getContactPoints())
                .setLocalDataCenter(sharedEmbeddedCassandra.getInstance().getLocalDataCenter())
                .build());

    SourceTableSchema.Builder sourceTableSchemaBuilder =
        SourceTableSchema.builder(MapperType.CASSANDRA).setTableName(PRIMITIVE_TYPES_TABLE);
    new CassandraSchemaDiscovery()
        .discoverTableSchema(
            dataSource, sourceSchemaReference, ImmutableList.of(PRIMITIVE_TYPES_TABLE))
        .get(PRIMITIVE_TYPES_TABLE)
        .forEach(sourceTableSchemaBuilder::addSourceColumnNameToSourceColumnType);

    CassandraSourceRowMapper cassandraSourceRowMapper =
        CassandraSourceRowMapper.builder()
            .setSourceSchemaReference(sourceSchemaReference)
            .setSourceTableSchema(sourceTableSchemaBuilder.build())
            .build();

    ResultSet resultSet;
    String query = "SELECT * FROM " + PRIMITIVE_TYPES_TABLE;
    com.datastax.oss.driver.api.core.cql.SimpleStatement statement =
        SimpleStatement.newInstance(query);
    Cluster cluster =
        Cluster.builder()
            .addContactPointsWithPorts(dataSource.cassandra().contactPoints())
            .withClusterName(dataSource.cassandra().clusterName())
            .withoutJMXReporting()
            .withLoadBalancingPolicy(
                new DCAwareRoundRobinPolicy.Builder()
                    .withLocalDc(dataSource.cassandra().localDataCenter())
                    .build())
            .build();
    try (CassandraConnector cassandraConnectorWithSchemaReference =
        new CassandraConnector(dataSource.cassandra(), sourceSchemaReference.cassandra())) {
      resultSet = cluster.connect(TEST_KEYSPACE).execute(query);
      ImmutableList.Builder<SourceRow> readRowsBuilder = ImmutableList.builder();
      cassandraSourceRowMapper.map(resultSet).forEachRemaining(row -> readRowsBuilder.add(row));
      ImmutableList<SourceRow> readRows = readRowsBuilder.build();

      readRows.forEach(r -> assertThat(r.tableName() == PRIMITIVE_TYPES_TABLE));
      readRows.forEach(r -> assertThat(r.sourceSchemaReference() == sourceSchemaReference));
      assertThat(
              readRows.stream()
                  .map(r -> r.getPayload().toString())
                  .sorted()
                  .collect(ImmutableList.toImmutableList()))
          .isEqualTo(
              PRIMITIVE_TYPES_TABLE_AVRO_ROWS.stream()
                  .sorted()
                  .collect(ImmutableList.toImmutableList()));

      // Since we will use CassandraIO only for reads, we don't need to support the `deleteAsync`
      // and `saveAsync` functions of the CassandraIO mapper interface.
      assertThrows(
          UnsupportedOperationException.class,
          () -> cassandraSourceRowMapper.deleteAsync(readRows.get(1)));
      assertThrows(
          UnsupportedOperationException.class,
          () -> cassandraSourceRowMapper.saveAsync(readRows.get(1)));
    }
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
}
