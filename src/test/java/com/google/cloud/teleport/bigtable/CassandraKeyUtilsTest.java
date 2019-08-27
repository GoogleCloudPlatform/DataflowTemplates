/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.google.cloud.teleport.bigtable;

import static org.junit.Assert.assertEquals;
import static org.scassandra.cql.PrimitiveType.INT;
import static org.scassandra.cql.PrimitiveType.TEXT;
import static org.scassandra.cql.PrimitiveType.VARCHAR;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.scassandra.http.client.types.ColumnMetadata.column;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.scassandra.http.client.PrimingRequest;

/** Tests {@link CassandraKeyUtils}. */
public class CassandraKeyUtilsTest extends CassandraBaseTest {

  @Test
  public void testSimplePrimaryKeyOrder() {

    String keyspace = "mykeyspace";
    String table = "mytable";

    String query = CassandraKeyUtils.primarykeyCQL(keyspace, table).toString();

    Map<String, Object> row =
        ImmutableMap.of(
            "column_name", "col1",
            "kind", "partition_key",
            "position", 0);

    PrimingRequest primingRequest =
        PrimingRequest.queryBuilder()
            .withQuery(query)
            .withThen(
                then()
                    .withColumnTypes(
                        column("column_name", TEXT), column("kind", TEXT), column("position", INT))
                    .withRows(row))
            .build();
    primingClient.prime(primingRequest);

    Cluster cluster =
        Cluster.builder().addContactPoint("localhost").withPort(scassandra.getBinaryPort()).build();
    Session session = cluster.connect();

    Map<String, Integer> expected = new HashMap();
    expected.put("col1", 0);

    assertEquals(expected, CassandraKeyUtils.primaryKeyOrder(session, keyspace, table));
  }

  @Test
  public void testComplexPrimaryKeyOrder() {

    String keyspace = "mykeyspace";
    String table = "mytable";

    String query = CassandraKeyUtils.primarykeyCQL(keyspace, table).toString();

    Map<String, Object> row0 =
        ImmutableMap.of(
            "column_name", "col0",
            "kind", "partition_key",
            "position", 0);

    Map<String, Object> row1 =
        ImmutableMap.of(
            "column_name", "col1",
            "kind", "partition_key",
            "position", 1);

    Map<String, Object> row2 =
        ImmutableMap.of(
            "column_name", "col2",
            "kind", "clustering",
            "position", 0);

    Map<String, Object> row3 =
        ImmutableMap.of(
            "column_name", "col3",
            "kind", "clustering",
            "position", 1);

    PrimingRequest primingRequest =
        PrimingRequest.queryBuilder()
            .withQuery(query)
            .withThen(
                then()
                    .withColumnTypes(
                        column("column_name", TEXT), column("kind", TEXT), column("position", INT))
                    .withRows(row0, row1, row2, row3))
            .build();
    primingClient.prime(primingRequest);

    Cluster cluster =
        Cluster.builder().addContactPoint("localhost").withPort(scassandra.getBinaryPort()).build();
    Session session = cluster.connect();

    Map<String, Integer> expected = new HashMap();
    expected.put("col0", 0);
    expected.put("col1", 1);
    expected.put("col2", 2);
    expected.put("col3", 3);

    assertEquals(expected, CassandraKeyUtils.primaryKeyOrder(session, keyspace, table));
  }

  @Test
  public void testGenerateSchema() {

    String keyspace = "mykeyspace";
    String table = "mytable";

    String query = CassandraKeyUtils.primarykeyCQL(keyspace, table).toString();

    Map<String, Object> row0 =
        ImmutableMap.of(
            "column_name", "col0",
            "kind", "partition_key",
            "position", 0);

    PrimingRequest primingRequest =
        PrimingRequest.queryBuilder()
            .withQuery(query)
            .withThen(
                then()
                    .withColumnTypes(
                        column("column_name", TEXT), column("kind", TEXT), column("position", INT))
                    .withRows(row0))
            .build();
    primingClient.prime(primingRequest);

    Map<String, ?> row = ImmutableMap.of("col0", "primary key", "col1", "normal column");
    primingRequest =
        PrimingRequest.queryBuilder()
            .withQuery("select * from testing")
            .withThen(
                then()
                    .withColumnTypes(column("col0", VARCHAR), column("col1", VARCHAR))
                    .withRows(row))
            .build();
    primingClient.prime(primingRequest);

    Cluster cluster =
        Cluster.builder().addContactPoint("localhost").withPort(scassandra.getBinaryPort()).build();
    Session session = cluster.connect();

    CassandraRowMapperFn mapper =
        new CassandraRowMapperFn(
            session,
            ValueProvider.StaticValueProvider.of(keyspace),
            ValueProvider.StaticValueProvider.of(table));
    ResultSet rs = session.execute("select * from testing");
    Iterator<Row> rows = mapper.map(rs);
    Row next = rows.next();

    Field field = next.getSchema().getField("col0"); 
    assertEquals("0", field.getType().getMetadataString(CassandraRowMapperFn.KEY_ORDER_METADATA_KEY));
  }
}
