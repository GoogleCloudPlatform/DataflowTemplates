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
import static org.scassandra.cql.ListType.list;
import static org.scassandra.cql.PrimitiveType.ASCII;
import static org.scassandra.cql.PrimitiveType.BIG_INT;
import static org.scassandra.cql.PrimitiveType.BLOB;
import static org.scassandra.cql.PrimitiveType.BOOLEAN;
import static org.scassandra.cql.PrimitiveType.COUNTER;
import static org.scassandra.cql.PrimitiveType.DATE;
import static org.scassandra.cql.PrimitiveType.DECIMAL;
import static org.scassandra.cql.PrimitiveType.DOUBLE;
import static org.scassandra.cql.PrimitiveType.FLOAT;
import static org.scassandra.cql.PrimitiveType.INET;
import static org.scassandra.cql.PrimitiveType.INT;
import static org.scassandra.cql.PrimitiveType.SMALL_INT;
import static org.scassandra.cql.PrimitiveType.TEXT;
import static org.scassandra.cql.PrimitiveType.TIME;
import static org.scassandra.cql.PrimitiveType.TIMESTAMP;
import static org.scassandra.cql.PrimitiveType.TINY_INT;
import static org.scassandra.cql.PrimitiveType.UUID;
import static org.scassandra.cql.PrimitiveType.VARCHAR;
import static org.scassandra.cql.PrimitiveType.VAR_INT;
import static org.scassandra.cql.SetType.set;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.scassandra.http.client.types.ColumnMetadata.column;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.InetAddresses;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.codec.binary.Hex;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.scassandra.cql.CqlType;
import org.scassandra.cql.MapType;
import org.scassandra.http.client.PrimingRequest;

/** Tests {@link CassandraRowMapperFn}. */
public class CassandraRowMapperFnTest extends CassandraBaseTest {

  private CassandraRowMapperFn cassandraRowMapper;

  private void primeWithType(Object val, CqlType type) {
    Map<String, ?> row = ImmutableMap.of("col", val);
    PrimingRequest primingRequest =
        PrimingRequest.queryBuilder()
            .withQuery("select * from testing")
            .withThen(then().withColumnTypes(column("col", type)).withRows(row))
            .build();
    primingClient.prime(primingRequest);
  }

  private ResultSet getResultSet() {
    Cluster cluster =
        Cluster.builder().addContactPoint("localhost").withPort(scassandra.getBinaryPort()).build();
    Session session = cluster.connect();
    return session.execute("select * from testing");
  }

  @Before
  public void setupMapper() {
    Cluster cluster =
        Cluster.builder().addContactPoint("localhost").withPort(scassandra.getBinaryPort()).build();
    Session session = cluster.connect();
    cassandraRowMapper =
        new CassandraRowMapperFn(
            session,
            ValueProvider.StaticValueProvider.of(""),
            ValueProvider.StaticValueProvider.of("testing"));
  }

  @Test
  public void testMapVarCharColumn() {
    String value = "Hello world!";
    primeWithType(value, VARCHAR);
    ResultSet resultSet = getResultSet();

    Schema schema = Schema.builder().addNullableField("col", FieldType.STRING).build();
    Row expected = Row.withSchema(schema).addValue(value).build();

    assertEquals(expected, cassandraRowMapper.map(resultSet).next());
  }

  @Test
  public void testMapIntColumn() {
    Integer value = 12355;
    primeWithType(value, INT);
    ResultSet resultSet = getResultSet();

    Schema schema = Schema.builder().addNullableField("col", FieldType.INT32).build();
    Row expected = Row.withSchema(schema).addValue(value).build();

    assertEquals(expected, cassandraRowMapper.map(resultSet).next());
  }

  @Test
  public void testMapFloatColumn() {
    Float value = 123.4f;
    primeWithType(value, FLOAT);
    ResultSet resultSet = getResultSet();

    Schema schema = Schema.builder().addNullableField("col", FieldType.FLOAT).build();
    Row expected = Row.withSchema(schema).addValue(value).build();

    assertEquals(expected, cassandraRowMapper.map(resultSet).next());
  }

  @Test
  public void testMapDoubleColumn() {
    Double value = Double.MIN_VALUE;
    primeWithType(value, DOUBLE);
    ResultSet resultSet = getResultSet();

    Schema schema = Schema.builder().addNullableField("col", FieldType.DOUBLE).build();
    Row expected = Row.withSchema(schema).addValue(value).build();

    assertEquals(expected, cassandraRowMapper.map(resultSet).next());
  }

  @Test
  public void testMapBigIntColumn() {
    BigInteger value = BigInteger.TEN;
    primeWithType(value, BIG_INT);
    ResultSet resultSet = getResultSet();

    Schema schema = Schema.builder().addNullableField("col", FieldType.INT64).build();
    Row expected = Row.withSchema(schema).addValue(value.longValue()).build();

    assertEquals(expected, cassandraRowMapper.map(resultSet).next());
  }

  @Test
  public void testMapBlobColumn() {
    byte[] value = new byte[20];
    new Random().nextBytes(value);

    primeWithType(Hex.encodeHexString(value), BLOB);
    ResultSet resultSet = getResultSet();

    Schema schema = Schema.builder().addNullableField("col", FieldType.BYTES).build();
    Row expected = Row.withSchema(schema).addValue(value).build();

    assertEquals(expected, cassandraRowMapper.map(resultSet).next());
  }

  @Test
  public void testMapBooleanColumn() {
    Boolean value = Boolean.FALSE;
    primeWithType(value, BOOLEAN);
    ResultSet resultSet = getResultSet();

    Schema schema = Schema.builder().addNullableField("col", FieldType.BOOLEAN).build();
    Row expected = Row.withSchema(schema).addValue(value).build();

    assertEquals(expected, cassandraRowMapper.map(resultSet).next());
  }

  @Test
  public void testCounterColumn() {
    Long value = 100L;
    primeWithType(value, COUNTER);
    ResultSet resultSet = getResultSet();

    Schema schema = Schema.builder().addNullableField("col", FieldType.INT64).build();
    Row expected = Row.withSchema(schema).addValue(value).build();

    assertEquals(expected, cassandraRowMapper.map(resultSet).next());
  }

  @Test
  public void testIPv4Column() {
    String value = "10.10.10.10";
    primeWithType(value, INET);
    ResultSet resultSet = getResultSet();

    Schema schema = Schema.builder().addNullableField("col", FieldType.STRING).build();
    Row expected = Row.withSchema(schema).addValue(value).build();

    assertEquals(expected, cassandraRowMapper.map(resultSet).next());
  }

  @Test
  public void testIPv6Column() {
    String value = InetAddresses.fromInteger(new Random().nextInt()).getHostAddress();
    primeWithType(value, INET);
    ResultSet resultSet = getResultSet();

    Schema schema = Schema.builder().addNullableField("col", FieldType.STRING).build();
    Row expected = Row.withSchema(schema).addValue(value).build();

    assertEquals(expected, cassandraRowMapper.map(resultSet).next());
  }

  @Test
  public void testSmallIntColumn() {
    Short value = Short.MAX_VALUE;
    primeWithType(value, SMALL_INT);
    ResultSet resultSet = getResultSet();

    Schema schema = Schema.builder().addNullableField("col", FieldType.INT16).build();
    Row expected = Row.withSchema(schema).addValue(value).build();

    assertEquals(expected, cassandraRowMapper.map(resultSet).next());
  }

  @Test
  public void testTextColumn() {
    String value = "Hello world!";
    primeWithType(value, TEXT);
    ResultSet resultSet = getResultSet();

    Schema schema = Schema.builder().addNullableField("col", FieldType.STRING).build();
    Row expected = Row.withSchema(schema).addValue(value).build();

    Row next = cassandraRowMapper.map(resultSet).next();

    assertEquals(expected, next);
  }

  @Test
  public void testType4UUIDColumn() {
    UUID value = java.util.UUID.randomUUID();
    primeWithType(value, UUID);
    ResultSet resultSet = getResultSet();

    Schema schema = Schema.builder().addNullableField("col", FieldType.STRING).build();
    Row expected = Row.withSchema(schema).addValue(value.toString()).build();

    assertEquals(expected, cassandraRowMapper.map(resultSet).next());
  }

  @Test
  public void testType1UUIDColumn() {
    String value = com.datastax.driver.core.utils.UUIDs.timeBased().toString();
    primeWithType(value, UUID);
    ResultSet resultSet = getResultSet();

    Schema schema = Schema.builder().addNullableField("col", FieldType.STRING).build();
    Row expected = Row.withSchema(schema).addValue(value).build();

    assertEquals(expected, cassandraRowMapper.map(resultSet).next());
  }

  @Test
  public void testDateColumn() {
    LocalDate now = LocalDate.now();
    LocalDate epoch = LocalDate.ofEpochDay(0);
    long value = (long) Math.pow(2, 31) + ChronoUnit.DAYS.between(epoch, now);
    DateTime expectedDate =
        new DateTime(now.getYear(), now.getMonthValue(), now.getDayOfMonth(), 0, 0);

    primeWithType(value, DATE);
    ResultSet resultSet = getResultSet();

    Schema schema = Schema.builder().addNullableField("col", FieldType.DATETIME).build();
    Row expected = Row.withSchema(schema).addValue(expectedDate).build();

    assertEquals(expected, cassandraRowMapper.map(resultSet).next());
  }

  @Test
  public void testTimeColumn() {
    LocalTime now = LocalTime.now(ZoneId.systemDefault());

    Long value = now.toNanoOfDay();
    primeWithType(value, TIME);
    ResultSet resultSet = getResultSet();

    Schema schema = Schema.builder().addNullableField("col", FieldType.INT64).build();
    Row expected = Row.withSchema(schema).addValue(value).build();

    assertEquals(expected, cassandraRowMapper.map(resultSet).next());
  }

  @Test
  public void testTinyIntColumn() {
    byte value = 0;
    primeWithType(value, TINY_INT);
    ResultSet resultSet = getResultSet();

    Schema schema = Schema.builder().addNullableField("col", FieldType.BYTE).build();
    Row expected = Row.withSchema(schema).addValue(value).build();

    assertEquals(expected, cassandraRowMapper.map(resultSet).next());
  }

  @Test
  public void testVarIntColumn() {
    BigInteger value = new BigInteger(32, new Random());
    primeWithType(value, VAR_INT);
    ResultSet resultSet = getResultSet();

    Schema schema = Schema.builder().addNullableField("col", FieldType.DECIMAL).build();
    Row expected = Row.withSchema(schema).addValue(new BigDecimal(value)).build();

    assertEquals(expected, cassandraRowMapper.map(resultSet).next());
  }

  @Test
  public void testDecimalColumn() {
    BigDecimal value = new BigDecimal(0.6);
    primeWithType(value, DECIMAL);
    ResultSet resultSet = getResultSet();

    Schema schema = Schema.builder().addNullableField("col", FieldType.DECIMAL).build();
    Row expected = Row.withSchema(schema).addValue(value).build();

    assertEquals(expected, cassandraRowMapper.map(resultSet).next());
  }

  @Test
  public void testTimestampColumn() {
    Long value = System.currentTimeMillis();
    primeWithType(value, TIMESTAMP);
    ResultSet resultSet = getResultSet();

    Schema schema = Schema.builder().addNullableField("col", FieldType.DATETIME).build();
    Row expected = Row.withSchema(schema).addValue(new DateTime(value)).build();

    assertEquals(expected, cassandraRowMapper.map(resultSet).next());
  }

  @Test
  public void testASCIIColumn() {
    String s = "Hello world";
    // I cant find a viable way of storing ASCII strings with scassandra. This will have to be
    // tested more in the integration tests.
    primeWithType(s, ASCII);
    ResultSet resultSet = getResultSet();

    Schema schema = Schema.builder().addNullableField("col", FieldType.STRING).build();
    Row expected = Row.withSchema(schema).addValue(s).build();

    assertEquals(expected, cassandraRowMapper.map(resultSet).next());
  }

  @Test
  public void testListColumn() {
    ArrayList<String> value = new ArrayList<>();
    value.add("Hello");
    value.add("world!");

    primeWithType(value, list(VARCHAR));
    ResultSet resultSet = getResultSet();

    Schema schema =
        Schema.builder().addNullableField("col", FieldType.array(FieldType.STRING)).build();
    Row expected = Row.withSchema(schema).addValue(value).build();

    assertEquals(expected, cassandraRowMapper.map(resultSet).next());
  }

  @Test
  public void testSetColumn() {
    Set<Integer> value = new HashSet<>();
    value.add(1);
    value.add(2);

    primeWithType(value, set(INT));
    ResultSet resultSet = getResultSet();

    Schema schema =
        Schema.builder().addNullableField("col", FieldType.array(FieldType.INT32)).build();
    Row expected = Row.withSchema(schema).addValue(new ArrayList<>(value)).build();

    assertEquals(expected, cassandraRowMapper.map(resultSet).next());
  }

  @Test
  @Ignore
  public void testTupleColumn() {
    throw new UnsupportedOperationException();
  }

  @Test
  public void testMapColumn() {
    Map<String, UUID> cassandraInsert = new HashMap<>();
    UUID uuid1 = java.util.UUID.randomUUID();
    UUID uuid2 = java.util.UUID.randomUUID();
    cassandraInsert.put("1", uuid1);
    cassandraInsert.put("2", uuid2);

    Map<String, String> beamValue = new HashMap<>();
    beamValue.put("1", uuid1.toString());
    beamValue.put("2", uuid2.toString());

    primeWithType(cassandraInsert, MapType.map(VARCHAR, UUID));
    ResultSet resultSet = getResultSet();

    Schema schema =
        Schema.builder()
            .addNullableField("col", FieldType.map(FieldType.STRING, FieldType.STRING))
            .build();
    Row expected = Row.withSchema(schema).addValue(beamValue).build();

    assertEquals(expected, cassandraRowMapper.map(resultSet).next());
  }
}
