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
package com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils;

import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.codec.binary.Hex;

/**
 * Test utility class to provide details need to start `basic_test.cql` schema on an embedded
 * cassandra cluster.
 */
public class BasicTestSchema {

  private static final String TEST_RESOURCE_ROOT = "/CassandraUT/";
  public static final String TEST_KEYSPACE = "test_keyspace";
  public static final String TEST_CONFIG = TEST_RESOURCE_ROOT + "basicConfig.yaml";
  public static final String TEST_CQLSH = TEST_RESOURCE_ROOT + "basicTest.cql";
  public static final String BASIC_TEST_TABLE = "basic_test_table";
  public static final String PRIMITIVE_TYPES_TABLE = "primitive_types_table";
  public static final String LIST_TYPES_TABLE = "list_types_table";
  public static final String SET_TYPES_TABLE = "set_types_table";
  public static final String MAP_TYPES_TABLE = "map_types_table";
  public static final Long PRIMITIVE_TYPES_TABLE_ROW_COUNT = 6L;
  public static final ImmutableMap<String, ImmutableMap<String, SourceColumnType>>
      BASIC_TEST_TABLE_SCHEMA =
          ImmutableMap.of(
              BASIC_TEST_TABLE,
              ImmutableMap.of(
                  "id", new SourceColumnType("TEXT", new Long[] {}, new Long[] {}),
                  "name", new SourceColumnType("TEXT", new Long[] {}, new Long[] {})));
  public static final ImmutableMap<String, ImmutableMap<String, SourceColumnType>>
      LIST_TEST_TABLE_SCHEMA =
          ImmutableMap.of(
              LIST_TYPES_TABLE,
              ImmutableMap.<String, SourceColumnType>builder()
                  .put("primary_key", new SourceColumnType("UUID", new Long[] {}, new Long[] {}))
                  .put(
                      "ascii_list",
                      new SourceColumnType("LIST<ASCII>", new Long[] {}, new Long[] {}))
                  .put(
                      "frozen_ascii_list",
                      new SourceColumnType("LIST<ASCII>", new Long[] {}, new Long[] {}))
                  .put(
                      "bigint_list",
                      new SourceColumnType("LIST<BIGINT>", new Long[] {}, new Long[] {}))
                  .put(
                      "blob_list", new SourceColumnType("LIST<BLOB>", new Long[] {}, new Long[] {}))
                  .put(
                      "boolean_list",
                      new SourceColumnType("LIST<BOOLEAN>", new Long[] {}, new Long[] {}))
                  .put(
                      "date_list", new SourceColumnType("LIST<DATE>", new Long[] {}, new Long[] {}))
                  .put(
                      "decimal_list",
                      new SourceColumnType("LIST<DECIMAL>", new Long[] {}, new Long[] {}))
                  .put(
                      "double_list",
                      new SourceColumnType("LIST<DOUBLE>", new Long[] {}, new Long[] {}))
                  .put(
                      "duration_list",
                      new SourceColumnType("LIST<DURATION>", new Long[] {}, new Long[] {}))
                  .put(
                      "float_list",
                      new SourceColumnType("LIST<FLOAT>", new Long[] {}, new Long[] {}))
                  .put(
                      "inet_list", new SourceColumnType("LIST<INET>", new Long[] {}, new Long[] {}))
                  .put("int_list", new SourceColumnType("LIST<INT>", new Long[] {}, new Long[] {}))
                  .put(
                      "smallint_list",
                      new SourceColumnType("LIST<SMALLINT>", new Long[] {}, new Long[] {}))
                  .put(
                      "text_list", new SourceColumnType("LIST<TEXT>", new Long[] {}, new Long[] {}))
                  .put(
                      "time_list", new SourceColumnType("LIST<TIME>", new Long[] {}, new Long[] {}))
                  .put(
                      "timestamp_list",
                      new SourceColumnType("LIST<TIMESTAMP>", new Long[] {}, new Long[] {}))
                  .put(
                      "timeuuid_list",
                      new SourceColumnType("LIST<TIMEUUID>", new Long[] {}, new Long[] {}))
                  .put(
                      "tinyint_list",
                      new SourceColumnType("LIST<TINYINT>", new Long[] {}, new Long[] {}))
                  .put(
                      "uuid_list", new SourceColumnType("LIST<UUID>", new Long[] {}, new Long[] {}))
                  .put(
                      "varchar_list",
                      new SourceColumnType("LIST<TEXT>", new Long[] {}, new Long[] {}))
                  .put(
                      "varint_list",
                      new SourceColumnType("LIST<VARINT>", new Long[] {}, new Long[] {}))
                  .build());
  public static final ImmutableMap<String, ImmutableMap<String, SourceColumnType>>
      SET_TEST_TABLE_SCHEMA =
          ImmutableMap.of(
              SET_TYPES_TABLE,
              ImmutableMap.<String, SourceColumnType>builder()
                  .put("primary_key", new SourceColumnType("UUID", new Long[] {}, new Long[] {}))
                  .put(
                      "ascii_set", new SourceColumnType("SET<ASCII>", new Long[] {}, new Long[] {}))
                  .put(
                      "frozen_ascii_set",
                      new SourceColumnType("SET<ASCII>", new Long[] {}, new Long[] {}))
                  .put(
                      "bigint_set",
                      new SourceColumnType("SET<BIGINT>", new Long[] {}, new Long[] {}))
                  .put("blob_set", new SourceColumnType("SET<BLOB>", new Long[] {}, new Long[] {}))
                  .put(
                      "boolean_set",
                      new SourceColumnType("SET<BOOLEAN>", new Long[] {}, new Long[] {}))
                  .put("date_set", new SourceColumnType("SET<DATE>", new Long[] {}, new Long[] {}))
                  .put(
                      "decimal_set",
                      new SourceColumnType("SET<DECIMAL>", new Long[] {}, new Long[] {}))
                  .put(
                      "double_set",
                      new SourceColumnType("SET<DOUBLE>", new Long[] {}, new Long[] {}))
                  .put(
                      "float_set", new SourceColumnType("SET<FLOAT>", new Long[] {}, new Long[] {}))
                  .put("inet_set", new SourceColumnType("SET<INET>", new Long[] {}, new Long[] {}))
                  .put("int_set", new SourceColumnType("SET<INT>", new Long[] {}, new Long[] {}))
                  .put(
                      "smallint_set",
                      new SourceColumnType("SET<SMALLINT>", new Long[] {}, new Long[] {}))
                  .put("text_set", new SourceColumnType("SET<TEXT>", new Long[] {}, new Long[] {}))
                  .put("time_set", new SourceColumnType("SET<TIME>", new Long[] {}, new Long[] {}))
                  .put(
                      "timestamp_set",
                      new SourceColumnType("SET<TIMESTAMP>", new Long[] {}, new Long[] {}))
                  .put(
                      "timeuuid_set",
                      new SourceColumnType("SET<TIMEUUID>", new Long[] {}, new Long[] {}))
                  .put(
                      "tinyint_set",
                      new SourceColumnType("SET<TINYINT>", new Long[] {}, new Long[] {}))
                  .put("uuid_set", new SourceColumnType("SET<UUID>", new Long[] {}, new Long[] {}))
                  .put(
                      "varchar_set",
                      new SourceColumnType("SET<TEXT>", new Long[] {}, new Long[] {}))
                  .put(
                      "varint_set",
                      new SourceColumnType("SET<VARINT>", new Long[] {}, new Long[] {}))
                  .build());

  public static final ImmutableMap<String, ImmutableMap<String, SourceColumnType>>
      MAP_TEST_TABLE_SCHEMA =
          ImmutableMap.of(
              MAP_TYPES_TABLE,
              ImmutableMap.<String, SourceColumnType>builder()
                  .put("primary_key", new SourceColumnType("UUID", new Long[] {}, new Long[] {}))
                  .put(
                      "ascii_text_map",
                      new SourceColumnType("MAP<ASCII,TEXT>", new Long[] {}, new Long[] {}))
                  .put(
                      "bigint_boolean_map",
                      new SourceColumnType("MAP<BIGINT,BOOLEAN>", new Long[] {}, new Long[] {}))
                  .put(
                      "blob_int_map",
                      new SourceColumnType("MAP<BLOB,INT>", new Long[] {}, new Long[] {}))
                  .put(
                      "boolean_decimal_map",
                      new SourceColumnType("MAP<BOOLEAN,DECIMAL>", new Long[] {}, new Long[] {}))
                  .put(
                      "date_double_map",
                      new SourceColumnType("MAP<DATE,DOUBLE>", new Long[] {}, new Long[] {}))
                  .put(
                      "decimal_duration_map",
                      new SourceColumnType("MAP<DECIMAL,DURATION>", new Long[] {}, new Long[] {}))
                  .put(
                      "double_float_map",
                      new SourceColumnType("MAP<DOUBLE,FLOAT>", new Long[] {}, new Long[] {}))
                  .put(
                      "double_inet_map",
                      new SourceColumnType("MAP<DOUBLE,INET>", new Long[] {}, new Long[] {}))
                  .put(
                      "float_smallint_map",
                      new SourceColumnType("MAP<FLOAT,SMALLINT>", new Long[] {}, new Long[] {}))
                  .put(
                      "inet_text_map",
                      new SourceColumnType("MAP<INET,TEXT>", new Long[] {}, new Long[] {}))
                  .put(
                      "int_time_map",
                      new SourceColumnType("MAP<INT,TIME>", new Long[] {}, new Long[] {}))
                  .put(
                      "smallint_timestamp_map",
                      new SourceColumnType("MAP<SMALLINT,TIMESTAMP>", new Long[] {}, new Long[] {}))
                  .put(
                      "text_timeuuid_map",
                      new SourceColumnType("MAP<TEXT,TIMEUUID>", new Long[] {}, new Long[] {}))
                  .put(
                      "time_tinyint_map",
                      new SourceColumnType("MAP<TIME,TINYINT>", new Long[] {}, new Long[] {}))
                  .put(
                      "timestamp_uuid_map",
                      new SourceColumnType("MAP<TIMESTAMP,UUID>", new Long[] {}, new Long[] {}))
                  .put(
                      "timeuuid_varchar_map",
                      new SourceColumnType("MAP<TIMEUUID,TEXT>", new Long[] {}, new Long[] {}))
                  .put(
                      "tinyint_varint_map",
                      new SourceColumnType("MAP<TINYINT,VARINT>", new Long[] {}, new Long[] {}))
                  .put(
                      "uuid_ascii_map",
                      new SourceColumnType("MAP<UUID,ASCII>", new Long[] {}, new Long[] {}))
                  .put(
                      "varchar_bigint_map",
                      new SourceColumnType("MAP<TEXT,BIGINT>", new Long[] {}, new Long[] {}))
                  .put(
                      "varint_blob_map",
                      new SourceColumnType("MAP<VARINT,BLOB>", new Long[] {}, new Long[] {}))
                  .build());

  public static final ImmutableList<String> TEST_TABLES =
      ImmutableList.of(
          BASIC_TEST_TABLE,
          LIST_TYPES_TABLE,
          MAP_TYPES_TABLE,
          PRIMITIVE_TYPES_TABLE,
          SET_TYPES_TABLE);

  public static final ImmutableList<String> PRIMITIVE_TYPES_TABLE_AVRO_ROWS =
      ImmutableList.of(
          "{\"primary_key\": \"dfcad8f3-3cdc-49c7-bce9-575f307c0637\", \"ascii_col\": \"ascii1\", \"bigint_col\": 1234567890, \"blob_col\": \"cafebabe\", \"boolean_col\": true, \"date_col\": 19694, \"decimal_col\": \"123.456\", \"double_col\": 123.456789, \"duration_col\": {\"years\": 0, \"months\": 0, \"days\": 0, \"hours\": 0, \"minutes\": 0, \"seconds\": 0, \"nanos\": 45296000000000}, \"float_col\": 123.45, \"inet_col\": \"/127.0.0.1\", \"int_col\": 12345, \"smallint_col\": 123, \"text_col\": \"text1\", \"time_col\": 45296789000000, \"timestamp_col\": 1733296987117000000, \"timeuuid_col\": \"9b9419da-b210-11ef-890e-9d9a41af9e54\", \"tinyint_col\": 123, \"uuid_col\": \"c3de3455-6b4e-4a81-a6d7-ab61610f08c6\", \"varchar_col\": \"varchar1\", \"varint_col\": \"1234567890123456789\"}",
          "{\"primary_key\": \"fe3263a0-1577-4851-95f8-3af47628baa4\", \"ascii_col\": \"ascii2\", \"bigint_col\": 9876543210, \"blob_col\": \"deadbeef\", \"boolean_col\": false, \"date_col\": 19298, \"decimal_col\": \"987.654\", \"double_col\": 987.654321, \"duration_col\": {\"years\": 0, \"months\": 0, \"days\": 0, \"hours\": 0, \"minutes\": 0, \"seconds\": 0, \"nanos\": -45296000000000}, \"float_col\": 987.65, \"inet_col\": \"/0:0:0:0:0:0:0:1\", \"int_col\": 98765, \"smallint_col\": 987, \"text_col\": \"text2\", \"time_col\": 86399999000000, \"timestamp_col\": 1733296987122000000, \"timeuuid_col\": \"9b94dd2a-b210-11ef-890e-9d9a41af9e54\", \"tinyint_col\": -123, \"uuid_col\": \"6324e301-94fb-44fe-95ac-91d2f7236e2e\", \"varchar_col\": \"varchar2\", \"varint_col\": \"-9876543210987654321\"}",
          "{\"primary_key\": \"9a0acb7d-674c-4ee1-9644-9da24b7a72f4\", \"ascii_col\": \"ascii3\", \"bigint_col\": 1010101010, \"blob_col\": \"facefeed\", \"boolean_col\": true, \"date_col\": 19723, \"decimal_col\": \"10.101\", \"double_col\": 10.10101, \"duration_col\": {\"years\": 0, \"months\": 14, \"days\": 3, \"hours\": 0, \"minutes\": 0, \"seconds\": 0, \"nanos\": 14706000000000}, \"float_col\": 10.1, \"inet_col\": \"/192.168.1.1\", \"int_col\": 10101, \"smallint_col\": 101, \"text_col\": \"text3\", \"time_col\": 0, \"timestamp_col\": 1733296987127000000, \"timeuuid_col\": \"9b95a07a-b210-11ef-890e-9d9a41af9e54\", \"tinyint_col\": 101, \"uuid_col\": \"f0e1d922-06b5-4f07-a7a6-ec0c9f23e172\", \"varchar_col\": \"varchar3\", \"varint_col\": \"10101010101010101010\"}",
          "{\"primary_key\": \"e6bc8562-2575-420f-9344-9fedc4945f61\", \"ascii_col\": null, \"bigint_col\": 0, \"blob_col\": null, \"boolean_col\": false, \"date_col\": null, \"decimal_col\": null, \"double_col\": 0.0, \"duration_col\": null, \"float_col\": 0.0, \"inet_col\": null, \"int_col\": 0, \"smallint_col\": 0, \"text_col\": null, \"time_col\": 0, \"timestamp_col\": null, \"timeuuid_col\": null, \"tinyint_col\": 0, \"uuid_col\": null, \"varchar_col\": null, \"varint_col\": null}",
          "{\"primary_key\": \"a389de30-f01f-4395-a0c6-c407bfbe81d0\", \"ascii_col\": \"zzzzzzzzzz\", \"bigint_col\": 9223372036854775807, \"blob_col\": \"ffffffff\", \"boolean_col\": true, \"date_col\": 2932896, \"decimal_col\": \"10000000000000000000000000000000000000\", \"double_col\": 1.7976931348623157E308, \"duration_col\": {\"years\": 0, \"months\": 0, \"days\": 0, \"hours\": 0, \"minutes\": 0, \"seconds\": 0, \"nanos\": 320949000000000}, \"float_col\": 3.4028235E38, \"inet_col\": \"/255.255.255.255\", \"int_col\": 2147483647, \"smallint_col\": 32767, \"text_col\": \"abcdef\", \"time_col\": 86399999000000, \"timestamp_col\": -1000, \"timeuuid_col\": null, \"tinyint_col\": 127, \"uuid_col\": \"00e4afef-52f8-4e1f-9afa-0632c8ccf790\", \"varchar_col\": \"abcdef\", \"varint_col\": \"9223372036854775807\"}",
          "{\"primary_key\": \"29e38561-6376-4b45-b1a0-1709e11cfc8c\", \"ascii_col\": \"\", \"bigint_col\": -9223372036854775808, \"blob_col\": \"00\", \"boolean_col\": false, \"date_col\": -354285, \"decimal_col\": \"-10000000000000000000000000000000000000\", \"double_col\": -1.7976931348623157E308, \"duration_col\": {\"years\": 0, \"months\": 0, \"days\": 0, \"hours\": 0, \"minutes\": 0, \"seconds\": 0, \"nanos\": 320949000000000}, \"float_col\": -3.4028235E38, \"inet_col\": \"/0.0.0.0\", \"int_col\": -2147483648, \"smallint_col\": -32768, \"text_col\": \"\", \"time_col\": 0, \"timestamp_col\": 0, \"timeuuid_col\": null, \"tinyint_col\": -128, \"uuid_col\": \"fff6d876-560f-48bc-8088-90c69e5a0c40\", \"varchar_col\": \"\", \"varint_col\": \"-9223372036854775808\"}");
  public static final ImmutableList<String> LIST_TYPES_TABLE_AVRO_ROWS =
      ImmutableList.of(
          "{\"primary_key\": \"a389de30-f01f-4395-a0c6-c407bfbe81d0\", \"ascii_list\": [\"a\", \"b\", \"c\"], \"bigint_list\": [1, 2, 3], \"blob_list\": [\""
              + new String(Hex.encodeHex(new byte[] {'H', 'e', 'l', 'l', 'o'}))
              + "\"], \"boolean_list\": [true, false], \"date_list\": [20023, 20024], \"decimal_list\": [\"123.45\", \"678.90\"], \"double_list\": [1.23, 4.56], \"duration_list\": [{\"years\": 0, \"months\": 14, \"days\": 3, \"hours\": 0, \"minutes\": 0, \"seconds\": 0, \"nanos\": 14706000000000}, {\"years\": 0, \"months\": 26, \"days\": 3, \"hours\": 0, \"minutes\": 0, \"seconds\": 0, \"nanos\": 14706000000000}], \"float_list\": [1.23, 4.56], \"frozen_ascii_list\": [\"d\", \"e\", \"f\"], \"inet_list\": [\"/192.168.1.1\", \"/10.0.0.1\"], \"int_list\": [10, 20, 30], \"smallint_list\": [100, 200, 300], \"text_list\": [\"hello\", \"world\"], \"time_list\": [36000000000000, 43200000000000], \"timestamp_list\": [1730023200000000, 1730116800000000], \"timeuuid_list\": [\"9b9419da-b210-11ef-890e-9d9a41af9e54\"], \"tinyint_list\": [1, 2, 3], \"uuid_list\": [\"f0e1d922-06b5-4f07-a7a6-ec0c9f23e172\"], \"varchar_list\": [\"varchar1\", \"varchar2\"], \"varint_list\": [\"1234567890\", \"9876543210\"]}");

  public static final ImmutableList<String> SET_TYPES_TABLE_AVRO_ROWS =
      ImmutableList.of(
          "{\"primary_key\": \"a389de30-f01f-4395-a0c6-c407bfbe81d0\", \"ascii_set\": [\"a\", \"b\", \"c\"], \"bigint_set\": [1, 2, 3], \"blob_set\": [\"48656c6c6f\"], \"boolean_set\": [false, true], \"date_set\": [20023, 20024], \"decimal_set\": [\"123.45\", \"678.90\"], \"double_set\": [1.23, 4.56], \"float_set\": [1.23, 4.56], \"frozen_ascii_set\": [\"d\", \"e\", \"f\"], \"inet_set\": [\"/10.0.0.1\", \"/192.168.1.1\"], \"int_set\": [10, 20, 30], \"smallint_set\": [100, 200, 300], \"text_set\": [\"hello\", \"world\"], \"time_set\": [36000000000000, 43200000000000], \"timestamp_set\": [1730023200000000, 1730116800000000], \"timeuuid_set\": [\"9b9419da-b210-11ef-890e-9d9a41af9e54\"], \"tinyint_set\": [1, 2, 3], \"uuid_set\": [\"f0e1d922-06b5-4f07-a7a6-ec0c9f23e172\"], \"varchar_set\": [\"varchar1\", \"varchar2\"], \"varint_set\": [\"1234567890\", \"9876543210\"]}");

  public static final ImmutableList<String> MAP_TYPES_TABLE_AVRO_ROWS =
      ImmutableList.of(
          "{\"primary_key\": \"550e8400-e29b-41d4-a716-446655440000\", \"ascii_text_map\": \"{\\\"key1\\\":\\\"value1\\\",\\\"key2\\\":\\\"value2\\\"}\", \"bigint_boolean_map\": \"{\\\"123\\\":\\\"true\\\",\\\"456\\\":\\\"false\\\"}\", \"blob_int_map\": \"{\\\"010203\\\":\\\"456\\\",\\\"040506\\\":\\\"789\\\"}\", \"boolean_decimal_map\": \"{\\\"false\\\":\\\"456.789\\\",\\\"true\\\":\\\"123.456\\\"}\", \"date_double_map\": \"{\\\"19737\\\":\\\"1.23\\\",\\\"19738\\\":\\\"4.56\\\"}\", \"decimal_duration_map\": \"{\\\"123.456\\\":\\\"{\\\\\\\"years\\\\\\\": 0, \\\\\\\"months\\\\\\\": 0, \\\\\\\"days\\\\\\\": 1, \\\\\\\"hours\\\\\\\": 0, \\\\\\\"minutes\\\\\\\": 0, \\\\\\\"seconds\\\\\\\": 0, \\\\\\\"nanos\\\\\\\": 45296000000000}\\\",\\\"456.789\\\":\\\"{\\\\\\\"years\\\\\\\": 0, \\\\\\\"months\\\\\\\": 0, \\\\\\\"days\\\\\\\": 0, \\\\\\\"hours\\\\\\\": 0, \\\\\\\"minutes\\\\\\\": 0, \\\\\\\"seconds\\\\\\\": 0, \\\\\\\"nanos\\\\\\\": 65731000000000}\\\"}\", \"double_float_map\": \"{\\\"12.34\\\":\\\"1.23\\\",\\\"45.67\\\":\\\"4.56\\\"}\", \"double_inet_map\": \"{\\\"3.14\\\":\\\"/192.168.1.1\\\",\\\"6.28\\\":\\\"/127.0.0.1\\\"}\", \"float_smallint_map\": \"{\\\"1.23\\\":\\\"10\\\",\\\"4.56\\\":\\\"20\\\"}\", \"inet_text_map\": \"{\\\"/127.0.0.1\\\":\\\"other text\\\",\\\"/192.168.1.1\\\":\\\"some text\\\"}\", \"int_time_map\": \"{\\\"10\\\":\\\"37800000000000\\\",\\\"20\\\":\\\"42300000000000\\\"}\", \"smallint_timestamp_map\": \"{\\\"100\\\":\\\"1705314600000000\\\",\\\"200\\\":\\\"1705405500000000\\\"}\", \"text_timeuuid_map\": \"{\\\"text1\\\":\\\"9b9419da-b210-11ef-890e-9d9a41af9e54\\\",\\\"text2\\\":\\\"9b9419da-b210-11ef-890e-9d9a41af9e54\\\"}\", \"time_tinyint_map\": \"{\\\"37800000000000\\\":\\\"1\\\",\\\"42300000000000\\\":\\\"2\\\"}\", \"timestamp_uuid_map\": \"{\\\"1705314600000000\\\":\\\"550e8400-e29b-41d4-a716-446655440001\\\",\\\"1705405500000000\\\":\\\"550e8400-e29b-41d4-a716-446655440002\\\"}\", \"timeuuid_varchar_map\": \"{\\\"9b9419da-b210-11ef-890e-9d9a41af9e54\\\":\\\"varchar1\\\"}\", \"tinyint_varint_map\": \"{\\\"1\\\":\\\"789\\\",\\\"2\\\":\\\"1234\\\"}\", \"uuid_ascii_map\": \"{\\\"550e8400-e29b-41d4-a716-446655440003\\\":\\\"ascii1\\\",\\\"550e8400-e29b-41d4-a716-446655440004\\\":\\\"ascii2\\\"}\", \"varchar_bigint_map\": \"{\\\"varchar1\\\":\\\"123\\\",\\\"varchar2\\\":\\\"456\\\"}\", \"varint_blob_map\": \"{\\\"789\\\":\\\"010203\\\",\\\"1234\\\":\\\"040506\\\"}\"}",
          "{\"primary_key\": \"550e8400-e29b-41d4-a716-446655440005\", \"ascii_text_map\": \"{}\", \"bigint_boolean_map\": \"{}\", \"blob_int_map\": \"{}\", \"boolean_decimal_map\": \"{}\", \"date_double_map\": \"{}\", \"decimal_duration_map\": \"{}\", \"double_float_map\": \"{}\", \"double_inet_map\": \"{}\", \"float_smallint_map\": \"{}\", \"inet_text_map\": \"{}\", \"int_time_map\": \"{}\", \"smallint_timestamp_map\": \"{}\", \"text_timeuuid_map\": \"{}\", \"time_tinyint_map\": \"{}\", \"timestamp_uuid_map\": \"{}\", \"timeuuid_varchar_map\": \"{}\", \"tinyint_varint_map\": \"{}\", \"uuid_ascii_map\": \"{}\", \"varchar_bigint_map\": \"{}\", \"varint_blob_map\": \"{}\"}");

  private BasicTestSchema() {}
  ;
}
