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
  public static final ImmutableMap<String, ImmutableMap<String, SourceColumnType>>
      BASIC_TEST_TABLE_SCHEMA =
          ImmutableMap.of(
              BASIC_TEST_TABLE,
              ImmutableMap.of(
                  "id", new SourceColumnType("TEXT", new Long[] {}, new Long[] {}),
                  "name", new SourceColumnType("TEXT", new Long[] {}, new Long[] {})));
  public static final ImmutableList<String> TEST_TABLES =
      ImmutableList.of(BASIC_TEST_TABLE, PRIMITIVE_TYPES_TABLE);

  public static final ImmutableList<String> PRIMITIVE_TYPES_TABLE_AVRO_ROWS =
      ImmutableList.of(
          "{\"primary_key\": \"dfcad8f3-3cdc-49c7-bce9-575f307c0637\", \"ascii_col\": \"ascii1\", \"bigint_col\": 1234567890, \"blob_col\": \"cafebabe\", \"boolean_col\": true, \"date_col\": 19694, \"decimal_col\": \"123.456\", \"double_col\": 123.456789, \"duration_col\": {\"years\": 0, \"months\": 0, \"days\": 0, \"hours\": 0, \"minutes\": 0, \"seconds\": 0, \"nanos\": 45296000000000}, \"float_col\": 123.45, \"inet_col\": \"/127.0.0.1\", \"int_col\": 12345, \"smallint_col\": 123, \"text_col\": \"text1\", \"time_col\": {\"years\": 0, \"months\": 0, \"days\": 0, \"hours\": 0, \"minutes\": 0, \"seconds\": 0, \"nanos\": 45296789000000}, \"timestamp_col\": 1733296987117000000, \"timeuuid_col\": \"9b9419da-b210-11ef-890e-9d9a41af9e54\", \"tinyint_col\": 123, \"uuid_col\": \"c3de3455-6b4e-4a81-a6d7-ab61610f08c6\", \"varchar_col\": \"varchar1\", \"varint_col\": \"1234567890123456789\"}",
          "{\"primary_key\": \"fe3263a0-1577-4851-95f8-3af47628baa4\", \"ascii_col\": \"ascii2\", \"bigint_col\": 9876543210, \"blob_col\": \"deadbeef\", \"boolean_col\": false, \"date_col\": 19298, \"decimal_col\": \"987.654\", \"double_col\": 987.654321, \"duration_col\": {\"years\": 0, \"months\": 0, \"days\": 0, \"hours\": 0, \"minutes\": 0, \"seconds\": 0, \"nanos\": -45296000000000}, \"float_col\": 987.65, \"inet_col\": \"/0:0:0:0:0:0:0:1\", \"int_col\": 98765, \"smallint_col\": 987, \"text_col\": \"text2\", \"time_col\": {\"years\": 0, \"months\": 0, \"days\": 0, \"hours\": 0, \"minutes\": 0, \"seconds\": 0, \"nanos\": 86399999000000}, \"timestamp_col\": 1733296987122000000, \"timeuuid_col\": \"9b94dd2a-b210-11ef-890e-9d9a41af9e54\", \"tinyint_col\": -123, \"uuid_col\": \"6324e301-94fb-44fe-95ac-91d2f7236e2e\", \"varchar_col\": \"varchar2\", \"varint_col\": \"-9876543210987654321\"}",
          "{\"primary_key\": \"9a0acb7d-674c-4ee1-9644-9da24b7a72f4\", \"ascii_col\": \"ascii3\", \"bigint_col\": 1010101010, \"blob_col\": \"facefeed\", \"boolean_col\": true, \"date_col\": 19723, \"decimal_col\": \"10.101\", \"double_col\": 10.10101, \"duration_col\": {\"years\": 0, \"months\": 14, \"days\": 3, \"hours\": 0, \"minutes\": 0, \"seconds\": 0, \"nanos\": 14706000000000}, \"float_col\": 10.1, \"inet_col\": \"/192.168.1.1\", \"int_col\": 10101, \"smallint_col\": 101, \"text_col\": \"text3\", \"time_col\": {\"years\": 0, \"months\": 0, \"days\": 0, \"hours\": 0, \"minutes\": 0, \"seconds\": 0, \"nanos\": 0}, \"timestamp_col\": 1733296987127000000, \"timeuuid_col\": \"9b95a07a-b210-11ef-890e-9d9a41af9e54\", \"tinyint_col\": 101, \"uuid_col\": \"f0e1d922-06b5-4f07-a7a6-ec0c9f23e172\", \"varchar_col\": \"varchar3\", \"varint_col\": \"10101010101010101010\"}",
          "{\"primary_key\": \"e6bc8562-2575-420f-9344-9fedc4945f61\", \"ascii_col\": null, \"bigint_col\": 0, \"blob_col\": null, \"boolean_col\": false, \"date_col\": null, \"decimal_col\": null, \"double_col\": 0.0, \"duration_col\": null, \"float_col\": 0.0, \"inet_col\": null, \"int_col\": 0, \"smallint_col\": 0, \"text_col\": null, \"time_col\": {\"years\": 0, \"months\": 0, \"days\": 0, \"hours\": 0, \"minutes\": 0, \"seconds\": 0, \"nanos\": 0}, \"timestamp_col\": null, \"timeuuid_col\": null, \"tinyint_col\": 0, \"uuid_col\": null, \"varchar_col\": null, \"varint_col\": null}",
          "{\"primary_key\": \"a389de30-f01f-4395-a0c6-c407bfbe81d0\", \"ascii_col\": \"zzzzzzzzzz\", \"bigint_col\": 9223372036854775807, \"blob_col\": \"ffffffff\", \"boolean_col\": true, \"date_col\": 2932896, \"decimal_col\": \"10000000000000000000000000000000000000\", \"double_col\": 1.7976931348623157E308, \"duration_col\": {\"years\": 0, \"months\": 0, \"days\": 0, \"hours\": 0, \"minutes\": 0, \"seconds\": 0, \"nanos\": 320949000000000}, \"float_col\": 3.4028235E38, \"inet_col\": \"/255.255.255.255\", \"int_col\": 2147483647, \"smallint_col\": 32767, \"text_col\": \"abcdef\", \"time_col\": {\"years\": 0, \"months\": 0, \"days\": 0, \"hours\": 0, \"minutes\": 0, \"seconds\": 0, \"nanos\": 86399999000000}, \"timestamp_col\": -1000, \"timeuuid_col\": null, \"tinyint_col\": 127, \"uuid_col\": \"00e4afef-52f8-4e1f-9afa-0632c8ccf790\", \"varchar_col\": \"abcdef\", \"varint_col\": \"9223372036854775807\"}",
          "{\"primary_key\": \"29e38561-6376-4b45-b1a0-1709e11cfc8c\", \"ascii_col\": \"\", \"bigint_col\": -9223372036854775808, \"blob_col\": \"00\", \"boolean_col\": false, \"date_col\": -354285, \"decimal_col\": \"-10000000000000000000000000000000000000\", \"double_col\": -1.7976931348623157E308, \"duration_col\": {\"years\": 0, \"months\": 0, \"days\": 0, \"hours\": 0, \"minutes\": 0, \"seconds\": 0, \"nanos\": 320949000000000}, \"float_col\": -3.4028235E38, \"inet_col\": \"/0.0.0.0\", \"int_col\": -2147483648, \"smallint_col\": -32768, \"text_col\": \"\", \"time_col\": {\"years\": 0, \"months\": 0, \"days\": 0, \"hours\": 0, \"minutes\": 0, \"seconds\": 0, \"nanos\": 0}, \"timestamp_col\": 0, \"timeuuid_col\": null, \"tinyint_col\": -128, \"uuid_col\": \"fff6d876-560f-48bc-8088-90c69e5a0c40\", \"varchar_col\": \"\", \"varint_col\": \"-9223372036854775808\"}");

  private BasicTestSchema() {}
  ;
}
