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
  public static final ImmutableMap<String, ImmutableMap<String, SourceColumnType>>
      TEST_TABLE_SCHEMA =
          ImmutableMap.of(
              "basic_test_table",
              ImmutableMap.of(
                  "id", new SourceColumnType("TEXT", new Long[] {}, new Long[] {}),
                  "name", new SourceColumnType("TEXT", new Long[] {}, new Long[] {})));
  public static final ImmutableList<String> TEST_TABLES =
      ImmutableList.copyOf(TEST_TABLE_SCHEMA.keySet());

  private BasicTestSchema() {}
  ;
}
