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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter;

import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper.CassandraDataSource;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.schema.CassandraSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter.MySqlVersion;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DialectAdapterTest {
  @Mock CassandraSchemaReference mockCassandraSchemaReference;
  @Mock CassandraDataSource mockCassandraDataSource;

  @Test
  public void testMismatchedSource() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new MysqlDialectAdapter(MySqlVersion.DEFAULT)
                .discoverTableSchema(
                    com.google.cloud.teleport.v2.source.reader.io.datasource.DataSource.ofCassandra(
                        mockCassandraDataSource),
                    SourceSchemaReference.ofCassandra(mockCassandraSchemaReference),
                    ImmutableList.of()));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new MysqlDialectAdapter(MySqlVersion.DEFAULT)
                .discoverTables(
                    com.google.cloud.teleport.v2.source.reader.io.datasource.DataSource.ofCassandra(
                        mockCassandraDataSource),
                    SourceSchemaReference.ofCassandra(mockCassandraSchemaReference)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new MysqlDialectAdapter(MySqlVersion.DEFAULT)
                .discoverTableIndexes(
                    com.google.cloud.teleport.v2.source.reader.io.datasource.DataSource.ofCassandra(
                        mockCassandraDataSource),
                    SourceSchemaReference.ofCassandra(mockCassandraSchemaReference),
                    ImmutableList.of()));
  }
}
