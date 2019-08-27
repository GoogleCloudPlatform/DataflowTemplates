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

import com.datastax.driver.core.Session;
import org.apache.beam.sdk.io.cassandra.Mapper;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * The CassandraRowMapperFactory is a factory method used to inject the {@link CassandraRowMapperFn}
 * into the CassandaIO.
 *
 * @see CassandraRowMapperFn
 */
public class CassandraRowMapperFactory implements SerializableFunction<Session, Mapper> {

  private final ValueProvider<String> keyspace;
  private final ValueProvider<String> table;

  /**
   * @param cassandraTable the Cassandra table to read from.
   * @param cassandraKeyspace the Cassandra keyspace to read from.
   */
  public CassandraRowMapperFactory(
      ValueProvider<String> cassandraTable, ValueProvider<String> cassandraKeyspace) {
    this.keyspace = cassandraKeyspace;
    this.table = cassandraTable;
  }

  @Override
  public Mapper apply(Session session) {
    return new CassandraRowMapperFn(session, keyspace, table);
  }
}
