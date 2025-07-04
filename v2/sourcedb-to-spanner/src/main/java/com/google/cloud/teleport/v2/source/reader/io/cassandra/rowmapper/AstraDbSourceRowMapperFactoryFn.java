/*
 * Copyright (C) 2025 Google LLC
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

import com.datastax.oss.driver.api.core.CqlSession;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import org.apache.beam.sdk.io.astra.db.mapping.AstraDbMapper;
import org.apache.beam.sdk.io.cassandra.CassandraIO;
import org.apache.beam.sdk.io.cassandra.Mapper;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * A simple utility to wrap {@link CassandraSourceRowMapper} into a mapperFactory. The {@link
 * CassandraIO.Read} api takes in {@link CassandraIO.Read#withMapperFactoryFn(SerializableFunction)}
 * which is a {@link SerializableFunction} that returns the actual {@link Mapper}.
 *
 * <p>{@link CassandraSourceRowMapper} maps the {@link com.datastax.driver.core.ResultSet Cassandra
 * ResultSet} to {@link SourceRow}.
 */
@AutoValue
public abstract class AstraDbSourceRowMapperFactoryFn
    implements SerializableFunction<CqlSession, AstraDbMapper<SourceRow>> {
  public static AstraDbSourceRowMapperFactoryFn create(
      AstraDbSourceRowMapper astraDbSourceRowMapper) {
    return new AutoValue_AstraDbSourceRowMapperFactoryFn(astraDbSourceRowMapper);
  }

  public abstract AstraDbSourceRowMapper astraDbSourceRowMapper();

  @Override
  public AstraDbMapper<SourceRow> apply(CqlSession input) {
    return astraDbSourceRowMapper();
  }
}
