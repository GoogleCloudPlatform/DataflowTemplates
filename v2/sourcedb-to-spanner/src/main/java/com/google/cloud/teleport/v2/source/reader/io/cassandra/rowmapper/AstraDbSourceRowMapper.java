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

import com.datastax.oss.driver.api.core.cql.Row;
import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableSchema;
import java.io.Serializable;
import java.util.concurrent.CompletionStage;
import org.apache.beam.sdk.io.astra.db.mapping.AstraDbMapper;

@AutoValue
public abstract class AstraDbSourceRowMapper implements AstraDbMapper<SourceRow>, Serializable {
  abstract SourceSchemaReference sourceSchemaReference();

  abstract SourceTableSchema sourceTableSchema();

  @Memoized
  public CassandraRowMapper cassandraRowMapper() {
    return CassandraRowMapper.create(sourceSchemaReference(), sourceTableSchema());
  }

  @Override
  public SourceRow mapRow(Row row) {
    return cassandraRowMapper().mapV4(row);
  }

  @Override
  public CompletionStage<Void> deleteAsync(SourceRow entity) {
    throw new UnsupportedOperationException("Only Read from Cassandra is supported");
  }

  @Override
  public CompletionStage<Void> saveAsync(SourceRow entity) {
    throw new UnsupportedOperationException("Only Read from Cassandra is supported");
  }

  public static Builder builder() {
    return new AutoValue_AstraDbSourceRowMapper.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setSourceSchemaReference(SourceSchemaReference value);

    public abstract Builder setSourceTableSchema(SourceTableSchema value);

    public abstract AstraDbSourceRowMapper build();
  }
}
