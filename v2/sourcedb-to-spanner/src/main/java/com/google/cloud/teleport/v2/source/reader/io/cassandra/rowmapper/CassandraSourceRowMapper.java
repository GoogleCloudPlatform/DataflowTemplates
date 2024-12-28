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

import com.datastax.driver.core.ResultSet;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableSchema;
import java.io.Serializable;
import java.util.Iterator;
import java.util.concurrent.Future;
import org.apache.beam.sdk.io.cassandra.Mapper;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

@AutoValue
public abstract class CassandraSourceRowMapper implements Mapper<SourceRow>, Serializable {
  abstract SourceSchemaReference sourceSchemaReference();

  abstract SourceTableSchema sourceTableSchema();

  @Override
  public @UnknownKeyFor @NonNull @Initialized Iterator<SourceRow> map(
      @UnknownKeyFor @NonNull @Initialized ResultSet resultSet) {
    var ret = new TransformIterator();
    ret.setIterator(resultSet.iterator());
    ret.setTransformer(CassandraRowMapper.create(sourceSchemaReference(), sourceTableSchema()));
    return ret;
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized Future<@UnknownKeyFor @Nullable @Initialized Void>
      deleteAsync(SourceRow entity) {
    throw new UnsupportedOperationException("Only Read from Cassandra is supported");
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized Future<@UnknownKeyFor @Nullable @Initialized Void>
      saveAsync(SourceRow entity) {
    throw new UnsupportedOperationException("Only Read from Cassandra is supported");
  }

  public static Builder builder() {
    return new AutoValue_CassandraSourceRowMapper.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setSourceSchemaReference(SourceSchemaReference value);

    public abstract Builder setSourceTableSchema(SourceTableSchema value);

    public abstract CassandraSourceRowMapper build();
  }
}
