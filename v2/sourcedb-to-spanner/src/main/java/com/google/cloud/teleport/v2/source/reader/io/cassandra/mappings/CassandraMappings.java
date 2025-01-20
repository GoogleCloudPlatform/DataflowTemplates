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
package com.google.cloud.teleport.v2.source.reader.io.cassandra.mappings;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.rowmapper.CassandraFieldMapper;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.rowmapper.CassandraRowValueExtractor;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.rowmapper.CassandraRowValueMapper;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.UnifiedTypeMapping;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.UnifiedMappingProvider;
import com.google.common.collect.ImmutableMap;

/** Represent Unified type mapping, value extractor and value mappings for Cassandra. */
@AutoValue
public abstract class CassandraMappings {
  public abstract ImmutableMap<String, UnifiedTypeMapping> typeMapping();

  public abstract ImmutableMap<String, CassandraFieldMapper<?>> fieldMapping();

  public static Builder builder() {
    return new AutoValue_CassandraMappings.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    abstract ImmutableMap.Builder<String, UnifiedTypeMapping> typeMappingBuilder();

    abstract ImmutableMap.Builder<String, CassandraFieldMapper<?>> fieldMappingBuilder();

    public <T> Builder put(
        String cassandraType,
        UnifiedMappingProvider.Type type,
        CassandraRowValueExtractor<T> rowValueExtractor,
        CassandraRowValueMapper<T> rowValueMapper) {
      this.typeMappingBuilder()
          .put(cassandraType.toUpperCase(), UnifiedMappingProvider.getMapping(type));
      this.fieldMappingBuilder()
          .put(
              cassandraType.toUpperCase(),
              CassandraFieldMapper.create(rowValueExtractor, rowValueMapper));
      return this;
    }

    public abstract CassandraMappings build();
  }
}
