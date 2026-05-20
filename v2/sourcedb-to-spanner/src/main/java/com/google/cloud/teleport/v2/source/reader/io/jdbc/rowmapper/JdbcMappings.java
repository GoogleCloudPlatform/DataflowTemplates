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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.function.Function;

/** Registry for JDBC type mappings, including value extraction, mapping, and size estimation. */
@AutoValue
public abstract class JdbcMappings implements Serializable {

  public abstract ImmutableMap<String, JdbcValueMapper<?>> mappings();

  public abstract ImmutableMap<String, Function<SourceColumnType, Integer>> sizeEstimators();

  public static Builder builder() {
    return new AutoValue_JdbcMappings.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    abstract ImmutableMap.Builder<String, JdbcValueMapper<?>> mappingsBuilder();

    abstract ImmutableMap.Builder<String, Function<SourceColumnType, Integer>>
        sizeEstimatorsBuilder();

    /**
     * Register a mapping with a constant size estimate.
     *
     * @param typeName The JDBC type name (e.g., "VARCHAR").
     * @param extractor The extractor to get value from ResultSet.
     * @param mapper The mapper to convert value to Avro.
     * @param constantSize The constant size estimate in bytes.
     */
    public Builder put(
        String typeName,
        ResultSetValueExtractor<?> extractor,
        ResultSetValueMapper<?> mapper,
        int constantSize) {
      return put(typeName, extractor, mapper, (ignore) -> constantSize);
    }

    /**
     * Register a mapping with a dynamic size estimator.
     *
     * @param typeName The JDBC type name (e.g., "VARCHAR").
     * @param extractor The extractor to get value from ResultSet.
     * @param mapper The mapper to convert value to Avro.
     * @param sizeEstimator The function to estimate size based on SourceColumnType.
     */
    public Builder put(
        String typeName,
        ResultSetValueExtractor<?> extractor,
        ResultSetValueMapper<?> mapper,
        Function<SourceColumnType, Integer> sizeEstimator) {
      mappingsBuilder().put(typeName, new JdbcValueMapper<>(extractor, mapper));
      sizeEstimatorsBuilder().put(typeName, sizeEstimator);
      return this;
    }

    public abstract JdbcMappings build();
  }
}
