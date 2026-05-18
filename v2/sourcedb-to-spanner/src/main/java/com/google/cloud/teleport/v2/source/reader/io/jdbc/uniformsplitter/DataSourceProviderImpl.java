/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;

/**
 * Concrete implementation of {@link DataSourceProvider} using {@link AutoValue}. It stores a
 * mapping of shard IDs to serializable DataSource provider functions.
 */
@AutoValue
public abstract class DataSourceProviderImpl implements DataSourceProvider, HasDisplayData {

  /**
   * Returns a mapping of shard identifiers to their corresponding serializable {@link DataSource}
   * provider functions.
   *
   * @return The immutable map of providers.
   */
  public abstract ImmutableMap<String, SerializableFunction<Void, DataSource>> providers();

  /**
   * Returns a new builder for {@link DataSourceProviderImpl}.
   *
   * @return A new builder instance.
   */
  public static Builder builder() {
    return new AutoValue_DataSourceProviderImpl.Builder();
  }

  /**
   * Returns a {@link DataSource} for the given identifier.
   *
   * @param datasourceId The unique ID of the physical shard.
   * @return The DataSource for the shard.
   * @throws IllegalArgumentException if no provider is found for the given ID.
   */
  @Override
  public DataSource getDataSource(String datasourceId) {
    SerializableFunction<Void, DataSource> provider = providers().get(datasourceId);
    if (provider == null) {
      throw new IllegalArgumentException("Unknown datasourceId: " + datasourceId);
    }
    return provider.apply(null);
  }

  /**
   * Returns a set of all DataSource identifiers managed by this provider.
   *
   * @return An immutable set of DataSource IDs.
   */
  @Override
  public ImmutableSet<String> getDataSourceIds() {
    return ImmutableSet.copyOf(providers().keySet());
  }

  /**
   * Populates display data with information about each configured data source.
   *
   * @param builder The display data builder.
   */
  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    for (Map.Entry<String, SerializableFunction<Void, DataSource>> entry : providers().entrySet()) {
      if (entry.getValue() instanceof HasDisplayData) {
        builder.include("dataSource-" + entry.getKey(), (HasDisplayData) entry.getValue());
      }
    }
  }

  /** Builder for {@link DataSourceProviderImpl}. */
  @AutoValue.Builder
  public abstract static class Builder {
    /**
     * Internal builder for the providers map.
     *
     * @return The providers builder.
     */
    abstract ImmutableMap.Builder<String, SerializableFunction<Void, DataSource>>
        providersBuilder();

    /**
     * Adds a DataSource provider with a specific identifier.
     *
     * @param datasourceId The unique ID for this physical shard.
     * @param provider The serializable function to provide the DataSource.
     * @return This builder instance for fluent chaining.
     * @throws IllegalArgumentException if the datasourceId has already been added.
     */
    public Builder addDataSource(
        String datasourceId, SerializableFunction<Void, DataSource> provider) {
      providersBuilder().put(datasourceId, provider);
      return this;
    }

    /**
     * Builds the {@link DataSourceProviderImpl} instance.
     *
     * @return The built instance.
     */
    public abstract DataSourceProviderImpl build();
  }
}
