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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Encapsulates the configuration for multiple JDBC shards. This is used to build a unified IO
 * wrapper for a single dependency level.
 *
 * <p>To optimize resource usage, callers should ensure that all tables belonging to a specific
 * logical shard for a given dependency level are included within a single configuration group.
 * Passing the same shard multiple times across different groups may lead to duplicate {@link
 * javax.sql.DataSource} instances. At the moment this fits naturally in the flow of pipeline
 * controller.
 */
@AutoValue
public abstract class JdbcIoWrapperConfigGroup {

  /**
   * Mapping from shard identifier to its individual JDBC configuration.
   *
   * @return A list of {@link JdbcIOWrapperConfig} instances.
   */
  public abstract ImmutableList<JdbcIOWrapperConfig> shardConfigs();

  /**
   * The SQL dialect of the source databases. Expected to be uniform across shards.
   *
   * @return The {@link SQLDialect} of the source databases.
   */
  public abstract SQLDialect sourceDbDialect();

  /**
   * Returns a new builder for {@link JdbcIoWrapperConfigGroup}.
   *
   * @return A new builder instance.
   */
  public static Builder builder() {
    return new AutoValue_JdbcIoWrapperConfigGroup.Builder();
  }

  /** Builder for {@link JdbcIoWrapperConfigGroup}. */
  @AutoValue.Builder
  public abstract static class Builder {
    /**
     * Internal builder for the list of shard configurations.
     *
     * @return The shard configurations builder.
     */
    abstract ImmutableList.Builder<JdbcIOWrapperConfig> shardConfigsBuilder();

    /**
     * Adds a single shard configuration to the group.
     *
     * <p>This method ensures that the dialect of the added shard matches the dialect of the group.
     * If the group doesn't have a dialect set yet, it will be automatically set to the shard's
     * dialect.
     *
     * @param config The JDBC configuration for the shard.
     * @return This builder instance for fluent chaining.
     * @throws IllegalArgumentException if the shard's dialect doesn't match the group's dialect.
     */
    public Builder addShardConfig(JdbcIOWrapperConfig config) {
      if (!this.sourceDbDialect().isPresent()) {
        this.setSourceDbDialect(config.sourceDbDialect());
      } else {
        Preconditions.checkArgument(
            this.sourceDbDialect().get().equals(config.sourceDbDialect()),
            "Migrating mixed SourceDB Dialects is not supported");
      }
      shardConfigsBuilder().add(config);
      return this;
    }

    /**
     * Sets multiple shard configurations at once.
     *
     * @param shardConfigs A list of shard configurations.
     * @return This builder instance for fluent chaining.
     */
    public Builder setShardConfigs(ImmutableList<JdbcIOWrapperConfig> shardConfigs) {
      shardConfigs.forEach(this::addShardConfig);
      return this;
    }

    /**
     * Internal optional for checking the currently set dialect.
     *
     * @return An optional containing the SQL dialect.
     */
    @Nullable
    abstract Optional<SQLDialect> sourceDbDialect();

    /**
     * Explicitly sets the SQL dialect for the configuration group.
     *
     * @param sourceDbDialect The SQL dialect to set.
     * @return This builder instance.
     */
    public abstract Builder setSourceDbDialect(SQLDialect sourceDbDialect);

    /**
     * Builds the {@link JdbcIoWrapperConfigGroup} instance.
     *
     * @return The built configuration group.
     */
    public abstract JdbcIoWrapperConfigGroup build();
  }
}
