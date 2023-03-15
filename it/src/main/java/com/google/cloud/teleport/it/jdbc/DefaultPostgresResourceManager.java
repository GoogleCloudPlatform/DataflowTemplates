/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.it.jdbc;

import com.google.common.annotations.VisibleForTesting;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Default class for the Postgres implementation of {@link AbstractJDBCResourceManager} abstract
 * class.
 *
 * <p>The class supports one database, and multiple tables per database object. A database is *
 * created when the container first spins up, if one is not given.
 *
 * <p>The class is thread-safe.
 */
public class DefaultPostgresResourceManager
    extends AbstractJDBCResourceManager<PostgreSQLContainer<?>> {

  private static final String DEFAULT_POSTGRES_CONTAINER_NAME = "postgres";

  // A list of available postgres Docker image tags can be found at
  // https://hub.docker.com/_/postgres/tags?tab=tags
  private static final String DEFAULT_POSTGRES_CONTAINER_TAG = "15.1";

  private DefaultPostgresResourceManager(DefaultPostgresResourceManager.Builder builder) {
    this(
        new PostgreSQLContainer<>(
            DockerImageName.parse(builder.containerImageName).withTag(builder.containerImageTag)),
        builder);
  }

  @VisibleForTesting
  DefaultPostgresResourceManager(
      PostgreSQLContainer<?> container, DefaultPostgresResourceManager.Builder builder) {
    super(container, builder);
  }

  public static DefaultPostgresResourceManager.Builder builder(String testId) {
    return new DefaultPostgresResourceManager.Builder(testId);
  }

  @Override
  protected int getJDBCPort() {
    return PostgreSQLContainer.POSTGRESQL_PORT;
  }

  @Override
  public String getJDBCPrefix() {
    return "postgresql";
  }

  @Override
  public synchronized String getDockerImageName() {
    return "postgresql";
  }

  /** Builder for {@link DefaultPostgresResourceManager}. */
  public static final class Builder
      extends AbstractJDBCResourceManager.Builder<PostgreSQLContainer<?>> {

    public Builder(String testId) {
      super(testId);
      this.containerImageName = DEFAULT_POSTGRES_CONTAINER_NAME;
      this.containerImageTag = DEFAULT_POSTGRES_CONTAINER_TAG;
    }

    @Override
    public DefaultPostgresResourceManager build() {
      return new DefaultPostgresResourceManager(this);
    }
  }
}
