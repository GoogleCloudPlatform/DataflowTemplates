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
package com.google.cloud.teleport.v2.utilities;

import org.apache.beam.it.jdbc.AbstractJDBCResourceManager;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Mysql resource manager to be used for sharded migrations. This class has been created as the
 * MySQL resource manager constructor is not public.
 *
 * @see org.apache.beam.it.jdbc.MySQLResourceManager
 */
public class ShardedMySQLResourceManager extends AbstractJDBCResourceManager<MySQLContainer<?>> {

  private static final String DEFAULT_MYSQL_CONTAINER_NAME = "mysql";

  // A list of available mySQL Docker image tags can be found at
  // https://hub.docker.com/_/mysql/tags?tab=tags
  private static final String DEFAULT_MYSQL_CONTAINER_TAG = "8.0.30";

  private int jdbcPort;

  private ShardedMySQLResourceManager(ShardedMySQLResourceManager.Builder builder) {
    this(
        new MySQLContainer<>(
            DockerImageName.parse(builder.containerImageName).withTag(builder.containerImageTag)),
        builder);
  }

  @VisibleForTesting
  ShardedMySQLResourceManager(
      MySQLContainer<?> container, ShardedMySQLResourceManager.Builder builder) {
    super(container, builder);
    this.jdbcPort = builder.jdbcPort;
  }

  public static ShardedMySQLResourceManager.Builder builder(String testId, int port) {
    return new ShardedMySQLResourceManager.Builder(testId, port);
  }

  @Override
  protected int getJDBCPort() {
    return this.jdbcPort;
  }

  @Override
  public String getJDBCPrefix() {
    return "mysql";
  }

  /** Builder for {@link ShardedMySQLResourceManager}. */
  public static final class Builder extends AbstractJDBCResourceManager.Builder<MySQLContainer<?>> {

    int jdbcPort;

    public Builder(String testId, int jdbcPort) {
      super(testId, DEFAULT_MYSQL_CONTAINER_NAME, DEFAULT_MYSQL_CONTAINER_TAG);
      this.jdbcPort = jdbcPort;
    }

    @Override
    public ShardedMySQLResourceManager build() {
      return new ShardedMySQLResourceManager(this);
    }
  }
}
