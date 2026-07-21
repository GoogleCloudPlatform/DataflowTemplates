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
package com.google.cloud.teleport.v2.templates.utils;

import org.apache.beam.it.jdbc.AbstractJDBCResourceManager;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Custom MySQL resource manager used for Spanner to SourceDB load tests. This configures the
 * container with additional settings like innodb buffer pool.
 */
public class LTMySQLResourceManager extends AbstractJDBCResourceManager<MySQLContainer<?>> {

  private static final String DEFAULT_MYSQL_CONTAINER_NAME = "mysql";
  private static final String DEFAULT_MYSQL_CONTAINER_TAG = "8.0.30";

  private LTMySQLResourceManager(Builder builder) {
    this(
        new MySQLContainer<>(
                DockerImageName.parse(builder.containerImageName)
                    .withTag(builder.containerImageTag))
            .withCommand("--innodb_buffer_pool_size=1G", "--innodb_redo_log_capacity=1G"),
        builder);
  }

  LTMySQLResourceManager(MySQLContainer<?> container, Builder builder) {
    super(container, builder);
  }

  public static Builder builder(String testId) {
    return new Builder(testId);
  }

  @Override
  protected int getJDBCPort() {
    return MySQLContainer.MYSQL_PORT;
  }

  @Override
  public String getJDBCPrefix() {
    return "mysql";
  }

  public static final class Builder extends AbstractJDBCResourceManager.Builder<MySQLContainer<?>> {

    public Builder(String testId) {
      super(testId, DEFAULT_MYSQL_CONTAINER_NAME, DEFAULT_MYSQL_CONTAINER_TAG);
    }

    @Override
    public LTMySQLResourceManager build() {
      return new LTMySQLResourceManager(this);
    }
  }
}
