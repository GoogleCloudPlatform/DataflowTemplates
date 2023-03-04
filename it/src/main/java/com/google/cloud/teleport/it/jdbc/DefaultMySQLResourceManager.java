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
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Default class for the MySQL implementation of {@link AbstractJDBCResourceManager} abstract class.
 *
 * <p>The class supports one database, and multiple tables per database object. A database is *
 * created when the container first spins up, if one is not given.
 *
 * <p>The class is thread-safe.
 */
public class DefaultMySQLResourceManager extends AbstractJDBCResourceManager<MySQLContainer<?>> {

  private static final String DEFAULT_MYSQL_CONTAINER_NAME = "mysql";

  // A list of available mySQL Docker image tags can be found at
  // https://hub.docker.com/_/mysql/tags?tab=tags
  private static final String DEFAULT_MYSQL_CONTAINER_TAG = "8.0.30";

  private DefaultMySQLResourceManager(Builder builder) {
    this(
        new MySQLContainer<>(
            DockerImageName.parse(builder.containerImageName).withTag(builder.containerImageTag)),
        builder);
  }

  @VisibleForTesting
  DefaultMySQLResourceManager(MySQLContainer<?> container, Builder builder) {
    super(container, builder);
  }

  public static DefaultMySQLResourceManager.Builder builder(String testId) {
    return new DefaultMySQLResourceManager.Builder(testId);
  }

  @Override
  protected int getJDBCPort() {
    return MySQLContainer.MYSQL_PORT;
  }

  @Override
  public String getJDBCPrefix() {
    return "mysql";
  }

  /** Builder for {@link DefaultMySQLResourceManager}. */
  public static final class Builder extends AbstractJDBCResourceManager.Builder<MySQLContainer<?>> {

    public Builder(String testId) {
      super(testId);
      this.containerImageName = DEFAULT_MYSQL_CONTAINER_NAME;
      this.containerImageTag = DEFAULT_MYSQL_CONTAINER_TAG;
    }

    @Override
    public DefaultMySQLResourceManager build() {
      return new DefaultMySQLResourceManager(this);
    }
  }
}
