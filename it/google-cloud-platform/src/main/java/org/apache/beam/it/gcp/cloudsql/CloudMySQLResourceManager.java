/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.it.gcp.cloudsql;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom class for the MySQL implementation of {@link CloudSqlResourceManager} abstract class.
 *
 * <p>The class supports one database, and multiple tables per database object. A database is *
 * created when the container first spins up, if one is not given.
 *
 * <p>The class is thread-safe.
 */
public class CloudMySQLResourceManager extends CloudSqlResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(CloudMySQLResourceManager.class);

  private CloudMySQLResourceManager(Builder builder) {
    super(builder);
  }

  public static Builder builder(String testId) {
    return new Builder(testId);
  }

  @Override
  public @NonNull String getJDBCPrefix() {
    return "mysql";
  }

  /** Builder for {@link CloudMySQLResourceManager}. */
  public static final class Builder extends CloudSqlResourceManager.Builder {

    public Builder(String testId) {
      super(testId);
    }

    @Override
    protected void configurePort() {
      if (System.getProperty("cloudProxyMySqlPort") != null) {
        this.setPort(Integer.parseInt(System.getProperty("cloudProxyMySqlPort")));
      } else {
        LOG.warn("Missing -DcloudProxyMySqlPort.");
      }
    }

    @Override
    public @NonNull CloudMySQLResourceManager build() {
      return new CloudMySQLResourceManager(this);
    }
  }
}
