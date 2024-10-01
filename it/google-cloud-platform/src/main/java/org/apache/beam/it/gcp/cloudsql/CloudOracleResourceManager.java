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
 * Custom class for the Oracle implementation of {@link CloudSqlResourceManager} abstract class.
 *
 * <p>This class leverages the API in the {@link CloudSqlResourceManager}, but assumes a self-hosted
 * Oracle instance on GCE or other provider.
 *
 * <p>The class supports one database (XE), and multiple tables. The database must already exist to
 * use this resource manager.
 *
 * <p>The class is thread-safe.
 */
public class CloudOracleResourceManager extends CloudSqlResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(CloudOracleResourceManager.class);

  private static final String DEFAULT_SYSTEM_IDENTIFIER = "xe";

  private static final int DEFAULT_ORACLE_PORT = 1521;

  private String systemIdentifier;

  protected CloudOracleResourceManager(Builder builder) {
    super(builder);

    this.systemIdentifier = builder.systemIdentifier;
    System.setProperty("oracle.jdbc.timezoneAsRegion", "false");
  }

  public static Builder builder(String testId) {
    return new Builder(testId);
  }

  @Override
  public @NonNull String getJDBCPrefix() {
    return "oracle";
  }

  @Override
  public synchronized @NonNull String getUri() {
    return String.format(
        "jdbc:%s:thin:@%s:%d:%s",
        getJDBCPrefix(), this.getHost(), this.getPort(getJDBCPort()), this.getDatabaseName());
  }

  @Override
  protected @NonNull String getFirstRow(@NonNull String tableName) {
    return "SELECT * FROM " + tableName + " WHERE ROWNUM <= 1";
  }

  /**
   * Return the SID of the connected DB.
   *
   * @return the SID.
   */
  public String getSystemIdentifier() {
    return this.systemIdentifier;
  }

  /** Builder for {@link CloudOracleResourceManager}. */
  public static final class Builder extends CloudSqlResourceManager.Builder {

    private String systemIdentifier;

    public Builder(String testId) {
      super(testId);

      this.setSystemIdentifier(DEFAULT_SYSTEM_IDENTIFIER);
      this.setDatabaseName(this.systemIdentifier);
    }

    @Override
    protected void configureHost() {
      if (System.getProperty("cloudOracleHost") != null) {
        this.setHost(System.getProperty("cloudOracleHost"));
      } else {
        LOG.warn("Missing -DcloudOracleHost.");
      }
    }

    @Override
    protected void configurePort() {
      if (System.getProperty("cloudOraclePort") != null) {
        this.setPort(Integer.parseInt(System.getProperty("cloudOraclePort")));
      } else {
        this.setPort(DEFAULT_ORACLE_PORT);
      }
    }

    @Override
    protected void configureUsername() {
      if (System.getProperty("cloudOracleUsername") != null) {
        this.setUsername(System.getProperty("cloudOracleUsername"));
      } else {
        super.configureUsername();
      }
    }

    @Override
    protected void configurePassword() {
      if (System.getProperty("cloudOraclePassword") != null) {
        this.setPassword(System.getProperty("cloudOraclePassword"));
      } else {
        super.configurePassword();
      }
    }

    public Builder setSystemIdentifier(String systemIdentifier) {
      this.systemIdentifier = systemIdentifier;
      return this;
    }

    @Override
    public @NonNull CloudOracleResourceManager build() {
      return new CloudOracleResourceManager(this);
    }
  }
}
