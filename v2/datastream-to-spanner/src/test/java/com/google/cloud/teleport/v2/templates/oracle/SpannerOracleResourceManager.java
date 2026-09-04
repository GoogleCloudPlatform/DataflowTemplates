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
package com.google.cloud.teleport.v2.templates.oracle;

import org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Custom class for Oracle implementations that require Service Name format (//host:port/Service)
 * instead of the legacy SID format. Specifically built for Spanner integration tests hitting
 * Pluggable Databases (PDBs) through GCP Datastream.
 */
public class SpannerOracleResourceManager extends CloudOracleResourceManager {

  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(SpannerOracleResourceManager.class);

  public SpannerOracleResourceManager(CloudOracleResourceManager.Builder builder) {
    super(builder);
  }

  @Override
  public synchronized @NonNull String getUri() {
    return String.format(
        "jdbc:%s:thin:@//%s:%d/%s",
        getJDBCPrefix(), this.getHost(), this.getPort(getJDBCPort()), this.getDatabaseName());
  }

  @Override
  public void cleanupAll() {
    super.cleanupAll();

    String userToDrop = this.getUsername();
    if (userToDrop != null
        && !userToDrop.equalsIgnoreCase("system")
        && !userToDrop.contains("sysdba")) {
      LOG.info("Attempting to dynamically drop isolated Oracle user: {}", userToDrop);
      try {
        CloudOracleResourceManager sysdba =
            (CloudOracleResourceManager)
                CloudOracleResourceManager.builder(this.getDatabaseName() + "_cleanup")
                    .setUsername("sys as sysdba")
                    .setPassword(System.getProperty("cloudOraclePassword", "TestPassword123"))
                    .setDatabaseName("XEPDB1")
                    .setHost(System.getProperty("cloudOracleHost", this.getHost()))
                    .setPort(1521)
                    .build();

        // Oracle Spanner implementation requires specific URI
        sysdba =
            new SpannerOracleResourceManager(
                (CloudOracleResourceManager.Builder)
                    CloudOracleResourceManager.builder(this.getDatabaseName() + "_cleanup")
                        .setUsername("sys as sysdba")
                        .setPassword(System.getProperty("cloudOraclePassword", "TestPassword123"))
                        .setDatabaseName("XEPDB1")
                        .setHost(System.getProperty("cloudOracleHost", this.getHost()))
                        .setPort(1521));

        sysdba.runSQLUpdate("DROP USER " + userToDrop + " CASCADE");
        LOG.info("Successfully dropped Oracle schema: {}", userToDrop);
      } catch (Exception e) {
        LOG.warn("Failed to drop schema: " + userToDrop, e);
      }
    }
  }
}
