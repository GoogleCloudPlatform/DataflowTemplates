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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SharedOracleLiveITInstance {
  private static final Logger LOG = LoggerFactory.getLogger(SharedOracleLiveITInstance.class);
  private static SpannerOracleResourceManager cdbAdmin;
  private static SpannerOracleResourceManager pdbAdmin;

  public static final String ORACLE_PASSWORD = "TestPassword123";
  private static final Object lock = new Object();

  private static SpannerOracleResourceManager getCdbAdmin() {
    if (cdbAdmin == null) {
      synchronized (lock) {
        if (cdbAdmin == null) {
          LOG.info("Initializing global Singleton CDB Admin Oracle pool (XE).");
          String host = System.getProperty("cloudOracleHost", "localhost");
          String password = System.getProperty("cloudOraclePassword", "TestPassword123");
          CloudOracleResourceManager.Builder builder =
              CloudOracleResourceManager.builder("cdb_admin");
          builder.setUsername("sys as sysdba");
          builder.setPassword(password);
          builder.setDatabaseName("XE");
          builder.setHost(host);
          builder.setPort(1521);
          cdbAdmin = new SpannerOracleResourceManager(builder);

          Runtime.getRuntime()
              .addShutdownHook(
                  new Thread(
                      () -> {
                        try {
                          cdbAdmin.cleanupAll();
                        } catch (Exception e) {
                        }
                      }));
        }
      }
    }
    return cdbAdmin;
  }

  public static SpannerOracleResourceManager getInstance() {
    if (pdbAdmin == null) {
      synchronized (lock) {
        if (pdbAdmin == null) {
          LOG.info("Initializing global Singleton PDB Admin Oracle pool (XEPDB1).");
          String host = System.getProperty("cloudOracleHost", "localhost");
          String password = System.getProperty("cloudOraclePassword", "TestPassword123");
          CloudOracleResourceManager.Builder builder =
              CloudOracleResourceManager.builder("pdb_admin");
          builder.setUsername("sys as sysdba");
          builder.setPassword(password);
          builder.setDatabaseName("XEPDB1");
          builder.setHost(host);
          builder.setPort(1521);
          pdbAdmin = new SpannerOracleResourceManager(builder);

          Runtime.getRuntime()
              .addShutdownHook(
                  new Thread(
                      () -> {
                        try {
                          pdbAdmin.cleanupAll();
                        } catch (Exception e) {
                        }
                      }));
        }
      }
    }
    return pdbAdmin;
  }

  public static void flushRedoLogs() {
    LOG.info("Flushing REDO logs via CDB Admin...");
    // getCdbAdmin().runSQLUpdate("ALTER SYSTEM SWITCH LOGFILE");
    String url = "jdbc:oracle:thin:@//" + System.getProperty("cloudOracleHost") + ":1521/XE";
    String user = System.getProperty("cloudOracleUsername", "system");
    String pass = System.getProperty("cloudOraclePassword", "Test@Password123");
    try (java.sql.Connection conn = java.sql.DriverManager.getConnection(url, user, pass);
        java.sql.Statement stmt = conn.createStatement()) {
      stmt.execute("ALTER SYSTEM SWITCH LOGFILE");
      LOG.info("Successfully flushed Oracle redo logs via raw JDBC fallback.");
    } catch (Exception ex) {
      LOG.error("Raw JDBC fallback log flush also failed.", ex);
    }
  }

  public static synchronized void dropUser(String user) {
    // Pending implementation
  }

  public static synchronized String setupOracleIsolatedUser() {
    SpannerOracleResourceManager cdbAdmin = getCdbAdmin();
    String testUsername =
        "C##LIVE_"
            + java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 8).toUpperCase();
    LOG.info(
        "Creating isolated Oracle COMMON user (for Datastream CDC) via CDB Admin: {}",
        testUsername);
    cdbAdmin.runSQLUpdate(
        "CREATE USER " + testUsername + " IDENTIFIED BY " + ORACLE_PASSWORD + " CONTAINER=ALL");
    cdbAdmin.runSQLUpdate("GRANT DBA TO " + testUsername + " CONTAINER=ALL");
    cdbAdmin.runSQLUpdate("GRANT SET CONTAINER TO " + testUsername + " CONTAINER=ALL");
    cdbAdmin.runSQLUpdate("GRANT EXECUTE ON SYS.DBMS_LOGMNR TO " + testUsername + " CONTAINER=ALL");
    cdbAdmin.runSQLUpdate("GRANT LOGMINING TO " + testUsername + " CONTAINER=ALL");
    cdbAdmin.runSQLUpdate("GRANT SELECT ANY TRANSACTION TO " + testUsername + " CONTAINER=ALL");
    cdbAdmin.runSQLUpdate("GRANT SELECT_CATALOG_ROLE TO " + testUsername + " CONTAINER=ALL");
    cdbAdmin.runSQLUpdate("GRANT EXECUTE_CATALOG_ROLE TO " + testUsername + " CONTAINER=ALL");
    cdbAdmin.runSQLUpdate("GRANT SELECT ON V_$DATABASE TO " + testUsername + " CONTAINER=ALL");
    cdbAdmin.runSQLUpdate(
        "GRANT SELECT ON V_$LOGMNR_CONTENTS TO " + testUsername + " CONTAINER=ALL");
    cdbAdmin.runSQLUpdate("GRANT SELECT ON V_$ARCHIVED_LOG TO " + testUsername + " CONTAINER=ALL");
    cdbAdmin.runSQLUpdate("GRANT SELECT ON V_$LOGFILE TO " + testUsername + " CONTAINER=ALL");
    cdbAdmin.runSQLUpdate("GRANT SELECT ON V_$LOG TO " + testUsername + " CONTAINER=ALL");
    cdbAdmin.runSQLUpdate(
        "GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO " + testUsername + " CONTAINER=ALL");
    cdbAdmin.runSQLUpdate("GRANT SELECT ON V_$TRANSACTION TO " + testUsername + " CONTAINER=ALL");
    cdbAdmin.runSQLUpdate("ALTER USER " + testUsername + " QUOTA 50m ON SYSTEM CONTAINER=ALL");
    return testUsername;
  }
}
