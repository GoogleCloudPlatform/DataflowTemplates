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

import com.google.cloud.teleport.v2.spanner.resourcemanager.OracleITContainer;
import com.google.cloud.teleport.v2.spanner.resourcemanager.SpannerOracleResourceManager;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared Oracle database used by the Datastream (Oracle) to Spanner live migration integration
 * tests.
 *
 * <p>A single Oracle Testcontainer, configured for change data capture (ARCHIVELOG, supplemental
 * logging, ...) by {@link OracleITContainer}, is started lazily on first use and shared by all the
 * ITs of the test JVM. Each IT gets its own isolated Oracle common user (schema) via {@link
 * #setupOracleIsolatedUser()}, which must be dropped at the end of the IT via {@link
 * #dropUser(String)}.
 */
public class SharedOracleLiveITInstance {
  private static final Logger LOG = LoggerFactory.getLogger(SharedOracleLiveITInstance.class);
  private static OracleITContainer container;
  private static SpannerOracleResourceManager cdbAdmin;
  private static SpannerOracleResourceManager pdbAdmin;

  /**
   * Password for the isolated per-test Oracle users (see {@link #setupOracleIsolatedUser()}). These
   * users are created and dropped by the tests, so a random password is generated once per JVM
   * instead of hardcoding one. It starts with a letter as required for unquoted Oracle passwords.
   */
  public static final String ORACLE_PASSWORD =
      "Tp" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);

  private static final Object lock = new Object();

  /** Returns the shared Oracle container, starting it on first use. */
  private static OracleITContainer getContainer() {
    synchronized (lock) {
      if (container == null) {
        LOG.info("Starting the shared Oracle container for the live ITs.");
        container = OracleITContainer.start(/* enableCdc= */ true);
      }
      return container;
    }
  }

  private static SpannerOracleResourceManager getCdbAdmin() {
    synchronized (lock) {
      if (cdbAdmin == null) {
        LOG.info("Initializing global Singleton CDB Admin Oracle pool (XE).");
        cdbAdmin =
            getContainer()
                .createAdminResourceManager("cdb_admin", OracleITContainer.CDB_SERVICE_NAME);
      }
      return cdbAdmin;
    }
  }

  public static SpannerOracleResourceManager getInstance() {
    synchronized (lock) {
      if (pdbAdmin == null) {
        LOG.info("Initializing global Singleton PDB Admin Oracle pool (XEPDB1).");
        pdbAdmin =
            getContainer()
                .createAdminResourceManager("pdb_admin", OracleITContainer.PDB_SERVICE_NAME);
      }
      return pdbAdmin;
    }
  }

  public static void flushRedoLogs() {
    LOG.info("Flushing REDO logs via CDB Admin...");
    // getCdbAdmin().runSQLUpdate("ALTER SYSTEM SWITCH LOGFILE");
    OracleITContainer oracle = getContainer();
    String url = oracle.getJdbcUrl(OracleITContainer.CDB_SERVICE_NAME);
    String user = OracleITContainer.SYSTEM_USERNAME;
    String pass = oracle.getPassword();
    try (java.sql.Connection conn = java.sql.DriverManager.getConnection(url, user, pass);
        java.sql.Statement stmt = conn.createStatement()) {
      stmt.execute("ALTER SYSTEM SWITCH LOGFILE");
      LOG.info("Successfully flushed Oracle redo logs via raw JDBC fallback.");
    } catch (Exception ex) {
      LOG.error("Raw JDBC fallback log flush also failed.", ex);
    }
  }

  public static synchronized void dropUser(String user) {
    if (user != null && !user.trim().isEmpty()) {
      try {
        SpannerOracleResourceManager cdbAdmin = getCdbAdmin();
        LOG.info("Killing active sessions for user {} before dropping", user);
        try {
          cdbAdmin.runSQLUpdate(
              "BEGIN "
                  + "FOR s IN (SELECT sid, serial# FROM v$session WHERE username = '"
                  + user
                  + "') LOOP "
                  + "  EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION ''' || s.sid || ',' || s.serial# || ''' IMMEDIATE'; "
                  + "END LOOP; "
                  + "END;");
        } catch (Exception killEx) {
          LOG.warn("Could not kill sessions for user {}, ignoring: {}", user, killEx.getMessage());
        }
        LOG.info("Dropping isolated Oracle COMMON user via CDB Admin: {}", user);
        cdbAdmin.runSQLUpdate("DROP USER " + user + " CASCADE");
      } catch (Exception e) {
        LOG.warn("Failed to drop user {} during test cleanup: {}", user, e.getStackTrace());
      }
    }
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
    cdbAdmin.runSQLUpdate("GRANT LOGMINING TO " + testUsername + " CONTAINER=ALL");
    cdbAdmin.runSQLUpdate("GRANT SELECT ANY TRANSACTION TO " + testUsername + " CONTAINER=ALL");
    // Access to the V$ views (V$DATABASE, V$LOG, V$LOGMNR_CONTENTS, ...) and DBMS_LOGMNR used by
    // Datastream comes from SELECT_CATALOG_ROLE / EXECUTE_CATALOG_ROLE (and DBA). Do not grant
    // object privileges on those SYS objects per test: an object GRANT is DDL that needs an
    // exclusive library cache lock on the object, and it times out (ORA-04021) while the
    // Datastream streams of concurrently running tests keep querying those views.
    cdbAdmin.runSQLUpdate("GRANT SELECT_CATALOG_ROLE TO " + testUsername + " CONTAINER=ALL");
    cdbAdmin.runSQLUpdate("GRANT EXECUTE_CATALOG_ROLE TO " + testUsername + " CONTAINER=ALL");
    cdbAdmin.runSQLUpdate("ALTER USER " + testUsername + " QUOTA 50m ON SYSTEM CONTAINER=ALL");
    return testUsername;
  }
}
