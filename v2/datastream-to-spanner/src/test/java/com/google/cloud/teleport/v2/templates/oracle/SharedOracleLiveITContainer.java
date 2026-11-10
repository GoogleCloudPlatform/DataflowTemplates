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

import com.google.cloud.teleport.v2.spanner.oracleutils.OracleITContainer;
import com.google.cloud.teleport.v2.spanner.oracleutils.SpannerOracleResourceManager;
import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.it.common.ResourceManager;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.datastream.OracleSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared Oracle database used by the Datastream (Oracle) to Spanner live migration integration
 * tests.
 *
 * <p>A single Oracle Testcontainer, configured for change data capture (ARCHIVELOG, FORCE LOGGING,
 * MIN/PK/ALL supplemental logging, ...) by {@link OracleITContainer}, is started lazily on first
 * use and shared by all the ITs of the test JVM. Each IT gets its own isolated Oracle common user
 * (schema) via {@link #setupOracleIsolatedUser()}, which must be dropped at the end of the IT via
 * {@link #dropUser(String)}, after the Datastream stream reading from it has been deleted.
 */
public final class SharedOracleLiveITContainer {

  private static final Logger LOG = LoggerFactory.getLogger(SharedOracleLiveITContainer.class);

  /**
   * Password for the isolated per-test Oracle users (see {@link #setupOracleIsolatedUser()}). These
   * users are created and dropped by the tests, so a random password is generated once per JVM
   * instead of hardcoding one. It starts with a letter as required for unquoted Oracle passwords.
   */
  public static final String ORACLE_PASSWORD =
      "Tp" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);

  private static final String ISOLATED_USER_PREFIX = "C##LIVE_";

  /**
   * Only users matching this pattern (i.e. created by {@link #setupOracleIsolatedUser()}) can be
   * dropped, to guard against accidentally dropping another user.
   */
  private static final Pattern ISOLATED_USER_PATTERN =
      Pattern.compile("^" + Pattern.quote(ISOLATED_USER_PREFIX) + "[A-Z0-9]{8}$");

  private static final int DROP_USER_MAX_ATTEMPTS = 5;
  private static final long DROP_USER_RETRY_DELAY_MILLIS = 5_000;

  private static final Object LOCK = new Object();

  private static OracleITContainer container;
  private static SpannerOracleResourceManager cdbAdmin;
  private static SpannerOracleResourceManager pdbAdmin;

  private SharedOracleLiveITContainer() {}

  /** Returns the shared Oracle container, starting it on first use. */
  private static OracleITContainer getContainer() {
    synchronized (LOCK) {
      if (container == null) {
        LOG.info("Starting the shared Oracle container for the live ITs.");
        container = OracleITContainer.start(/* enableCdc= */ true);
      }
      return container;
    }
  }

  private static SpannerOracleResourceManager getCdbAdmin() {
    synchronized (LOCK) {
      if (cdbAdmin == null) {
        LOG.info("Initializing global Singleton CDB Admin Oracle pool (XE).");
        cdbAdmin =
            getContainer()
                .createAdminResourceManager("cdb_admin", OracleITContainer.CDB_SERVICE_NAME);
      }
      return cdbAdmin;
    }
  }

  /** Returns the admin resource manager connected to the pluggable database used by the tests. */
  public static SpannerOracleResourceManager getInstance() {
    synchronized (LOCK) {
      if (pdbAdmin == null) {
        LOG.info("Initializing global Singleton PDB Admin Oracle pool (XEPDB1).");
        pdbAdmin =
            getContainer()
                .createAdminResourceManager("pdb_admin", OracleITContainer.PDB_SERVICE_NAME);
      }
      return pdbAdmin;
    }
  }

  /**
   * Returns an {@link OracleSource} builder connecting Datastream to the pluggable database of the
   * shared container as the given isolated user. Callers still set the allowed tables and build it.
   *
   * @param username the isolated user, as returned by {@link #setupOracleIsolatedUser()}.
   */
  public static OracleSource.Builder oracleSourceBuilder(String username) {
    SpannerOracleResourceManager pdb = getInstance();
    return OracleSource.builder(
        pdb.getHost(), username, ORACLE_PASSWORD, pdb.getPort(), pdb.getDatabaseName());
  }

  /** Forces a redo log switch, so that Datastream picks up the latest changes sooner. */
  public static void flushRedoLogs() {
    LOG.info("Flushing REDO logs via CDB Admin...");
    OracleITContainer oracle = getContainer();
    try (Connection conn =
            DriverManager.getConnection(
                oracle.getJdbcUrl(OracleITContainer.CDB_SERVICE_NAME),
                OracleITContainer.SYSTEM_USERNAME,
                oracle.getPassword());
        Statement stmt = conn.createStatement()) {
      stmt.execute("ALTER SYSTEM SWITCH LOGFILE");
      LOG.info("Successfully flushed Oracle redo logs.");
    } catch (Exception ex) {
      LOG.error("Failed to flush Oracle redo logs.", ex);
    }
  }

  /**
   * Creates a new uniquely named Oracle common user (and therefore schema) for a single IT, with
   * the privileges needed by Datastream CDC, identified by {@link #ORACLE_PASSWORD}.
   *
   * @return the name of the created user.
   */
  public static synchronized String setupOracleIsolatedUser() {
    SpannerOracleResourceManager cdbAdmin = getCdbAdmin();
    String testUsername =
        ISOLATED_USER_PREFIX
            + UUID.randomUUID().toString().replace("-", "").substring(0, 8).toUpperCase();
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

  /**
   * Drops an isolated user created by {@link #setupOracleIsolatedUser()}, killing its sessions
   * first. Retries a few times, as sessions (e.g. of a Datastream stream being deleted) can still
   * be connected (ORA-01940). Failures are logged and not rethrown, so that they don't fail the
   * test cleanup.
   *
   * @param user the isolated user, as returned by {@link #setupOracleIsolatedUser()}.
   */
  public static void dropUser(String user) {
    if (user == null || user.trim().isEmpty()) {
      return;
    }
    if (!ISOLATED_USER_PATTERN.matcher(user).matches()) {
      LOG.error("Refusing to drop Oracle user {} as it is not an isolated test user.", user);
      return;
    }
    SpannerOracleResourceManager cdbAdmin = getCdbAdmin();
    for (int attempt = 1; attempt <= DROP_USER_MAX_ATTEMPTS; attempt++) {
      try {
        LOG.info("Killing active sessions for user {} before dropping", user);
        killSessions(cdbAdmin, user);
        LOG.info("Dropping isolated Oracle COMMON user via CDB Admin: {}", user);
        cdbAdmin.runSQLUpdate("DROP USER " + user + " CASCADE");
        return;
      } catch (Exception e) {
        LOG.warn(
            "Attempt {}/{} to drop isolated Oracle user {} failed.",
            attempt,
            DROP_USER_MAX_ATTEMPTS,
            user,
            e);
      }
      if (attempt < DROP_USER_MAX_ATTEMPTS) {
        try {
          Thread.sleep(DROP_USER_RETRY_DELAY_MILLIS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOG.warn("Interrupted while dropping isolated Oracle user {}.", user);
          return;
        }
      }
    }
    LOG.error(
        "Failed to drop isolated Oracle user {} after {} attempts.", user, DROP_USER_MAX_ATTEMPTS);
  }

  private static void killSessions(SpannerOracleResourceManager cdbAdmin, String user) {
    try {
      cdbAdmin.runSQLUpdate(
          "BEGIN "
              + "FOR s IN (SELECT sid, serial# FROM v$session WHERE username = '"
              + user
              + "') LOOP "
              + "  EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION ''' || s.sid || ',' || s.serial# || ''' IMMEDIATE'; "
              + "END LOOP; "
              + "END;");
    } catch (Exception e) {
      LOG.warn("Could not kill sessions for user {}, ignoring: {}", user, e.getMessage());
    }
  }

  /**
   * Cleans up the resources of an IT, in an order that does not leave anything reading from a
   * dropped Oracle user: cancels the Dataflow jobs of the given test instances first, then cleans
   * up the resource managers (including the Datastream streams reading from Oracle) and finally
   * drops the isolated Oracle users. The Oracle users are dropped even if an earlier step fails.
   *
   * @param testInstances the test instances whose Dataflow jobs are cancelled.
   * @param oracleUsers the isolated users created by {@link #setupOracleIsolatedUser()}. Null
   *     entries (users that were not created) are ignored.
   * @param resourceManagers the resource managers to clean up. Null entries (resource managers that
   *     were not created) are ignored.
   * @throws RuntimeException if the resource managers could not be cleaned up.
   */
  public static void cleanUpTestResources(
      Collection<? extends TemplateTestBase> testInstances,
      Collection<String> oracleUsers,
      ResourceManager... resourceManagers) {
    try {
      for (TemplateTestBase instance : testInstances) {
        try {
          instance.tearDownBase();
        } catch (Exception e) {
          LOG.error("Failed to clean up the Dataflow jobs of a test instance.", e);
        }
      }
      ResourceManagerUtils.cleanResources(resourceManagers);
    } finally {
      for (String user : oracleUsers) {
        dropUser(user);
      }
    }
  }

  /**
   * Executes the {@code ;} separated SQL statements of a classpath resource in the schema of the
   * given user, as the PDB admin.
   *
   * <p>The script is parsed naively: {@code --} line comments are stripped and the remaining lines
   * are joined and split on {@code ;}. Hence a {@code ;} must not appear inside a string literal
   * and PL/SQL blocks are not supported.
   *
   * @param targetUsername the user whose schema is used as the current schema.
   * @param resourceName classpath resource containing {@code ;} separated SQL statements.
   */
  public static void executeSqlScript(String targetUsername, String resourceName)
      throws IOException, SQLException {
    String sql =
        Resources.readLines(Resources.getResource(resourceName), StandardCharsets.UTF_8).stream()
            .map(SharedOracleLiveITContainer::stripLineComment)
            .collect(Collectors.joining(" "));
    executeSql(targetUsername, sql);
  }

  /** Removes a {@code --} comment (outside of a string literal) from a line of SQL. */
  private static String stripLineComment(String line) {
    boolean inLiteral = false;
    for (int i = 0; i < line.length() - 1; i++) {
      char c = line.charAt(i);
      if (c == '\'') {
        inLiteral = !inLiteral;
      } else if (!inLiteral && c == '-' && line.charAt(i + 1) == '-') {
        return line.substring(0, i);
      }
    }
    return line;
  }

  /**
   * Executes {@code ;} separated SQL statements in the schema of the given user, as the PDB admin.
   *
   * @param targetUsername the user whose schema is used as the current schema.
   * @param sqlString one or more {@code ;} separated SQL statements.
   */
  public static void executeSql(String targetUsername, String sqlString) throws SQLException {
    String sql = sqlString.replaceAll("\r\n", " ").replaceAll("\n", " ").trim();
    String[] statements = sql.split(";");
    SpannerOracleResourceManager pdb = getInstance();
    try (Connection connection =
        DriverManager.getConnection(pdb.getUri(), pdb.getUsername(), pdb.getPassword())) {
      if (!"SYSTEM".equalsIgnoreCase(targetUsername)) {
        try (Statement stmt = connection.createStatement()) {
          stmt.execute("ALTER SESSION SET CURRENT_SCHEMA = \"" + targetUsername + "\"");
        }
      }
      try (Statement statement = connection.createStatement()) {
        for (String stmt : statements) {
          if (!stmt.trim().isBlank()) {
            statement.execute(stmt);
          }
        }
      }
    }
  }
}
