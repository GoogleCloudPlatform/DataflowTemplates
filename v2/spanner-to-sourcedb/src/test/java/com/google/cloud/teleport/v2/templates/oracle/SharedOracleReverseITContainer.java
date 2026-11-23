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
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared Oracle database used by the Spanner to Oracle (reverse replication) integration tests.
 *
 * <p>A single Oracle Testcontainer ({@link OracleITContainer}) is started lazily on first use and
 * shared by all the ITs of the test JVM, through a singleton admin connection. Each IT gets its own
 * isolated Oracle user (schema) via {@link #createIsolatedUser()}, which must be dropped at the end
 * of the IT via {@link #dropIsolatedUser(String)}, after the pipeline writing to it has been
 * cancelled.
 */
public class SharedOracleReverseITContainer {
  private static final Logger LOG = LoggerFactory.getLogger(SharedOracleReverseITContainer.class);
  private static final Object lock = new Object();

  /** Password used for every isolated user created by {@link #createIsolatedUser()}. */
  public static final String ISOLATED_USER_PASSWORD = "TestPassword123";

  private static final String ISOLATED_USER_PREFIX = "C##REV_";

  /**
   * Only users matching this pattern (i.e. created by {@link #createIsolatedUser()}) can be
   * dropped, to guard against accidentally dropping a real user.
   */
  private static final Pattern ISOLATED_USER_PATTERN =
      Pattern.compile("^" + Pattern.quote(ISOLATED_USER_PREFIX) + "[A-Z0-9]{8}$");

  private static final int DROP_USER_MAX_ATTEMPTS = 5;
  private static final long DROP_USER_RETRY_DELAY_MILLIS = 5_000;

  private static OracleITContainer container;
  private static SpannerOracleResourceManager instance;
  private static SpannerOracleResourceManager cdbAdmin;

  /** Returns the shared Oracle container, starting it on first use. */
  private static OracleITContainer getContainer() {
    synchronized (lock) {
      if (container == null) {
        LOG.info("Starting the shared Oracle container for the reverse replication ITs.");
        // Reverse replication only writes to Oracle, so no CDC setup (ARCHIVELOG, ...) is needed.
        container = OracleITContainer.start(/* enableCdc= */ false);
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
      if (instance == null) {
        LOG.info("Initializing global Singleton PDB Admin Oracle pool (XEPDB1).");
        instance =
            getContainer()
                .createAdminResourceManager("oracle_shared", OracleITContainer.PDB_SERVICE_NAME);
      }
      return instance;
    }
  }

  /**
   * Creates a new uniquely named Oracle user (and therefore schema) for a single IT, identified by
   * {@link #ISOLATED_USER_PASSWORD}.
   *
   * @return the name of the created user.
   */
  public static String createIsolatedUser() {
    String username =
        ISOLATED_USER_PREFIX
            + UUID.randomUUID().toString().replace("-", "").substring(0, 8).toUpperCase();
    LOG.info("Creating isolated Oracle user: {}", username);
    SpannerOracleResourceManager rm = getCdbAdmin();
    // The user is created from CDB$ROOT, but the tests and the pipeline connect to XEPDB1. Grants
    // without CONTAINER=ALL only apply to the current container (the root), so the user would
    // lack even CREATE SESSION in XEPDB1 (ORA-01045).
    rm.runSQLUpdate(
        "CREATE USER "
            + username
            + " IDENTIFIED BY \""
            + ISOLATED_USER_PASSWORD
            + "\" CONTAINER=ALL");
    rm.runSQLUpdate("GRANT ALL PRIVILEGES TO " + username + " CONTAINER=ALL");
    rm.runSQLUpdate("GRANT UNLIMITED TABLESPACE TO " + username + " CONTAINER=ALL");
    rm.runSQLUpdate("GRANT DBA TO " + username + " CONTAINER=ALL");
    return username;
  }

  /**
   * Drops an isolated user created by {@link #createIsolatedUser()} along with all its objects.
   *
   * <p>Should be called after the pipeline writing to this user has been cancelled. Any remaining
   * sessions of the user are killed before dropping it, and the drop is retried a few times. This
   * method never throws, so that cleanup failures don't fail the test; failures are logged instead.
   *
   * @param username the user to drop. {@code null} is ignored.
   */
  public static void dropIsolatedUser(String username) {
    if (username == null) {
      return;
    }
    if (!ISOLATED_USER_PATTERN.matcher(username).matches()) {
      LOG.error("Refusing to drop Oracle user {} as it is not an isolated test user.", username);
      return;
    }
    SpannerOracleResourceManager rm = getCdbAdmin();
    for (int attempt = 1; attempt <= DROP_USER_MAX_ATTEMPTS; attempt++) {
      try {
        killSessions(rm, username);
        rm.runSQLUpdate("DROP USER " + username + " CASCADE");
        LOG.info("Dropped isolated Oracle user: {}", username);
        return;
      } catch (Exception e) {
        LOG.warn(
            "Attempt {}/{} to drop isolated Oracle user {} failed.",
            attempt,
            DROP_USER_MAX_ATTEMPTS,
            username,
            e);
      }
      if (attempt < DROP_USER_MAX_ATTEMPTS) {
        try {
          Thread.sleep(DROP_USER_RETRY_DELAY_MILLIS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    LOG.error("Could not drop isolated Oracle user {}. It needs to be dropped manually.", username);
  }

  /** Kills all sessions of the given user, so that the user can be dropped. */
  private static void killSessions(SpannerOracleResourceManager rm, String username) {
    List<Map<String, Object>> sessions =
        rm.runSQLQuery(
            "SELECT sid || ',' || serial# AS session_id FROM v$session WHERE username = '"
                + username
                + "'");
    for (Map<String, Object> session : sessions) {
      // Single column result; read it by position to not depend on the returned column name case.
      Object sessionId = session.values().iterator().next();
      try {
        rm.runSQLUpdate("ALTER SYSTEM KILL SESSION '" + sessionId + "' IMMEDIATE");
      } catch (Exception e) {
        // The session may have ended on its own in the meantime.
        LOG.warn("Failed to kill session {} of Oracle user {}.", sessionId, username, e);
      }
    }
  }

  /**
   * Creates the tables of the given DDL resource file in the schema of the given isolated user.
   *
   * @param username the isolated user, as returned by {@link #createIsolatedUser()}.
   * @param schemaResource classpath resource containing {@code ;} separated DDL statements.
   */
  public static void createSchema(String username, String schemaResource) throws IOException {
    String ddl =
        String.join(
            " ",
            Resources.readLines(Resources.getResource(schemaResource), StandardCharsets.UTF_8));
    ddl = ddl.replaceAll("\r\n", " ").replaceAll("\n", " ");
    String[] ddls = ddl.split(";");
    try (Connection connection =
            DriverManager.getConnection(getInstance().getUri(), username, ISOLATED_USER_PASSWORD);
        Statement stmt = connection.createStatement()) {
      if (!"SYSTEM".equalsIgnoreCase(username)) {
        stmt.execute("ALTER SESSION SET CURRENT_SCHEMA = " + username);
      }
      for (String d : ddls) {
        if (!d.trim().isEmpty() && !d.trim().toUpperCase().startsWith("SELECT")) {
          try {
            stmt.executeUpdate(d);
          } catch (Exception e) {
            throw new RuntimeException("Failed to execute schema DDL: " + d, e);
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("failed creating isolated oracle schema", e);
    }
  }

  /** Returns the number of rows of a table in the schema of the given isolated user. */
  public static long getRowCount(String username, String tableName) {
    return getInstance().getRowCount(username + "." + tableName);
  }

  /** Reads all rows of a table in the schema of the given isolated user. */
  public static List<Map<String, Object>> readTable(String username, String tableName) {
    return getInstance().readTable(username + "." + tableName);
  }

  /**
   * Runs a query as the given isolated user, with the user's schema as the current schema. CLOB and
   * BLOB values are materialized as {@link String} and {@code byte[]} respectively, and column
   * names are lower-cased.
   */
  public static List<Map<String, Object>> runQuery(String username, String query) {
    try (Connection connection =
            DriverManager.getConnection(getInstance().getUri(), username, ISOLATED_USER_PASSWORD);
        Statement stmt = connection.createStatement()) {
      if (!"SYSTEM".equalsIgnoreCase(username)) {
        stmt.execute("ALTER SESSION SET CURRENT_SCHEMA = " + username);
      }
      List<Map<String, Object>> result = new ArrayList<>();
      try (ResultSet rs = stmt.executeQuery(query)) {
        ResultSetMetaData md = rs.getMetaData();
        int columns = md.getColumnCount();
        while (rs.next()) {
          Map<String, Object> row = new HashMap<>(columns);
          for (int i = 1; i <= columns; ++i) {
            Object obj = rs.getObject(i);
            if (obj instanceof Clob) {
              row.put(
                  md.getColumnName(i).toLowerCase(),
                  ((Clob) obj).getSubString(1, (int) ((Clob) obj).length()));
            } else if (obj instanceof Blob) {
              row.put(
                  md.getColumnName(i).toLowerCase(),
                  ((Blob) obj).getBytes(1, (int) ((Blob) obj).length()));
            } else {
              row.put(md.getColumnName(i).toLowerCase(), obj);
            }
          }
          result.add(row);
        }
      }
      return result;
    } catch (Exception e) {
      throw new RuntimeException("Error running isolated query", e);
    }
  }
}
