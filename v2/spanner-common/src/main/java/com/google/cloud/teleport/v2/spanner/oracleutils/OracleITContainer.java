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
package com.google.cloud.teleport.v2.spanner.oracleutils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * An Oracle XE (multitenant: CDB {@code XE}, PDB {@code XEPDB1}) Testcontainer meant to be started
 * once per test JVM and shared by all the Oracle ITs of a template module.
 *
 * <p>After the container starts, a one-time setup script is run inside it (CPU and memory are left
 * at what the XE container offers):
 *
 * <ul>
 *   <li>{@code processes} / {@code sessions}: default 4000 / 6000, overridable with {@code
 *       -DoracleProcesses} / {@code -DoracleSessions}. The static instance used 20000 / 30000, but
 *       XE caps the SGA at 1.5G and Oracle reserves SGA per allowed process/session: above ~5000
 *       processes the instance fails to start with ORA-00821.
 *   <li>Only when CDC is enabled (needed by Datastream): ARCHIVELOG mode (archived logs in Oracle's
 *       default location, without a quota), FORCE LOGGING and MIN/PK/ALL supplemental logging (as
 *       on the static instance), and 3 extra 100 MB redo log groups.
 * </ul>
 *
 * <p>The container is published on {@link TestProperties#hostIp()} (the IP of the machine running
 * the tests, passed as {@code -DhostIp}) and its mapped port, so that it is reachable by Dataflow
 * workers and Datastream, and not only from the test JVM. It is removed by Testcontainers when the
 * JVM exits.
 */
public final class OracleITContainer {

  private static final Logger LOG = LoggerFactory.getLogger(OracleITContainer.class);

  /** Service name of the container database (CDB$ROOT). */
  public static final String CDB_SERVICE_NAME = "XE";

  /** Service name of the pluggable database used by the tests. */
  public static final String PDB_SERVICE_NAME = "XEPDB1";

  /** Admin user used by the ITs, same as for the static Oracle instance. */
  public static final String ADMIN_USERNAME = "sys as sysdba";

  /** Non-SYSDBA admin user, whose password is also {@link #getPassword()}. */
  public static final String SYSTEM_USERNAME = "system";

  /** System property to override the container image, e.g. to use a mirror. */
  private static final String IMAGE_PROPERTY = "oracleContainerImage";

  /** System properties to override the {@code processes} / {@code sessions} parameters. */
  private static final String PROCESSES_PROPERTY = "oracleProcesses";

  private static final String SESSIONS_PROPERTY = "oracleSessions";

  /**
   * Defaults for {@code processes} / {@code sessions}. XE caps the SGA at 1536M; the instance fails
   * to start (ORA-00821) above ~5000 processes, so leave some SGA headroom for the shared pool and
   * buffer cache.
   */
  private static final int DEFAULT_PROCESSES = 4000;

  private static final int DEFAULT_SESSIONS = 6000;

  private static final String DEFAULT_IMAGE = "gvenzl/oracle-xe:21-slim-faststart";
  private static final int ORACLE_PORT = 1521;
  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(10);
  private static final Duration READY_TIMEOUT = Duration.ofMinutes(10);
  private static final Duration READY_POLL_INTERVAL = Duration.ofSeconds(5);

  private final OracleContainer container;
  private final String password;

  private OracleITContainer(OracleContainer container, String password) {
    this.container = container;
    this.password = password;
  }

  /**
   * Starts a new Oracle container and configures it.
   *
   * @param enableCdc whether to also configure the database for change data capture with Datastream
   *     (ARCHIVELOG, FORCE LOGGING, supplemental logging, redo log size).
   */
  public static OracleITContainer start(boolean enableCdc) {
    String image = System.getProperty(IMAGE_PROPERTY, DEFAULT_IMAGE);
    // Also the SYS and SYSTEM password. Starts with a letter as required for unquoted passwords.
    String password = "Or" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
    LOG.info("Starting Oracle container {} (CDC enabled: {}).", image, enableCdc);
    OracleContainer container =
        new OracleContainer(
                DockerImageName.parse(image).asCompatibleSubstituteFor("gvenzl/oracle-xe"))
            .withPassword(password)
            .withStartupTimeout(STARTUP_TIMEOUT);
    container.start();

    OracleITContainer oracle = new OracleITContainer(container, password);
    try {
      oracle.runSetupScript(enableCdc);
      oracle.waitUntilReady();
    } catch (RuntimeException e) {
      container.stop();
      throw e;
    }
    LOG.info(
        "Oracle container ready at {}:{} (CDB {}, PDB {}).",
        oracle.getHost(),
        oracle.getPort(),
        CDB_SERVICE_NAME,
        PDB_SERVICE_NAME);
    return oracle;
  }

  /** Host on which the container is reachable by the tests, Dataflow workers and Datastream. */
  public String getHost() {
    return TestProperties.hostIp();
  }

  /** Port on which the container's listener is reachable, on {@link #getHost()}. */
  public int getPort() {
    return container.getMappedPort(ORACLE_PORT);
  }

  /** Password of the SYS and SYSTEM users. */
  public String getPassword() {
    return password;
  }

  /** JDBC URL to the given service, e.g. {@link #CDB_SERVICE_NAME} or {@link #PDB_SERVICE_NAME}. */
  public String getJdbcUrl(String serviceName) {
    return String.format("jdbc:oracle:thin:@//%s:%d/%s", getHost(), getPort(), serviceName);
  }

  /**
   * Creates a resource manager connected to the given service as {@link #ADMIN_USERNAME}.
   *
   * @param testId id of the resource manager.
   * @param serviceName {@link #CDB_SERVICE_NAME} or {@link #PDB_SERVICE_NAME}.
   */
  public SpannerOracleResourceManager createAdminResourceManager(
      String testId, String serviceName) {
    CloudOracleResourceManager.Builder builder = CloudOracleResourceManager.builder(testId);
    builder.setUsername(ADMIN_USERNAME);
    builder.setPassword(password);
    builder.setDatabaseName(serviceName);
    builder.setHost(getHost());
    builder.setPort(getPort());
    return new SpannerOracleResourceManager(builder);
  }

  private void runSetupScript(boolean enableCdc) {
    ExecResult result;
    try {
      result =
          container.execInContainer(
              "bash",
              "-c",
              buildSetupScript(
                  enableCdc,
                  Integer.getInteger(PROCESSES_PROPERTY, DEFAULT_PROCESSES),
                  Integer.getInteger(SESSIONS_PROPERTY, DEFAULT_SESSIONS)));
    } catch (IOException e) {
      throw new IllegalStateException("Failed to run the Oracle container setup script", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while setting up the Oracle container", e);
    }
    LOG.info("Oracle container setup script output:\n{}", result.getStdout());
    if (result.getExitCode() != 0) {
      throw new IllegalStateException(
          String.format(
              "Oracle container setup script failed with exit code %d.%nstdout:%n%s%n"
                  + "stderr:%n%s",
              result.getExitCode(), result.getStdout(), result.getStderr()));
    }
  }

  /**
   * Waits until both services accept connections through {@link #getHost()}, as the listener
   * re-registers them after the database restart done by the setup script.
   */
  private void waitUntilReady() {
    Instant deadline = Instant.now().plus(READY_TIMEOUT);
    Exception lastFailure = null;
    while (Instant.now().isBefore(deadline)) {
      try {
        checkConnection(CDB_SERVICE_NAME);
        checkConnection(PDB_SERVICE_NAME);
        return;
      } catch (Exception e) {
        lastFailure = e;
        LOG.info("Oracle container not ready yet: {}", e.getMessage());
      }
      try {
        Thread.sleep(READY_POLL_INTERVAL.toMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while waiting for the Oracle container", e);
      }
    }
    throw new IllegalStateException(
        "Oracle container not reachable on " + getHost() + ":" + getPort(), lastFailure);
  }

  private void checkConnection(String serviceName) throws Exception {
    try (Connection connection =
            DriverManager.getConnection(getJdbcUrl(serviceName), SYSTEM_USERNAME, password);
        Statement statement = connection.createStatement()) {
      statement.execute("SELECT 1 FROM DUAL");
    }
  }

  /**
   * Builds the bash script run inside the container (as the {@code oracle} OS user) to configure
   * the database. Any SQL or OS error aborts the script with a non-zero exit code.
   */
  static String buildSetupScript(boolean enableCdc, int processes, int sessions) {
    StringBuilder sql = new StringBuilder();
    sql.append(
        """
        WHENEVER SQLERROR EXIT FAILURE
        WHENEVER OSERROR EXIT FAILURE
        SET FEEDBACK OFF LINESIZE 200 PAGESIZE 100
        COLUMN name FORMAT A28
        COLUMN value FORMAT A60
        COLUMN member FORMAT A70

        PROMPT ===== Image defaults (recovery area / archive destination / redo logs) =====
        SELECT name, value FROM v$parameter
        WHERE name IN ('db_recovery_file_dest', 'db_recovery_file_dest_size',
                       'db_create_file_dest', 'log_archive_dest', 'log_archive_dest_1')
        ORDER BY name;
        SELECT l.group#, l.bytes / 1024 / 1024 AS size_mb, f.member
        FROM v$log l JOIN v$logfile f ON f.group# = l.group# ORDER BY l.group#;

        -- processes / sessions, applied on restart. CPU and memory stay at the XE defaults.
        ALTER SYSTEM SET processes={{PROCESSES}} SCOPE=SPFILE;
        ALTER SYSTEM SET sessions={{SESSIONS}} SCOPE=SPFILE;
        """);

    if (enableCdc) {
      sql.append(
          """

          -- Restart in ARCHIVELOG mode, required by Datastream. No archive destination / fast
          -- recovery area is configured on purpose: the archived logs then go to Oracle's default
          -- location ($ORACLE_HOME/dbs), only bounded by the container's disk, so the archiver
          -- can't get stuck on a quota (ORA-00257).
          SHUTDOWN IMMEDIATE
          STARTUP MOUNT
          ALTER DATABASE ARCHIVELOG;
          ALTER DATABASE OPEN;
          """);
    } else {
      sql.append("""

          SHUTDOWN IMMEDIATE
          STARTUP
          """);
    }

    sql.append(
        """

        BEGIN
          EXECUTE IMMEDIATE 'ALTER PLUGGABLE DATABASE XEPDB1 OPEN';
        EXCEPTION WHEN OTHERS THEN
          IF SQLCODE != -65019 THEN RAISE; END IF; -- ORA-65019: already open
        END;
        /
        """);

    if (enableCdc) {
      sql.append(
          """

          -- Logging as on the static instance. CDB-level settings apply to all PDBs.
          ALTER DATABASE FORCE LOGGING;
          ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
          ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;
          ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

          -- The image ships only 2 x 10 MB redo log groups: with the frequent log switches of
          -- concurrent tests (flushRedoLogs) and FORCE LOGGING / ALL supplemental logging, writes
          -- would wait for the next group to be archived. Add 3 x 100 MB groups (as on the static
          -- instance), next to the existing redo log files so that no image path is hard-coded.
          DECLARE
            v_dir   VARCHAR2(512);
            v_group NUMBER;
          BEGIN
            SELECT SUBSTR(member, 1, INSTR(member, '/', -1)) INTO v_dir
            FROM v$logfile WHERE ROWNUM = 1;
            SELECT MAX(group#) INTO v_group FROM v$log;
            FOR i IN 1 .. 3 LOOP
              v_group := v_group + 1;
              EXECUTE IMMEDIATE 'ALTER DATABASE ADD LOGFILE GROUP ' || v_group || ' ('''
                  || v_dir || 'redo' || LPAD(v_group, 2, '0') || '.log'') SIZE 100M';
            END LOOP;
          END;
          /
          """);
    }

    sql.append(
        """

        PROMPT ===== Result =====
        SELECT name, value FROM v$parameter WHERE name IN ('processes', 'sessions') ORDER BY name;
        SELECT log_mode, force_logging, supplemental_log_data_min AS min_,
               supplemental_log_data_pk AS pk, supplemental_log_data_all AS all_
        FROM v$database;
        SELECT l.group#, l.bytes / 1024 / 1024 AS size_mb, l.status, f.member
        FROM v$log l JOIN v$logfile f ON f.group# = l.group# ORDER BY l.group#;
        COLUMN destination FORMAT A70
        SELECT dest_name, destination FROM v$archive_dest WHERE status = 'VALID';
        EXIT SUCCESS
        """);

    return "set -euo pipefail\n"
        + "sqlplus -s / as sysdba <<'EOF'\n"
        + sql.toString()
            .replace("{{PROCESSES}}", String.valueOf(processes))
            .replace("{{SESSIONS}}", String.valueOf(sessions))
        + "EOF\n";
  }
}
