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
package com.google.cloud.teleport.v2.spanner.resourcemanager;

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
 * <p>After the container starts, a one-time setup script is run inside it to mirror the settings of
 * the static Oracle instance previously used by the ITs (CPU and memory are left at what the XE
 * container offers):
 *
 * <ul>
 *   <li>{@code processes} / {@code sessions}: as high as XE allows (default 4000 / 6000,
 *       overridable with {@code -DoracleProcesses} / {@code -DoracleSessions}). The static instance
 *       used 20000 / 30000, but XE caps the SGA at 1.5G and Oracle reserves SGA per allowed
 *       process/session: above ~5000 processes the instance fails to start with ORA-00821.
 *   <li>{@code streams_pool_size=64M}, {@code open_cursors=300}.
 *   <li>Only when CDC is enabled (needed by Datastream): ARCHIVELOG mode with archived logs in a
 *       12G fast recovery area, FORCE LOGGING, MIN/PK/ALL supplemental logging at the CDB and PDB
 *       level and 3 x 100 MB redo log groups.
 * </ul>
 *
 * <p>The container is published on {@link TestProperties#hostIp()} (the IP of the machine running
 * the tests, passed as {@code -DhostIp}) and its mapped port, so that it is reachable by Dataflow
 * workers and Datastream, and not only from the test JVM. It is removed by Testcontainers when the
 * JVM exits. *
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
              "Oracle container setup script failed with exit code %d. The original parameters"
                  + " are saved in /opt/oracle/oradata/init_before_it_setup.ora.%nstdout:%n%s%n"
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
   * the database. Any SQL error aborts the script with a non-zero exit code.
   */
  static String buildSetupScript(boolean enableCdc, int processes, int sessions) {
    StringBuilder sql = new StringBuilder();
    sql.append(
        """
        WHENEVER SQLERROR EXIT FAILURE
        WHENEVER OSERROR EXIT FAILURE
        SET ECHO ON FEEDBACK ON SERVEROUTPUT ON LINESIZE 200 PAGESIZE 100 SQLBLANKLINES ON

        -- Keep a copy of the original parameters, to debug if the restart below fails.
        CREATE PFILE='/opt/oracle/oradata/init_before_it_setup.ora' FROM SPFILE;

        -- 1. Instance parameters (applied on restart). processes/sessions as high as XE allows.
        --    CPU and memory are left at the XE / container defaults.
        ALTER SYSTEM SET processes={{PROCESSES}} SCOPE=SPFILE;
        ALTER SYSTEM SET sessions={{SESSIONS}} SCOPE=SPFILE;
        ALTER SYSTEM SET streams_pool_size=64M SCOPE=SPFILE;
        ALTER SYSTEM SET open_cursors=300 SCOPE=BOTH;
        """);

    if (enableCdc) {
      sql.append(
          """

          -- 2. OMF and fast recovery area for the archived logs, as on the static instance.
          ALTER SYSTEM SET db_create_file_dest='/opt/oracle/oradata' SCOPE=BOTH;
          ALTER SYSTEM SET db_recovery_file_dest_size=12G SCOPE=BOTH;
          ALTER SYSTEM SET db_recovery_file_dest='/opt/oracle/oradata/fra' SCOPE=BOTH;
          ALTER SYSTEM SET log_archive_dest_1='LOCATION=USE_DB_RECOVERY_FILE_DEST' SCOPE=BOTH;

          -- 3. Restart in ARCHIVELOG mode.
          SHUTDOWN IMMEDIATE
          STARTUP MOUNT
          ALTER DATABASE ARCHIVELOG;
          ALTER DATABASE OPEN;
          """);
    } else {
      sql.append(
          """

          -- 2. Restart to apply the instance parameters.
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
        ALTER PLUGGABLE DATABASE XEPDB1 SAVE STATE;
        """);

    if (enableCdc) {
      sql.append(
          """

          -- 4. CDB-level logging: FORCE LOGGING and MIN / PK / ALL supplemental logging.
          ALTER DATABASE FORCE LOGGING;
          ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
          ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;
          ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

          -- 5. Redo logs: 3 groups of 100 MB, only if the image ships smaller ones.
          DECLARE
            v_small  NUMBER;
            v_status v$log.status%TYPE;
            v_arch   v$log.archived%TYPE;
          BEGIN
            SELECT COUNT(*) INTO v_small FROM v$log WHERE bytes < 100 * 1024 * 1024;
            IF v_small = 0 THEN
              DBMS_OUTPUT.PUT_LINE('Redo logs already >= 100 MB, nothing to do.');
              RETURN;
            END IF;
            FOR i IN 1 .. 3 LOOP
              EXECUTE IMMEDIATE 'ALTER DATABASE ADD LOGFILE SIZE 100M';
            END LOOP;
            FOR r IN (SELECT group# FROM v$log WHERE bytes < 100 * 1024 * 1024) LOOP
              FOR attempt IN 1 .. 30 LOOP
                SELECT status, archived INTO v_status, v_arch FROM v$log WHERE group# = r.group#;
                IF v_status IN ('INACTIVE', 'UNUSED') AND v_arch = 'YES' THEN
                  EXECUTE IMMEDIATE 'ALTER DATABASE DROP LOGFILE GROUP ' || r.group#;
                  DBMS_OUTPUT.PUT_LINE('Dropped small redo log group ' || r.group#);
                  EXIT;
                END IF;
                EXECUTE IMMEDIATE 'ALTER SYSTEM SWITCH LOGFILE';
                EXECUTE IMMEDIATE 'ALTER SYSTEM CHECKPOINT';
                DBMS_SESSION.SLEEP(1);
              END LOOP;
            END LOOP;
          END;
          /

          -- 6. PDB-level MIN / PK / ALL supplemental logging, as on the static XEPDB1. The
          --    CDB-level settings above already apply to all PDBs, so failures are only logged.
          ALTER SESSION SET CONTAINER = XEPDB1;
          DECLARE
            PROCEDURE add_log(p_clause VARCHAR2) IS
            BEGIN
              EXECUTE IMMEDIATE 'ALTER PLUGGABLE DATABASE ADD SUPPLEMENTAL LOG DATA' || p_clause;
            EXCEPTION WHEN OTHERS THEN
              IF SQLCODE = -32588 THEN
                NULL; -- ORA-32588: already enabled
              ELSE
                DBMS_OUTPUT.PUT_LINE('WARNING: PDB supplemental logging' || p_clause
                    || ' failed: ' || SQLERRM);
              END IF;
            END;
          BEGIN
            add_log('');
            add_log(' (PRIMARY KEY) COLUMNS');
            add_log(' (ALL) COLUMNS');
          END;
          /
          PROMPT ===== XEPDB1 supplemental logging =====
          SELECT * FROM dba_supplemental_logging;
          ALTER SESSION SET CONTAINER = CDB$ROOT;
          """);
    }

    sql.append(
        """

        -- Make sure the services are registered with the listener right away.
        ALTER SYSTEM REGISTER;

        -- Verification, to compare with the static instance.
        PROMPT ===== Parameters =====
        COLUMN name FORMAT A30
        COLUMN value FORMAT A60
        SELECT name, value FROM v$parameter
        WHERE name IN ('processes', 'sessions', 'streams_pool_size', 'open_cursors',
                       'db_create_file_dest', 'db_recovery_file_dest',
                       'db_recovery_file_dest_size', 'log_archive_dest_1', 'sga_target',
                       'pga_aggregate_target', 'cpu_count')
        ORDER BY name;
        PROMPT ===== Log mode / CDB supplemental logging =====
        SELECT log_mode, force_logging, supplemental_log_data_min AS min_,
               supplemental_log_data_pk AS pk, supplemental_log_data_ui AS ui,
               supplemental_log_data_fk AS fk, supplemental_log_data_all AS all_
        FROM v$database;
        PROMPT ===== Redo logs =====
        SELECT group#, bytes / 1024 / 1024 AS size_mb, status, archived FROM v$log ORDER BY group#;
        EXIT SUCCESS
        """);

    return "set -euo pipefail\n"
        + "mkdir -p /opt/oracle/oradata/fra\n"
        + "sqlplus -s / as sysdba <<'EOF'\n"
        + sql.toString()
            .replace("{{PROCESSES}}", String.valueOf(processes))
            .replace("{{SESSIONS}}", String.valueOf(sessions))
        + "EOF\n";
  }
}
