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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.DataSourceProvider;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableIdentifier;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableReadSpecification;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms.MultiTableReadAll.QueryProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DoFn} that executes SQL queries to read data from multiple database tables.
 *
 * <p>This {@code DoFn} is the core of the multi-table reader. It manages a persistent database
 * {@link Connection} per worker thread and dynamically adapts to the specific table being read by:
 *
 * <ul>
 *   <li>Selecting the correct {@link RowMapper} and {@link QueryProvider} for each {@link Range}.
 *   <li>Adjusting {@code fetchSize} on a per-table basis.
 *   <li>Reporting data lineage to the Dataflow service for each unique source table encountered.
 * </ul>
 *
 * <p>By using a single {@code DoFn} for all tables, we maintain a constant-size Dataflow graph
 * regardless of the number of tables in the migration.
 */
public class MultiTableReadFn<ParameterT, OutputT> extends DoFn<ParameterT, OutputT> {

  private final DataSourceProvider dataSourceProvider;
  private final ValueProvider<QueryProvider> query;
  private final PreparedStatementSetter<ParameterT> parameterSetter;
  private final ImmutableMap<TableIdentifier, TableReadSpecification<OutputT>>
      tableReadSpecifications;
  private final SerializableFunction<ParameterT, TableIdentifier> tableIdentifierFn;
  private final boolean disableAutoCommit;

  private transient Lock connectionLock;
  private transient DataSourceManager dataSourceManager;
  // Connections are instance-local and handled per-bundle for thread safety.
  private transient Map<String, Connection> connections;

  /** Keep track of the tables for which lineage has already been reported to avoid duplicates. */
  private transient Set<KV<String, String>> reportedLineages = ConcurrentHashMap.newKeySet();

  private static final Logger LOG = LoggerFactory.getLogger(MultiTableReadFn.class);

  public MultiTableReadFn(
      DataSourceProvider dataSourceProvider,
      ValueProvider<QueryProvider> query,
      PreparedStatementSetter<ParameterT> parameterSetter,
      ImmutableMap<TableIdentifier, TableReadSpecification<OutputT>> tableReadSpecifications,
      SerializableFunction<ParameterT, TableIdentifier> tableIdentifierFn,
      boolean disableAutoCommit) {
    this.dataSourceProvider = dataSourceProvider;
    this.query = query;
    this.parameterSetter = parameterSetter;
    this.tableReadSpecifications = tableReadSpecifications;
    this.tableIdentifierFn = tableIdentifierFn;
    this.disableAutoCommit = disableAutoCommit;
  }

  @Setup
  public void setup() throws Exception {
    this.reportedLineages = ConcurrentHashMap.newKeySet();
    this.connectionLock = new ReentrantLock();
  }

  @StartBundle
  public void startBundle() {
    this.connections = new ConcurrentHashMap<>();
    this.dataSourceManager =
        DataSourceManagerImpl.builder().setDataSourceProvider(dataSourceProvider).build();
  }

  /**
   * Lazily initializes and returns the database connection. The pattern is similar to the one used
   * in JdbcIO -
   * https://github.com/apache/beam/blob/676c998dec78e878d54ad21cde46f91cc9a598b7/sdks/java/io/jdbc/src/main/java/org/apache/beam/sdk/io/jdbc/JdbcIO.java#L2820
   *
   * @param element the input element used to generate the query for lineage reporting.
   * @return the active database connection.
   * @throws Exception if connection fails.
   */
  @VisibleForTesting
  protected Connection getConnection(ParameterT element) throws Exception {
    TableIdentifier tableIdentifier = tableIdentifierFn.apply(element);
    String dataSourceId = tableIdentifier.dataSourceId();
    Connection connection = this.connections.get(dataSourceId);
    if (connection == null) {
      connectionLock.lock();
      try {
        connection = this.connections.get(dataSourceId);
        if (connection == null) {
          DataSource dataSource = dataSourceManager.getDatasource(dataSourceId);
          connection = dataSource.getConnection();
          this.connections.put(dataSourceId, connection);

          // PostgreSQL requires autocommit to be disabled to enable cursor streaming
          // see https://jdbc.postgresql.org/documentation/head/query.html#query-with-cursor
          // This option is configurable as Informix will error
          // if calling setAutoCommit on a non-logged database
          if (disableAutoCommit) {
            LOG.info("Autocommit has been disabled for shard {}", dataSourceId);
            connection.setAutoCommit(false);
          }

          reportLineage(element, connection, dataSource, query, reportedLineages);
        }
      } finally {
        connectionLock.unlock();
      }
    }
    return connection;
  }

  /**
   * Reports data lineage to the Apache Beam {@link Lineage} metrics system.
   *
   * <p><b>What is Data Lineage Reporting?</b><br>
   * Data lineage reporting is the process of capturing and exposing the "provenance" of data as it
   * moves through a pipeline. In the context of Dataflow, this means identifying which source
   * tables and databases are being read from and which destinations they are being written to.
   *
   * <p><b>How it works:</b><br>
   * This method extracts the schema and table name from the SQL read query using regex. It then
   * resolves the Fully Qualified Name (FQN) components (scheme, host, database) from the {@link
   * DataSource} or {@link Connection}. Finally, it uses {@link Lineage#getSources()} to report this
   * information.
   *
   * <p><b>Why is it significant?</b>
   *
   * <ul>
   *   <li><b>Observability:</b> In large-scale migrations involving thousands of tables, it's
   *       crucial for operators to see exactly which data is in flight. Dataflow uses this
   *       information to populate the "Lineage" tab in the monitoring UI.
   *   <li><b>Compliance & Auditing:</b> Provides an automated audit trail of data movement, which
   *       is often a regulatory requirement for enterprise migrations.
   *   <li><b>Impact Analysis:</b> Helps engineers understand the downstream impact of source schema
   *       changes or pipeline failures by visualizing the end-to-end data flow.
   *   <li><b>Verification:</b> Ensures that the constant-size Dataflow graph (which hides the
   *       complexity of thousands of tables) is indeed processing the expected set of source
   *       entities.
   * </ul>
   *
   * <p>This method is thread-safe and ensures that lineage for a specific table is reported only
   * once per worker bundle to minimize overhead.
   *
   * @param element the input element (e.g., a Range) containing information to derive the query.
   * @param connection an active database connection.
   * @param validSource the data source used to establish the connection.
   * @param query the provider for the SQL query.
   * @param reportedLineages a set used to track and deduplicate reported lineages.
   * @throws Exception if an error occurs during lineage extraction or reporting.
   */
  @VisibleForTesting
  protected static <ParameterT> void reportLineage(
      ParameterT element,
      Connection connection,
      DataSource validSource,
      ValueProvider<QueryProvider> query,
      Set<KV<String, String>> reportedLineages)
      throws Exception {
    // REPORT LINEAGE:
    // We extract the table and schema names from the read query and report them to the
    // Dataflow service. This provides visibility into the source-to-destination mapping
    // in the Dataflow monitoring UI, which is critical for large-scale migrations.
    KV<String, String> schemaWithTable = extractTableFromReadQuery(query.get().getQuery(element));
    if (schemaWithTable != null && reportedLineages.add(schemaWithTable)) {
      FQNComponents fqn = FQNComponents.of(validSource);
      if (fqn == null) {
        fqn = FQNComponents.of(connection);
      }
      if (fqn != null) {
        fqn.reportLineage(Lineage.getSources(), schemaWithTable);
      }
    }
  }

  /** Extract schema and table name a SELECT statement. Return null if fail to extract. */
  // This is borrowed from
  // https://github.com/apache/beam/blob/676c998dec78e878d54ad21cde46f91cc9a598b7/sdks/java/io/jdbc/src/main/java/org/apache/beam/sdk/io/jdbc/JdbcUtil.java#L836
  private static final Pattern READ_STATEMENT_PATTERN =
      Pattern.compile(
          "SELECT\\s+.+?\\s+FROM\\s+(\\[?`?(?P<schemaName>[^\\s\\[\\]`]+)\\]?`?\\.)?\\[?`?(?P<tableName>[^\\s\\[\\]`]+)\\]?`?",
          Pattern.CASE_INSENSITIVE);

  /**
   * Extracts the schema and table name from a SELECT statement using regex.
   *
   * @param query the SQL query string.
   * @return a {@link KV} containing (schema, table), or null if extraction fails.
   */
  @VisibleForTesting
  protected static @Nullable KV<String, String> extractTableFromReadQuery(@Nullable String query) {
    if (query == null) {
      return null;
    }
    Matcher matchRead = READ_STATEMENT_PATTERN.matcher(query);
    if (matchRead.find()) {
      String matchedTable = matchRead.group("tableName");
      String matchedSchema = matchRead.group("schemaName");
      if (matchedTable != null) {
        return KV.of(matchedSchema, matchedTable);
      }
    }
    return null;
  }

  /**
   * Processes a single range, dynamically selecting the correct query and mapper for its table.
   *
   * @param context the process context.
   * @throws Exception if database read fails.
   */
  @ProcessElement
  public void processElement(ProcessContext context) throws Exception {
    // Only acquire the connection if we need to perform a read.
    ParameterT element = context.element();
    TableIdentifier tableIdentifier = tableIdentifierFn.apply(element);
    TableReadSpecification<OutputT> spec = tableReadSpecifications.get(tableIdentifier);
    if (spec == null) {
      throw new RuntimeException("TableReadSpecification not found for table: " + tableIdentifier);
    }
    Connection connection = getConnection(element);
    try (PreparedStatement statement =
        connection.prepareStatement(
            query.get().getQuery(element),
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY)) {
      statement.setFetchSize(spec.fetchSize());
      parameterSetter.setParameters(element, statement);
      try (ResultSet resultSet = statement.executeQuery()) {
        RowMapper<OutputT> rowMapper = spec.rowMapper();
        while (resultSet.next()) {
          context.output(rowMapper.mapRow(resultSet));
        }
      }
    }
  }

  @FinishBundle
  public void finishBundle() throws Exception {
    cleanUpConnection();
  }

  @Teardown
  public void tearDown() throws Exception {
    cleanUpConnection();
  }

  private void cleanUpConnection() {
    if (connectionLock == null || connections == null) {
      return;
    }
    connectionLock.lock();
    try {
      if (connections == null) {
        return;
      }
      for (Connection conn : connections.values()) {
        if (conn != null) {
          try {
            conn.close();
          } catch (SQLException e) {
            LOG.warn("Failed to close connection", e);
          }
        }
      }
      connections.clear();
    } finally {
      connectionLock.unlock();
      dataSourceManager.closeAll();
      this.connections = null;
      this.dataSourceManager = null;
    }
  }
}
