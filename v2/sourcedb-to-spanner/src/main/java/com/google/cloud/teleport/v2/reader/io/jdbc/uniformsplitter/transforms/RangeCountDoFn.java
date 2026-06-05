/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.transforms;

import com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.DataSourceProvider;
import com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.UniformSplitterDBAdapter;
import com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.range.RangePreparedStatementSetter;
import com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.range.TableIdentifier;
import com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.range.TableSplitSpecification;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DoFn to count a Range with timeout and execution plan estimation fallback. */
final class RangeCountDoFn extends DoFn<Range, Range> implements Serializable {

  /**
   * We set a millisecond precision timeout for the DB query. Additionally, we also set a timeout at
   * the level of prepared statement. Setting the timeout at the level of prepared statement covers
   * for older versions of MySQL that don't honour the magic comment for timeout (note that 5.7.4+
   * support the select level timeout). Since the timeout at the level of prepared statement is in
   * seconds, we add 1500 milliseconds to cover for the rounding off error and giving 500
   * milliseconds for db to vm network latency.
   */
  private static final long TIMEOUT_GRACE_MILLIS = 1500;

  private static final Logger logger = LoggerFactory.getLogger(RangeCountDoFn.class);
  private final DataSourceProvider dataSourceProvider;
  private final long timeoutMillis;

  private final UniformSplitterDBAdapter dbAdapter;

  private final RangeCountTransform.CountMode countMode;

  private final ImmutableMap<TableIdentifier, String> countQueries;
  private final ImmutableMap<TableIdentifier, String> approxCountQueries;

  private static final AtomicBoolean loggedApproxCountError = new AtomicBoolean(false);

  private final RangePreparedStatementSetter rangePreparedStatementSetter;

  private transient DataSourceManager dataSourceManager;

  RangeCountDoFn(
      DataSourceProvider dataSourceProvider,
      long timeoutMillis,
      UniformSplitterDBAdapter dbAdapter,
      ImmutableList<TableSplitSpecification> tableSplitSpecifications) {
    this(
        dataSourceProvider,
        timeoutMillis,
        dbAdapter,
        tableSplitSpecifications,
        RangeCountTransform.CountMode.TRY_EXACT);
  }

  RangeCountDoFn(
      DataSourceProvider dataSourceProvider,
      long timeoutMillis,
      UniformSplitterDBAdapter dbAdapter,
      ImmutableList<TableSplitSpecification> tableSplitSpecifications,
      RangeCountTransform.CountMode countMode) {
    this.dataSourceProvider = dataSourceProvider;
    this.timeoutMillis = timeoutMillis;
    this.dbAdapter = dbAdapter;
    this.countMode = countMode;

    ImmutableMap.Builder<TableIdentifier, String> countQueriesBuilder = ImmutableMap.builder();
    ImmutableMap.Builder<TableIdentifier, String> approxCountQueriesBuilder =
        ImmutableMap.builder();

    for (TableSplitSpecification tableSplitSpecification : tableSplitSpecifications) {
      TableIdentifier tableIdentifier = tableSplitSpecification.tableIdentifier();
      ImmutableList<String> colNames =
          tableSplitSpecification.partitionColumns().stream()
              .map(pc -> pc.columnName())
              .collect(ImmutableList.toImmutableList());

      countQueriesBuilder.put(
          tableIdentifier,
          dbAdapter.getCountQuery(tableIdentifier.tableName(), colNames, timeoutMillis));

      if (dbAdapter.supportsApproximateCounts()) {
        approxCountQueriesBuilder.put(
            tableIdentifier,
            dbAdapter.getApproximateCountQuery(tableIdentifier.tableName(), colNames));
      }
    }
    this.countQueries = countQueriesBuilder.build();
    this.approxCountQueries = approxCountQueriesBuilder.build();
    this.rangePreparedStatementSetter = new RangePreparedStatementSetter(tableSplitSpecifications);
    this.dataSourceManager =
        DataSourceManagerImpl.builder().setDataSourceProvider(dataSourceProvider).build();
  }

  @StartBundle
  public void startBundle() throws Exception {
    this.dataSourceManager =
        DataSourceManagerImpl.builder().setDataSourceProvider(dataSourceProvider).build();
  }

  /**
   * Count a Range using two-tier operational fallback based on {@link
   * RangeCountTransform.CountMode}.
   *
   * @param input range.
   * @param out output receiver to get counted range.
   * @param c process context.
   * @throws SQLException if a fatal non-timeout database error occurs.
   */
  @ProcessElement
  public void processElement(@Element Range input, OutputReceiver<Range> out, ProcessContext c)
      throws SQLException {

    if (isInvalidValidRange(input, countQueries)) {
      logger.error(
          "Got Range {} for unknown tableIdentifier. Known Identifiers are {} and {}",
          input,
          countQueries);
      throw new RuntimeException("Invalid Range");
    }

    DataSource dataSource = dataSourceManager.getDatasource(input.tableIdentifier().dataSourceId());

    long count = Range.INDETERMINATE_COUNT;
    long approxCount = Range.INDETERMINATE_COUNT;

    if (this.countMode == RangeCountTransform.CountMode.APPROX) {
      approxCount = executeApproxCount(input, dataSource);
    } else {
      try {
        count = executeExactCount(input, dataSource);
        approxCount = count;
      } catch (SQLException e) {
        if (checkTimeout(e)) {
          logger.warn("Hard count timed out for Range = {}; falling back to EXPLAIN...", input);
          approxCount = executeApproxCount(input, dataSource);
        } else {
          throw e;
        }
      }
    }

    Range output = input.withCounts(count, approxCount, c);
    logger.debug(
        "Counting Range = {}, Query = {}, DataSource = {}",
        output,
        countQueries.get(input.tableIdentifier()),
        dataSource);
    out.output(output);
  }

  private long executeExactCount(Range input, DataSource dataSource) throws SQLException {
    long count = Range.INDETERMINATE_COUNT;
    String countQuery = countQueries.get(input.tableIdentifier());
    try (Connection conn = dataSource.getConnection()) {
      PreparedStatement stmt =
          conn.prepareStatement(
              countQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      stmt.setQueryTimeout((int) ((this.timeoutMillis + TIMEOUT_GRACE_MILLIS) / 1000));
      rangePreparedStatementSetter.setParameters(input, stmt);
      ResultSet rs = stmt.executeQuery();
      if (rs.next()) {
        count = rs.getLong(1);
        if (rs.wasNull()) {
          logger.error(
              "Got null for count resultSet. Range = {}, Query = {}, DataSource = {}",
              input,
              countQuery,
              dataSource);
          count = Range.INDETERMINATE_COUNT;
        }
      } else {
        logger.error(
            "Got empty count resultSet. Range = {}, Query = {}, DataSource = {}",
            input,
            countQuery,
            dataSource);
      }
    } catch (SQLException e) {
      if (checkTimeout(e)) {
        logger.warn(
            "Handled timeout while counting Range = {}, Query = {}, DataSource = {}, timeoutMillis = {}, Exception {}, sqlState {}, errorCode {}",
            input,
            countQuery,
            dataSource,
            timeoutMillis,
            e,
            e.getSQLState(),
            e.getErrorCode());
      } else {
        logger.error(
            "Non-timeout SQL Exception while counting Range = {}, Query = {}, DataSource = {}, timeoutMillis = {}, exception {}, sqlState {}, errorCode {}",
            input,
            countQuery,
            dataSource,
            timeoutMillis,
            e,
            e.getSQLState(),
            e.getErrorCode());
      }
      throw e;
    } catch (Exception e) {
      logger.error(
          "Exception = {}, Range = {}, Query = {}, DataSource = {}",
          e,
          input,
          countQuery,
          dataSource);
      throw new RuntimeException(e);
    }
    return count;
  }

  private long executeApproxCount(Range input, DataSource dataSource) {
    if (!dbAdapter.supportsApproximateCounts()) {
      return Range.INDETERMINATE_COUNT;
    }
    String query = approxCountQueries.get(input.tableIdentifier());
    if (query == null) {
      return Range.INDETERMINATE_COUNT;
    }
    try (Connection conn = dataSource.getConnection();
        PreparedStatement stmt = conn.prepareStatement(query)) {
      stmt.setQueryTimeout((int) ((this.timeoutMillis + TIMEOUT_GRACE_MILLIS) / 1000));
      rangePreparedStatementSetter.setParameters(input, stmt);
      try (ResultSet rs = stmt.executeQuery()) {
        long approxCount = dbAdapter.parseApproximateCount(rs);
        return approxCount == -1L ? Range.INDETERMINATE_COUNT : approxCount;
      }
    } catch (SQLException e) {
      if (loggedApproxCountError.compareAndSet(false, true)) {
        logger.error("EXPLAIN approximate count query failed on worker: {}", e.getMessage(), e);
      }
      return Range.INDETERMINATE_COUNT;
    } catch (Exception e) {
      if (loggedApproxCountError.compareAndSet(false, true)) {
        logger.error("Unexpected error in approximate count on worker: {}", e.getMessage(), e);
      }
      return Range.INDETERMINATE_COUNT;
    }
  }

  @VisibleForTesting
  protected static boolean isInvalidValidRange(
      Range input, ImmutableMap<TableIdentifier, String> countQueries) {
    return !countQueries.containsKey(input.tableIdentifier());
  }

  private boolean checkTimeout(SQLException e) {
    if (e instanceof SQLTimeoutException) {
      return true;
    }
    return dbAdapter.checkForTimeout(e);
  }

  @FinishBundle
  public void finishBundle() throws Exception {
    cleanupDataSource();
  }

  @Teardown
  public void tearDown() throws Exception {
    cleanupDataSource();
  }

  /**
   * Closes all active data source connections.
   *
   * <p>This method ensures that the {@link DataSourceManager} releases all resources, preventing
   * connection pool leaks during bundle finish or worker teardown.
   */
  void cleanupDataSource() {
    if (this.dataSourceManager != null) {
      this.dataSourceManager.closeAll();
      this.dataSourceManager = null;
    }
  }

  @VisibleForTesting
  static void resetLoggedApproxCountError() {
    loggedApproxCountError.set(false);
  }
}
