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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.UniformSplitterDBAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.RangePreparedStatementSetter;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DoFn to count a Range with timeout. */
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
  private final SerializableFunction<Void, DataSource> dataSourceProviderFn;
  private final long timeoutMillis;

  private final UniformSplitterDBAdapter dbAdapter;

  private final String countQuery;

  private final long numColumns;

  @JsonIgnore private transient @Nullable DataSource dataSource;

  RangeCountDoFn(
      SerializableFunction<Void, DataSource> dataSourceProviderFn,
      long timeoutMillis,
      UniformSplitterDBAdapter dbAdapter,
      String tableNme,
      ImmutableList<String> partitionColumns) {
    this.dataSourceProviderFn = dataSourceProviderFn;
    this.timeoutMillis = timeoutMillis;
    this.dbAdapter = dbAdapter;
    this.countQuery = dbAdapter.getCountQuery(tableNme, partitionColumns, timeoutMillis);
    this.numColumns = partitionColumns.size();
    this.dataSource = null;
  }

  @Setup
  public void setup() throws Exception {
    dataSource = dataSourceProviderFn.apply(null);
  }

  private Connection acquireConnection() throws SQLException {
    return checkStateNotNull(this.dataSource).getConnection();
  }

  /**
   * Count a Range with timeout. In case of timeout, the count of range is set as {@link
   * Range#INDETERMINATE_COUNT}.
   *
   * @param input range.
   * @param out output receiver to get counted range.
   * @param c process context.
   * @throws SQLException
   */
  @ProcessElement
  public void processElement(@Element Range input, OutputReceiver<Range> out, ProcessContext c)
      throws SQLException {

    long count = Range.INDETERMINATE_COUNT;
    try (Connection conn = acquireConnection()) {
      PreparedStatement stmt =
          conn.prepareStatement(
              this.countQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      stmt.setQueryTimeout((int) ((this.timeoutMillis + TIMEOUT_GRACE_MILLIS) / 1000));
      new RangePreparedStatementSetter(numColumns).setParameters(input, stmt);
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
        throw e;
      }
    } catch (Exception e) {
      // This exception is triggered from nullness checks of checker framework for the input to
      // statement preparator and hence should
      // indicate a programming error. Any other exception in the code flow returns a SQL Exception.
      // It's hard to trigger this exception for UT as the checks are not running by default in the
      // UT.
      logger.error(
          "Exception = {}, Range = {}, Query = {}, DataSource = {}",
          e,
          input,
          countQuery,
          dataSource);
      throw new RuntimeException(e);
    }
    Range output = input.withCount(count, c);
    logger.debug(
        "Counting Range = {}, Query = {}, DataSource = {}", output, countQuery, dataSource);
    out.output(output); // Output the counted Range.
  }

  private boolean checkTimeout(SQLException e) {
    if (e instanceof SQLTimeoutException) {
      return true;
    }
    return dbAdapter.checkForTimeout(e);
  }
}
