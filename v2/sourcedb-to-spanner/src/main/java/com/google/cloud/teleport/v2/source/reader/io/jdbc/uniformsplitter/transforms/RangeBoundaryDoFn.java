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
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.columnboundary.ColumnForBoundaryQuery;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.columnboundary.ColumnForBoundaryQueryPreparedStatementSetter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundaryExtractorFactory;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundaryTypeMapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableIdentifier;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableSplitSpecification;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DoFn to find boundary (min, max) for a column (optionally for a given parent range). */
final class RangeBoundaryDoFn extends DoFn<ColumnForBoundaryQuery, Range> implements Serializable {

  private static final Logger logger = LoggerFactory.getLogger(RangeBoundaryDoFn.class);
  private final SerializableFunction<Void, DataSource> dataSourceProviderFn;

  private final UniformSplitterDBAdapter dbAdapter;

  private final ImmutableList<TableSplitSpecification> tableSplitSpecifications;
  private final ColumnForBoundaryQueryPreparedStatementSetter
      columnForBoundaryQueryPreparedStatementSetter;

  @JsonIgnore
  private transient @Nullable Map<TableIdentifier, TableSplitSpecification>
      tableSplitSpecificationMap;

  @Nullable private BoundaryTypeMapper boundaryTypeMapper;

  @JsonIgnore private transient @Nullable DataSource dataSource;

  RangeBoundaryDoFn(
      SerializableFunction<Void, DataSource> dataSourceProviderFn,
      UniformSplitterDBAdapter dbAdapter,
      ImmutableList<TableSplitSpecification> tableSplitSpecifications,
      BoundaryTypeMapper boundaryTypeMapper) {
    this.dataSourceProviderFn = dataSourceProviderFn;
    this.dbAdapter = dbAdapter;
    this.tableSplitSpecifications = tableSplitSpecifications;
    this.dataSource = null;
    this.boundaryTypeMapper = boundaryTypeMapper;
    this.columnForBoundaryQueryPreparedStatementSetter =
        new ColumnForBoundaryQueryPreparedStatementSetter(tableSplitSpecifications);
  }

  @Setup
  public void setup() throws Exception {
    dataSource = dataSourceProviderFn.apply(null);
    this.tableSplitSpecificationMap =
        this.tableSplitSpecifications.stream()
            .collect(
                Collectors.toMap(TableSplitSpecification::tableIdentifier, Function.identity()));
  }

  private Connection acquireConnection() throws SQLException {
    return checkStateNotNull(this.dataSource).getConnection();
  }

  /**
   * DoFn to find boundary (min, max) for a column (optionally for a given parent range).
   *
   * @param input Details for the column and parent range for which a boundary is requested.
   * @param out new Range with column boundary.
   * @param c process context.
   * @throws SQLException - since this is in the run time, beam will auto retry the exception.
   */
  @ProcessElement
  public void processElement(
      @Element ColumnForBoundaryQuery input, OutputReceiver<Range> out, ProcessContext c)
      throws SQLException {
    TableSplitSpecification tableSplitSpecification =
        this.tableSplitSpecificationMap.get(input.tableIdentifier());
    String boundaryQuery =
        dbAdapter.getBoundaryQuery(
            checkStateNotNull(tableSplitSpecification).tableIdentifier().tableName(),
            tableSplitSpecification.partitionColumns().stream()
                .map(pc -> pc.columnName())
                .collect(ImmutableList.toImmutableList()),
            input.columnName());

    try (Connection conn = acquireConnection()) {
      PreparedStatement stmt =
          conn.prepareStatement(
              boundaryQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      columnForBoundaryQueryPreparedStatementSetter.setParameters(input, stmt);
      ResultSet rs = stmt.executeQuery();
      Range output =
          BoundaryExtractorFactory.create(input.columnClass())
              .getBoundary(input.partitionColumn(), rs, boundaryTypeMapper, input.tableIdentifier())
              .toRange(input.parentRange(), c);
      logger.debug(
          "Got Boundary, Input = {}, Range = {}, Query = {}, DataSource = {}",
          input,
          output,
          boundaryQuery,
          dataSource);
      out.output(output); // Output the counted Range.
    } catch (SQLException e) {
      logger.warn(
          "SQL Exception = {} while getting boundary of columnForRange = {}, Query = {}, DataSource = {}. This will be retried by Beam Runner.",
          e,
          input,
          boundaryQuery,
          dataSource);
      throw e;
    } catch (Exception e) {
      // This exception is triggered from nullness checks of checker framework for the input to
      // statement preparator and hence should
      // indicate a programming error. Any other exception in the code flow returns a SQL Exception.
      // It's hard to trigger this exception for UT as the checks are not running by default in the
      // UT.
      logger.error(
          "Exception = {}, ColumnForRange = {}, Query = {}, DataSource = {}",
          e,
          input,
          boundaryQuery,
          dataSource);
      throw new RuntimeException(e);
    }
  }
}
