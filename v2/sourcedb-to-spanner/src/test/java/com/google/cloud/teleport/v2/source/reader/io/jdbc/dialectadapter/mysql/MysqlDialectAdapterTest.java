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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter.InformationSchemaCols;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter.InformationSchemaStatsCols;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter.MySqlVersion;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo.IndexType;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.OngoingStubbing;

/** Test class for {@link MysqlDialectAdapter}. */
@RunWith(MockitoJUnitRunner.class)
public class MysqlDialectAdapterTest {
  @Mock DataSource mockDataSource;
  @Mock Connection mockConnection;

  @Mock PreparedStatement mockPreparedStatement;

  @Mock Statement mockStatement;

  @Test
  public void testDiscoverTableSchema() throws SQLException, RetriableSchemaDiscoveryException {
    final String testTable = "testTable";
    final SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.builder().setDbName("testDB").build();
    final ResultSet mockResultSet = getMockInfoSchemaRs();

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    doNothing().when(mockPreparedStatement).setString(1, testTable);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);

    assertThat(
            new MysqlDialectAdapter(MySqlVersion.DEFAULT)
                .discoverTableSchema(
                    mockDataSource, sourceSchemaReference, ImmutableList.of(testTable)))
        .isEqualTo(getExpectedColumnMapping(testTable));
  }

  @Test
  public void testDiscoverTableSchemaGetConnectionException() throws SQLException {
    final String testTable = "testTable";
    final SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.builder().setDbName("testDB").build();

    when(mockDataSource.getConnection())
        .thenThrow(new SQLTransientConnectionException("test"))
        .thenThrow(new SQLNonTransientConnectionException("test"));

    assertThrows(
        RetriableSchemaDiscoveryException.class,
        () ->
            new MysqlDialectAdapter(MySqlVersion.DEFAULT)
                .discoverTableSchema(
                    mockDataSource, sourceSchemaReference, ImmutableList.of(testTable)));

    assertThrows(
        SchemaDiscoveryException.class,
        () ->
            new MysqlDialectAdapter(MySqlVersion.DEFAULT)
                .discoverTableSchema(
                    mockDataSource, sourceSchemaReference, ImmutableList.of(testTable)));
  }

  @Test
  public void testDiscoverTableSchemaPrepareStatementException() throws SQLException {
    final String testTable = "testTable";
    final SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.builder().setDbName("testDB").build();

    when(mockConnection.prepareStatement(anyString())).thenThrow(new SQLException("test"));
    when(mockDataSource.getConnection()).thenReturn(mockConnection);

    assertThrows(
        SchemaDiscoveryException.class,
        () ->
            new MysqlDialectAdapter(MySqlVersion.DEFAULT)
                .discoverTableSchema(
                    mockDataSource, sourceSchemaReference, ImmutableList.of(testTable)));
  }

  @Test
  public void testDiscoverTableSchemaSetStringException() throws SQLException {
    final String testTable = "testTable";
    final SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.builder().setDbName("testDB").build();

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    doThrow(SQLException.class).when(mockPreparedStatement).setString(1, testTable);

    assertThrows(
        SchemaDiscoveryException.class,
        () ->
            new MysqlDialectAdapter(MySqlVersion.DEFAULT)
                .discoverTableSchema(
                    mockDataSource, sourceSchemaReference, ImmutableList.of(testTable)));
  }

  @Test
  public void testDiscoverTableSchemaExecuteQueryException() throws SQLException {

    final String testTable = "testTable";
    final SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.builder().setDbName("testDB").build();

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    doNothing().when(mockPreparedStatement).setString(1, testTable);
    when(mockPreparedStatement.executeQuery()).thenThrow(new SQLException());

    assertThrows(
        SchemaDiscoveryException.class,
        () ->
            new MysqlDialectAdapter(MySqlVersion.DEFAULT)
                .discoverTableSchema(
                    mockDataSource, sourceSchemaReference, ImmutableList.of(testTable)));
  }

  @Test
  public void testDiscoverTableSchemaRsException() throws SQLException {

    final String testTable = "testTable";
    final SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.builder().setDbName("testDB").build();
    ResultSet mockResultSet = mock(ResultSet.class);

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    doNothing().when(mockPreparedStatement).setString(1, testTable);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenThrow(new SQLException());

    assertThrows(
        SchemaDiscoveryException.class,
        () ->
            new MysqlDialectAdapter(MySqlVersion.DEFAULT)
                .discoverTableSchema(
                    mockDataSource, sourceSchemaReference, ImmutableList.of(testTable)));
  }

  @Test
  public void testGetSchemaDiscoveryQuery() {
    assertThat(
            MysqlDialectAdapter.getSchemaDiscoveryQuery(
                SourceSchemaReference.builder().setDbName("testDB").build()))
        .isEqualTo(
            "SELECT COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE FROM INFORMATION_SCHEMA.Columns WHERE TABLE_SCHEMA = 'testDB' AND TABLE_NAME = ?");
  }

  @Test
  public void testDiscoverTablesBasic() throws SQLException, RetriableSchemaDiscoveryException {
    ImmutableList<String> testTables =
        ImmutableList.of("testTable1", "testTable2", "testTable3", "testTable4");

    final SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.builder().setDbName("testDB").build();
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery("SHOW TABLES in testDB")).thenReturn(mockResultSet);
    OngoingStubbing stubGetString = when(mockResultSet.getString(1));
    for (String tbl : testTables) {
      stubGetString = stubGetString.thenReturn(tbl);
    }
    // Unfortunately Mocktio does not let us wire 2 stubs in parallel.
    OngoingStubbing stubNext = when(mockResultSet.next());
    for (String tbl : testTables) {
      stubNext = stubNext.thenReturn(true);
    }
    stubNext.thenReturn(false);

    ImmutableList<String> tables =
        new MysqlDialectAdapter(MySqlVersion.DEFAULT)
            .discoverTables(mockDataSource, sourceSchemaReference);

    assertThat(tables).isEqualTo(testTables);
  }

  @Test
  public void testDiscoverTablesGetConnectionException() throws SQLException {

    final SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.builder().setDbName("testDB").build();

    when(mockDataSource.getConnection())
        .thenThrow(new SQLTransientConnectionException("test"))
        .thenThrow(new SQLNonTransientConnectionException("test"));

    assertThrows(
        RetriableSchemaDiscoveryException.class,
        () ->
            new MysqlDialectAdapter(MySqlVersion.DEFAULT)
                .discoverTables(mockDataSource, sourceSchemaReference));

    assertThrows(
        SchemaDiscoveryException.class,
        () ->
            new MysqlDialectAdapter(MySqlVersion.DEFAULT)
                .discoverTables(mockDataSource, sourceSchemaReference));
  }

  @Test
  public void testDiscoverTablesRsException() throws SQLException {

    final SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.builder().setDbName("testDB").build();
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery("SHOW TABLES in testDB")).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString(1)).thenThrow(new SQLException("test"));

    assertThrows(
        SchemaDiscoveryException.class,
        () ->
            new MysqlDialectAdapter(MySqlVersion.DEFAULT)
                .discoverTables(mockDataSource, sourceSchemaReference));
  }

  @Test
  public void testDiscoverIndexesBasic() throws SQLException, RetriableSchemaDiscoveryException {
    ImmutableList<String> testTables = ImmutableList.of("testTable1");
    ImmutableList<String> colTypes = ImmutableList.of("varchar", "integer");
    ImmutableList<SourceColumnIndexInfo> expectedSourceColumnIndexInfos =
        ImmutableList.of(
            SourceColumnIndexInfo.builder()
                .setColumnName("testCol1")
                .setIndexName("testIndex1")
                .setIsUnique(false)
                .setIsPrimary(false)
                .setCardinality(42L)
                .setOrdinalPosition(1)
                .setIndexType(IndexType.OTHER)
                .build(),
            SourceColumnIndexInfo.builder()
                .setColumnName("testCol1")
                .setIndexName("primary")
                .setIsUnique(true)
                .setIsPrimary(true)
                .setCardinality(42L)
                .setIndexType(IndexType.NUMERIC)
                .setOrdinalPosition(1)
                .build());

    final SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.builder().setDbName("testDB").build();
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    doNothing().when(mockPreparedStatement).setString(1, testTables.get(0));
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);

    OngoingStubbing stubGetColName =
        when(mockResultSet.getString(InformationSchemaStatsCols.COL_NAME_COL));
    for (SourceColumnIndexInfo info : expectedSourceColumnIndexInfos) {
      stubGetColName = stubGetColName.thenReturn(info.columnName());
    }
    // Unfortunately Mocktio does not let us wire 2 stubs in parallel.
    OngoingStubbing stubGetIndexName =
        when(mockResultSet.getString(InformationSchemaStatsCols.INDEX_NAME_COL));
    for (SourceColumnIndexInfo info : expectedSourceColumnIndexInfos) {
      stubGetIndexName = stubGetIndexName.thenReturn(info.indexName());
    }
    OngoingStubbing stubGetNonUnique =
        when(mockResultSet.getBoolean(InformationSchemaStatsCols.NON_UNIQ_COL));
    for (SourceColumnIndexInfo info : expectedSourceColumnIndexInfos) {
      stubGetNonUnique = stubGetNonUnique.thenReturn(!info.isUnique());
    }
    OngoingStubbing stubGetCardinality =
        when(mockResultSet.getLong(InformationSchemaStatsCols.CARDINALITY_COL));
    for (SourceColumnIndexInfo info : expectedSourceColumnIndexInfos) {
      stubGetCardinality = stubGetCardinality.thenReturn(info.cardinality());
    }
    OngoingStubbing stubGetOrdinalPos =
        when(mockResultSet.getLong(InformationSchemaStatsCols.ORDINAL_POS_COL));
    for (SourceColumnIndexInfo info : expectedSourceColumnIndexInfos) {
      stubGetOrdinalPos = stubGetOrdinalPos.thenReturn(info.ordinalPosition());
    }
    OngoingStubbing stubGetColType =
        when(mockResultSet.getString(InformationSchemaStatsCols.TYPE_COL));
    for (String colType : colTypes) {
      stubGetColType = stubGetColType.thenReturn(colType);
    }
    OngoingStubbing stubNext = when(mockResultSet.next());
    for (long i = 0; i < expectedSourceColumnIndexInfos.size(); i++) {
      stubNext = stubNext.thenReturn(true);
    }
    stubNext = stubNext.thenReturn(false);

    ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> discoveredIndexes =
        new MysqlDialectAdapter(MySqlVersion.DEFAULT)
            .discoverTableIndexes(mockDataSource, sourceSchemaReference, testTables);

    assertThat(discoveredIndexes)
        .isEqualTo(ImmutableMap.of(testTables.get(0), expectedSourceColumnIndexInfos));
  }

  @Test
  public void testGetIndexDiscoveryQuery() {
    assertThat(
            MysqlDialectAdapter.getIndexDiscoveryQuery(
                SourceSchemaReference.builder().setDbName("testDB").build()))
        .isEqualTo(
            "SELECT stats.COLUMN_NAME,stats.INDEX_NAME,stats.SEQ_IN_INDEX,stats.NON_UNIQUE,stats.CARDINALITY,cols.DATA_TYPE FROM INFORMATION_SCHEMA.STATISTICS stats JOIN INFORMATION_SCHEMA.COLUMNS cols ON stats.table_schema = cols.table_schema AND stats.table_name = cols.table_name AND stats.column_name = cols.column_name WHERE stats.TABLE_SCHEMA = 'testDB' AND stats.TABLE_NAME = ?");
  }

  @Test
  public void testDiscoverTableIndexesGetConnectionException() throws SQLException {
    final String testTable = "testTable";
    final SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.builder().setDbName("testDB").build();

    when(mockDataSource.getConnection())
        .thenThrow(new SQLTransientConnectionException("test"))
        .thenThrow(new SQLNonTransientConnectionException("test"));

    assertThrows(
        RetriableSchemaDiscoveryException.class,
        () ->
            new MysqlDialectAdapter(MySqlVersion.DEFAULT)
                .discoverTableIndexes(
                    mockDataSource, sourceSchemaReference, ImmutableList.of(testTable)));

    assertThrows(
        SchemaDiscoveryException.class,
        () ->
            new MysqlDialectAdapter(MySqlVersion.DEFAULT)
                .discoverTableIndexes(
                    mockDataSource, sourceSchemaReference, ImmutableList.of(testTable)));
  }

  @Test
  public void testDiscoverIndexesSqlExceptions()
      throws SQLException, RetriableSchemaDiscoveryException {
    ImmutableList<String> testTables = ImmutableList.of("testTable1");
    long exceptionCount = 0;

    final SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.builder().setDbName("testDB").build();
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString()))
        .thenThrow(new SQLException("test"))
        .thenReturn(mockPreparedStatement);
    exceptionCount++;
    doThrow(new SQLException("test"))
        .doNothing()
        .when(mockPreparedStatement)
        .setString(1, testTables.get(0));
    exceptionCount++;
    when(mockPreparedStatement.executeQuery())
        .thenThrow(new SQLException("test"))
        .thenReturn(mockResultSet);
    exceptionCount++;
    when(mockResultSet.next()).thenThrow(new SQLException("test")).thenReturn(true);
    exceptionCount++;
    when(mockResultSet.getString(anyString())).thenThrow(new SQLException("test"));
    exceptionCount++;
    for (long i = 0; i < exceptionCount; i++) {
      assertThrows(
          SchemaDiscoveryException.class,
          () ->
              new MysqlDialectAdapter(MySqlVersion.DEFAULT)
                  .discoverTableIndexes(mockDataSource, sourceSchemaReference, testTables));
    }
  }

  private static ResultSet getMockInfoSchemaRs() throws SQLException {
    return new MockRSBuilder(
            MockInformationSchema.builder()
                /* Row of Information Schema Table */
                .withColName("int_col")
                .withDataType("int")
                .withCharMaxLength(null)
                .withNumericPrecision(null)
                .withNumericScale(null)
                /* Row of Information Schema Table */
                .withColName("varbinary_col")
                .withDataType("varbinary(10)")
                .withCharMaxLength(10L)
                .withNumericPrecision(null)
                .withNumericScale(null)
                /* Row of Information Schema Table */
                .withColName("dec_precision_col")
                .withDataType("dec(10)")
                .withCharMaxLength(null)
                .withNumericPrecision(10L)
                .withNumericScale(null)
                /* Row of Information Schema Table. */
                .withColName("dec_precision_scale_col")
                .withDataType("dec")
                .withCharMaxLength(null)
                .withNumericPrecision(10L)
                .withNumericScale(5L)
                /* Test DataType normalizations */
                /* Row of Information Schema Table.*/
                .withColName("tinyint_col")
                .withDataType("Tinyint(1)")
                .withCharMaxLength(null)
                .withNumericPrecision(null)
                .withNumericScale(null)

                /* Row of Information Schema Table.*/
                .withColName("bigint_col")
                .withDataType("BIGINT(20)")
                .withCharMaxLength(null)
                .withNumericPrecision(15L)
                .withNumericScale(null)

                /* Row of Information Schema Table.*/
                .withColName("bigint_unsigned_col")
                .withDataType("BIGINT(20) UNSIGNED")
                .withCharMaxLength(null)
                .withNumericPrecision(10L)
                .withNumericScale(null)

                /* Row of Information Schema Table.*/
                .withColName("int_unsigned_col")
                .withDataType("int(20) UNSIGNED")
                .withCharMaxLength(null)
                .withNumericPrecision(null)
                .withNumericScale(null)
                .withColName("tiny_int_unsigned_col")
                .withDataType("tinyint(20) UNSIGNED")
                .withCharMaxLength(null)
                .withNumericPrecision(null)
                .withNumericScale(null)
                .build())
        .createMock();
  }

  private static ImmutableMap<String, ImmutableMap<String, SourceColumnType>>
      getExpectedColumnMapping(String testTable) {

    return ImmutableMap.of(
        testTable,
        ImmutableMap.<String, SourceColumnType>builder()
            .put("int_col", new SourceColumnType("INTEGER", new Long[] {}, null))
            .put("varbinary_col", new SourceColumnType("VARBINARY", new Long[] {10L}, null))
            .put("dec_precision_col", new SourceColumnType("DECIMAL", new Long[] {10L}, null))
            .put(
                "dec_precision_scale_col",
                new SourceColumnType("DECIMAL", new Long[] {10L, 5L}, null))
            .put("tinyint_col", new SourceColumnType("TINYINT", new Long[] {}, null))
            .put("bigint_col", new SourceColumnType("BIGINT", new Long[] {15L}, null))
            .put(
                "bigint_unsigned_col",
                new SourceColumnType("BIGINT UNSIGNED", new Long[] {10L}, null))
            .put("int_unsigned_col", new SourceColumnType("INTEGER UNSIGNED", new Long[] {}, null))
            .put("tiny_int_unsigned_col", new SourceColumnType("TINYINT", new Long[] {}, null))
            .build());
  }
}

class MockRSBuilder {
  private final MockInformationSchema schema;
  private int rowIndex;
  private Boolean wasNull = null;

  MockRSBuilder(MockInformationSchema schema) {
    this.schema = schema;
    this.rowIndex = -1;
  }

  ResultSet createMock() throws SQLException {
    final var rs = mock(ResultSet.class);

    // mock rs.next()
    doAnswer(
            invocation -> {
              rowIndex = rowIndex + 1;
              wasNull = null;
              return rowIndex < schema.colNames().size();
            })
        .when(rs)
        .next();

    // mock rs.getString("COLUMN_NAME");
    doAnswer(
            invocation -> {
              wasNull = null;
              return schema.colNames().get(rowIndex);
            })
        .when(rs)
        .getString(InformationSchemaCols.NAME_COL);

    // mock rs.getString("DATA_TYPE");
    doAnswer(
            invocation -> {
              wasNull = null;
              return schema.dataTypes().get(rowIndex);
            })
        .when(rs)
        .getString(InformationSchemaCols.TYPE_COL);

    // mock rs.getString("CHARACTER_MAXIMUM_LENGTH");
    doAnswer(
            invocation -> {
              wasNull = schema.charMaxLengthWasNulls().get(rowIndex);
              return schema.charMaxLengths().get(rowIndex);
            })
        .when(rs)
        .getLong(InformationSchemaCols.CHAR_MAX_LENGTH_COL);

    doAnswer(
            invocation -> {
              wasNull = schema.numericPrecisionWasNulls().get(rowIndex);
              return schema.numericPrecisions().get(rowIndex);
            })
        .when(rs)
        .getLong(InformationSchemaCols.NUMERIC_PRECISION_COL);

    doAnswer(
            invocation -> {
              wasNull = schema.numericScaleWasNulls().get(rowIndex);
              return schema.numericScales().get(rowIndex);
            })
        .when(rs)
        .getLong(InformationSchemaCols.NUMERIC_SCALE_COL);

    doAnswer(invocation -> wasNull).when(rs).wasNull();
    return rs;
  }
}

@AutoValue
abstract class MockInformationSchema {
  abstract ImmutableList<String> colNames();

  abstract ImmutableList<String> dataTypes();

  abstract ImmutableList<Long> charMaxLengths();

  abstract ImmutableList<Boolean> charMaxLengthWasNulls();

  abstract ImmutableList<Long> numericPrecisions();

  abstract ImmutableList<Boolean> numericPrecisionWasNulls();

  abstract ImmutableList<Long> numericScales();

  abstract ImmutableList<Boolean> numericScaleWasNulls();

  public static Builder builder() {
    return new AutoValue_MockInformationSchema.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract ImmutableList.Builder<String> colNamesBuilder();

    abstract ImmutableList.Builder<String> dataTypesBuilder();

    abstract ImmutableList.Builder<Long> charMaxLengthsBuilder();

    abstract ImmutableList.Builder<Boolean> charMaxLengthWasNullsBuilder();

    abstract ImmutableList.Builder<Long> numericPrecisionsBuilder();

    abstract ImmutableList.Builder<Boolean> numericPrecisionWasNullsBuilder();

    abstract ImmutableList.Builder<Long> numericScalesBuilder();

    abstract ImmutableList.Builder<Boolean> numericScaleWasNullsBuilder();

    public Builder withColName(String colName) {
      this.colNamesBuilder().add(colName);
      return this;
    }

    public Builder withDataType(String dataType) {
      this.dataTypesBuilder().add(dataType);
      return this;
    }

    public Builder withCharMaxLength(Long charMaxLength) {
      if (charMaxLength == null) {
        this.charMaxLengthsBuilder().add(0L);
        this.charMaxLengthWasNullsBuilder().add(true);
      } else {
        this.charMaxLengthsBuilder().add(charMaxLength);
        this.charMaxLengthWasNullsBuilder().add(false);
      }
      return this;
    }

    public Builder withNumericPrecision(Long precision) {
      if (precision == null) {
        this.numericPrecisionsBuilder().add(0L);
        this.numericPrecisionWasNullsBuilder().add(true);
      } else {
        this.numericPrecisionsBuilder().add(precision);
        this.numericPrecisionWasNullsBuilder().add(false);
      }
      return this;
    }

    public Builder withNumericScale(Long scale) {
      if (scale == null) {
        this.numericScalesBuilder().add(0L);
        this.numericScaleWasNullsBuilder().add(true);
      } else {
        this.numericScalesBuilder().add(scale);
        this.numericScaleWasNullsBuilder().add(false);
      }
      return this;
    }

    public abstract MockInformationSchema build();
  }
}
