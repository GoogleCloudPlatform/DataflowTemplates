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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.exception.ValueMappingException;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.provider.MysqlJdbcValueMappings;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SchemaTestUtils;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomSchema.DateTime;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link SourceRow}. */
@RunWith(MockitoJUnitRunner.class)
public class JdbcSourceRowMapperTest {
  Connection conn;

  @BeforeClass
  public static void beforeClass() {
    // by default, derby uses a lock timeout of 60 seconds. In order to speed up the test
    // and detect the lock faster, we decrease this timeout
    System.setProperty("derby.locks.waitTimeout", "2");
    System.setProperty("derby.stream.error.file", "build/derby.log");
  }

  @Before
  public void initDerby() throws SQLException, ClassNotFoundException {
    // Creating testDB database
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
    conn = DriverManager.getConnection("jdbc:derby:memory:TestingDB;create=true");
  }

  @Test
  public void testMapRow() throws SQLException {
    String testTable = "test_table";

    var testCols = getTestCols();
    // Setup Test Derby DB.
    try (var statement = conn.createStatement()) {
      String createTable =
          new StringBuffer()
              .append("create table ")
              .append(testTable)
              .append(" ( ")
              .append(
                  testCols.stream()
                      .map(col -> col.colName() + " " + col.colType())
                      .collect(Collectors.joining(", ")))
              .append(" )")
              .toString();
      statement.executeUpdate(createTable);
    }
    String insertQuery =
        new StringBuffer()
            .append("INSERT INTO ")
            .append(testTable)
            .append(" ( ")
            .append(testCols.stream().map(Col::colName).collect(Collectors.joining(", ")))
            .append(" ) ")
            .append(" values ( ")
            .append(testCols.stream().map(col -> "?").collect(Collectors.joining(", ")))
            .append(" )")
            .toString();
    long maxNonNullValues = testCols.get(0).values().size();
    try (var statement = conn.prepareStatement(insertQuery)) {
      for (int valueIdx = 0; valueIdx < maxNonNullValues; valueIdx++) {
        for (int colIdx = 0; colIdx < testCols.size(); colIdx++) {
          statement.setObject(colIdx + 1, testCols.get(colIdx).values().get(valueIdx));
        }
        statement.executeUpdate();
      }
      // Add a null value for each column.
      for (int colIdx = 0; colIdx < testCols.size(); colIdx++) {
        statement.setObject(colIdx + 1, null);
      }
      statement.executeUpdate();
      conn.commit();
    }

    // Build SourceRowMapper.
    var sourceTableSchemaBuilder = SourceTableSchema.builder().setTableName(testTable);
    testCols.stream()
        .forEach(
            col ->
                sourceTableSchemaBuilder.addSourceColumnNameToSourceColumnType(
                    col.colName(), col.sourceColumnType()));

    var sourceSchemaRef = SchemaTestUtils.generateSchemaReference("public", "mydb");
    JdbcSourceRowMapper mapper =
        new JdbcSourceRowMapper(
            new MysqlJdbcValueMappings(),
            sourceSchemaRef,
            sourceTableSchemaBuilder.build(),
            "shard1");

    // Read Test Database and verify mapper.
    try (var statement = conn.createStatement()) {
      var rs = statement.executeQuery("SELECT * FROM " + testTable);
      int valueIdx = -1;
      while (rs.next()) {
        valueIdx++;
        var sourceRow = mapper.mapRow(rs);
        assertEquals(sourceSchemaRef, sourceRow.sourceSchemaReference());
        assertEquals(testTable, sourceRow.tableName());
        assertEquals("shard1", sourceRow.shardId());
        for (int colIdx = 0; colIdx < testCols.size(); colIdx++) {
          if (valueIdx < maxNonNullValues) {
            assertEquals(
                "Failed for col: " + testCols.get(colIdx).colName() + " for valueIdx: " + valueIdx,
                testCols.get(colIdx).expectedFields().get(valueIdx),
                sourceRow.getPayload().get(colIdx));
          } else {
            assertThat(sourceRow.getPayload().get(colIdx)).isNull();
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Add test case for shard id
  @Test
  public void testMapRowException() {

    String testTable = "test_table";

    var testCols = getTestCols();
    var sourceTableSchemaBuilder = SourceTableSchema.builder().setTableName(testTable);
    testCols.stream()
        .forEach(
            col ->
                sourceTableSchemaBuilder.addSourceColumnNameToSourceColumnType(
                    col.colName(), col.sourceColumnType()));
    var sourceSchemaRef = SchemaTestUtils.generateSchemaReference("public", "mydb");
    JdbcSourceRowMapper mapper =
        new JdbcSourceRowMapper(
            new MysqlJdbcValueMappings(), sourceSchemaRef, sourceTableSchemaBuilder.build(), null);
    ResultSet mockResultSet =
        Mockito.mock(
            ResultSet.class,
            withSettings()
                .defaultAnswer(
                    invocationOnMock -> {
                      throw new SQLException();
                    }));
    assertThrows(ValueMappingException.class, () -> mapper.mapRow(mockResultSet));
  }

  @Test
  public void testTimeStringMapping() throws SQLException {
    var mapping = new MysqlJdbcValueMappings().getMappings().get("TIME");
    ResultSet mockResultSet = Mockito.mock(ResultSet.class);
    when(mockResultSet.getString(anyString())).thenReturn("-838:59:58.999999");
    assertThat(mapping.mapValue(mockResultSet, "testField", null)).isEqualTo(-3020398999999L);

    when(mockResultSet.getString(anyString())).thenReturn("838:59:58.999999");
    assertThat(mapping.mapValue(mockResultSet, "testField", null)).isEqualTo(3020398999999L);

    when(mockResultSet.getString(anyString())).thenReturn("00:00:00");
    assertThat(mapping.mapValue(mockResultSet, "testField", null)).isEqualTo(0L);

    when(mockResultSet.getString(anyString())).thenReturn("invalid_data");
    Assert.assertThrows(
        java.lang.IllegalArgumentException.class,
        () -> mapping.mapValue(mockResultSet, "testField", null));
  }

  @Test
  public void testUnsupportedMapping() {
    String testTable = "test_table";
    ResultSet mockResultSet =
        Mockito.mock(
            ResultSet.class,
            withSettings()
                .defaultAnswer(
                    invocationOnMock -> {
                      throw new SQLException();
                    }));

    var sourceTableSchema =
        SourceTableSchema.builder()
            .setTableName(testTable)
            .addSourceColumnNameToSourceColumnType(
                "unsupported_col", new SourceColumnType("UNSUPPORTED", new Long[] {}, null))
            .build();
    var sourceSchemaRef = SchemaTestUtils.generateSchemaReference("public", "mydb");
    JdbcSourceRowMapper mapper =
        new JdbcSourceRowMapper(
            new MysqlJdbcValueMappings(), sourceSchemaRef, sourceTableSchema, null);
    assertThat(mapper.mapRow(mockResultSet).getPayload().get("unsupported_col")).isNull();
  }

  private static ImmutableList<Col> getTestCols() {
    // Unfortunately Derby supports only UTC.
    // TODO (vardhanvthigle): Verify time zone conversion on actual DB.
    java.sql.Timestamp timestamp = Timestamp.valueOf("2024-05-02 18:48:05.123456");

    return ImmutableList.<Col>builder()
        .add(
            Col.builder(
                    "big_int_unsigned_col",
                    "BIGINT" /* Derby does not support unsigned */,
                    new SourceColumnType("BIGINT UNSIGNED", new Long[] {20L, 0L}, null))
                .withValue(12345L, ByteBuffer.wrap(new byte[] {(byte) 0x30, (byte) 0x39}))
                .build())
        .add(
            Col.builder(
                    "big_int_col", "BIGINT", new SourceColumnType("BIGINT", new Long[] {}, null))
                .withValue(12345L)
                .build())
        .add(
            Col.builder(
                    "binary_col",
                    "CHAR(4) FOR BIT DATA /*Derby mapping for Binary*/",
                    new SourceColumnType("BINARY", new Long[] {4L}, null))
                .withValue(
                    new byte[] {0x65, 0x66},
                    new String(
                        new char[] {'6', '5', '6', '6', /*space-padding*/ '2', '0', '2', '0'}))
                .build())
        .add(
            Col.builder(
                    "bit_col",
                    "CHAR(8) FOR BIT DATA /*Derby mapping for Bit*/",
                    new SourceColumnType("BIT", new Long[] {}, null))
                .withValue(ByteBuffer.allocate(8).putLong(5L).array(), 5L)
                .build())
        .add(
            Col.builder("blob_col", "BLOB", new SourceColumnType("BLOB", new Long[] {10L}, null))
                .withValue(new byte[] {0x65, 0x66}, "6566")
                .build())
        .add(
            Col.builder("bool_col", "BOOLEAN", new SourceColumnType("BOOL", new Long[] {}, null))
                .withValue(true, 1)
                .build())
        .add(
            Col.builder(
                    "char_col", "CHAR(10)", new SourceColumnType("CHAR", new Long[] {10L}, null))
                .withValue("test", "test      ")
                .build())
        .add(
            Col.builder("date_col", "DATE", new SourceColumnType("DATE", new Long[] {}, null))
                .withValue(
                    java.sql.Date.valueOf("2024-05-02"),
                    java.sql.Date.valueOf("2024-05-02").getTime() * 1000)
                .build())
        .add(
            Col.builder(
                    "datetime_col",
                    "TIMESTAMP" /*Derby mapping for datetime*/,
                    new SourceColumnType("DATETIME", new Long[] {}, null))
                .withValue(
                    timestamp,
                    new GenericRecordBuilder(DateTime.SCHEMA)
                        .set(DateTime.DATE_FIELD_NAME, 19845)
                        .set(DateTime.TIME_FIELD_NAME, 67685123456L)
                        .build())
                .build())
        .add(
            Col.builder(
                    "decimal_col",
                    "DECIMAL(8,2)",
                    new SourceColumnType("DECIMAL", new Long[] {8L, 2L}, null))
                .withValue(123456.78, ByteBuffer.allocate(4).putInt(12345678).rewind())
                .build())
        .add(
            Col.builder("double_col", "DOUBLE", new SourceColumnType("DOUBLE", new Long[] {}, null))
                .withValue(678.12)
                .build())
        .add(
            Col.builder(
                    "enum_col",
                    "VARCHAR(20)" /* Derby does not support enum */,
                    new SourceColumnType("ENUM", new Long[] {}, null))
                .withValue("Books")
                .build())
        .add(
            Col.builder("float_col", "FLOAT", new SourceColumnType("FLOAT", new Long[] {}, null))
                .withValue(145.67f)
                .build())
        .add(
            Col.builder(
                    "integer_col", "INTEGER", new SourceColumnType("INTEGER", new Long[] {}, null))
                .withValue(42)
                .build())
        .add(
            Col.builder(
                    "unsigned_integer_col",
                    "INTEGER" /* Derby does not support unsigned */,
                    new SourceColumnType("INTEGER UNSIGNED", new Long[] {}, null))
                .withValue(2_147_483_647, 2_147_483_647L)
                .build())
        .add(
            Col.builder(
                    "json_col",
                    "VARCHAR(100)" /*derby does not support json*/,
                    new SourceColumnType("JSON", new Long[] {}, null))
                .withValue(
                    "{\"author\": \"Stephen Hawking\", \"title\": \"A Brief History of Time\", \"subject\": \"Physics\"}")
                .build())
        .add(
            Col.builder(
                    "long_blob_col",
                    "BLOB",
                    new SourceColumnType("LONGBLOB", new Long[] {10L}, null))
                .withValue(new byte[] {0x65, 0x66}, "6566")
                .build())
        .add(
            Col.builder(
                    "long_text_col",
                    "VARCHAR(10)",
                    new SourceColumnType("LONGTEXT", new Long[] {}, null))
                .withValue("test")
                .build())
        .add(
            Col.builder(
                    "medium_blob_col",
                    "BLOB",
                    new SourceColumnType("MEDIUMBLOB", new Long[] {10L}, null))
                .withValue(new byte[] {0x65, 0x66}, "6566")
                .build())
        .add(
            Col.builder(
                    "medium_int_col", "INT", new SourceColumnType("MEDIUMINT", new Long[] {}, null))
                .withValue(42)
                .build())
        .add(
            Col.builder(
                    "medium_text_col",
                    "VARCHAR(10)",
                    new SourceColumnType("MEDIUMTEXT", new Long[] {}, null))
                .withValue("text")
                .build())
        .add(
            Col.builder("set_col", "VARCHAR(10)", new SourceColumnType("SET", new Long[] {}, null))
                .withValue("New")
                .build())
        .add(
            Col.builder(
                    "small_int_col", "INT", new SourceColumnType("SMALLINT", new Long[] {}, null))
                .withValue(42)
                .build())
        .add(
            Col.builder(
                    "text_col", "VARCHAR(10)", new SourceColumnType("TEXT", new Long[] {}, null))
                .withValue("test")
                .build())
        .add(
            Col.builder("time_col", "TIME", new SourceColumnType("TIME", new Long[] {}, null))
                .withValue("23:09:02" /* Derby supports only time of the day */, 83342000000L)
                .build())
        .add(
            Col.builder(
                    "timestamp_col",
                    "TIMESTAMP",
                    new SourceColumnType("TIMESTAMP", new Long[] {}, null))
                .withValue(timestamp, 1714675685123456L)
                .build())
        .add(
            Col.builder(
                    "tiny_blob_col", "BLOB", new SourceColumnType("TINYBLOB", new Long[] {}, null))
                .withValue(new byte[] {0x65, 0x66}, "6566")
                .build())
        .add(
            Col.builder(
                    "tiny_int_col",
                    "SMALLINT",
                    new SourceColumnType("TINYINT", new Long[] {}, null))
                .withValue(42)
                .build())
        .add(
            Col.builder(
                    "tiny_text_col",
                    "VARCHAR(10)",
                    new SourceColumnType("TINYTEXT", new Long[] {}, null))
                .withValue("test")
                .build())
        .add(
            Col.builder(
                    "varbinary_col",
                    "VARCHAR(4) FOR BIT DATA /*Derby mapping for Binary*/",
                    new SourceColumnType("BINARY", new Long[] {4L}, null))
                .withValue(new byte[] {0x65, 0x66}, "6566")
                .build())
        .add(
            Col.builder(
                    "varchar_col",
                    "VARCHAR(4)",
                    new SourceColumnType("VARCHAR", new Long[] {4L}, null))
                .withValue("test")
                .build())
        .add(
            Col.builder("year_col", "BIGINT", new SourceColumnType("YEAR", new Long[] {}, null))
                .withValue(2024, 2024)
                .build())
        .build();
  }

  @After
  public void exitDerby() throws SQLException {
    conn.close();
  }
}

@AutoValue
abstract class Col {
  abstract String colName();

  abstract String colType();

  abstract SourceColumnType sourceColumnType();

  abstract ImmutableList<Object> values();

  abstract ImmutableList<Object> expectedFields();

  static Builder builder(String columnName, String columnType, SourceColumnType sourceColumnType) {
    var builder = new AutoValue_Col.Builder();
    return builder
        .setColName(columnName)
        .setColType(columnType)
        .setSourceColumnType(sourceColumnType);
  }

  @AutoValue.Builder
  public abstract static class Builder {

    abstract Builder setColName(String value);

    abstract Builder setColType(String value);

    abstract Builder setSourceColumnType(SourceColumnType value);

    abstract ImmutableList.Builder<Object> valuesBuilder();

    abstract ImmutableList.Builder<Object> expectedFieldsBuilder();

    public Builder withValue(Object value, Object field) {
      this.valuesBuilder().add(value);
      this.expectedFieldsBuilder().add(field);
      return this;
    }

    public Builder withValue(Object value) {
      this.valuesBuilder().add(value);
      this.expectedFieldsBuilder().add(value);
      return this;
    }

    public abstract Col build();
  }
}
