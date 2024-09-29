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

import static com.google.cloud.teleport.v2.source.reader.io.CustomAsserts.assertColumnEquals;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.cloud.teleport.v2.source.reader.io.exception.ValueMappingException;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.provider.MysqlJdbcValueMappings;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.provider.PostgreSQLJdbcValueMappings;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SchemaTestUtils;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.UnifiedTypeMapper.MapperType;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomSchema.DateTime;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomSchema.TimeStampTz;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomSchema.TimeTz;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Locale;
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
    Locale.setDefault(Locale.US);
  }

  @Before
  public void initDerby() throws SQLException, ClassNotFoundException {
    // Creating testDB database
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
    conn = DriverManager.getConnection("jdbc:derby:memory:TestingDB;create=true");
  }

  @After
  public void exitDerby() throws SQLException {
    conn.close();
  }

  @Test
  public void testMapMSQLRows() throws SQLException {
    String namespace = "public";
    String dbName = "db";
    String table = "mysql_test_table";
    String shardId = "shard1";
    JdbcValueMappingsProvider valueMappings = new MysqlJdbcValueMappings();
    MapperType mapperType = MapperType.MYSQL;
    ImmutableList<Column> columns = mySQLColumns();

    // Create table
    createTableFrom(table, columns);
    for (Column column : columns) {
      // Insert column with a non-null value followed by a null value
      populateTable(table, column);
    }
    SourceTableSchema sourceTableSchema = sourceTableSchemaFrom(table, mapperType, columns);
    SourceSchemaReference schemaReference =
        SchemaTestUtils.generateSchemaReference(namespace, dbName);
    JdbcSourceRowMapper mapper =
        new JdbcSourceRowMapper(valueMappings, schemaReference, sourceTableSchema, shardId);

    // Assert rows match column values
    try (Statement statement = conn.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT * FROM " + table);
      int i = 0;
      while (rs.next()) {
        Column expectedColumn = columns.get(i);

        // Non-null value
        SourceRow sourceRow = mapper.mapRow(rs);

        assertNotNull(sourceRow);
        assertEquals(schemaReference, sourceRow.sourceSchemaReference());
        assertEquals(table, sourceRow.tableName());
        assertEquals(shardId, sourceRow.shardId());
        assertColumnEquals(
            "Failed for column: " + expectedColumn.name + " for value index: " + i,
            expectedColumn.mappedValue,
            sourceRow.getPayload().get(i));

        // Null value
        rs.next();
        sourceRow = mapper.mapRow(rs);

        assertNotNull(sourceRow);
        assertNull(sourceRow.getPayload().get(i));

        i++;
      }
    }
  }

  @Test
  public void testMapPostgreSQLRows() throws SQLException {
    String namespace = "public";
    String dbName = "db";
    String table = "pg_test_table";
    String shardId = "shard1";
    JdbcValueMappingsProvider valueMappings = new PostgreSQLJdbcValueMappings();
    MapperType mapperType = MapperType.POSTGRESQL;
    ImmutableList<Column> columns = postgreSQLColumns();

    // Create table
    createTableFrom(table, columns);
    for (Column column : columns) {
      // Insert column with a non-null value followed by a null value
      populateTable(table, column);
    }
    SourceTableSchema sourceTableSchema = sourceTableSchemaFrom(table, mapperType, columns);
    SourceSchemaReference schemaReference =
        SchemaTestUtils.generateSchemaReference(namespace, dbName);
    JdbcSourceRowMapper mapper =
        new JdbcSourceRowMapper(valueMappings, schemaReference, sourceTableSchema, shardId);

    // Assert rows match column values
    try (Statement statement = conn.createStatement()) {
      ResultSet rs = statement.executeQuery("SELECT * FROM " + table);
      int i = 0;
      while (rs.next()) {
        Column expectedColumn = columns.get(i);

        // Non-null value
        SourceRow sourceRow = mapper.mapRow(rs);

        assertNotNull(sourceRow);
        assertEquals(schemaReference, sourceRow.sourceSchemaReference());
        assertEquals(table, sourceRow.tableName());
        assertEquals(shardId, sourceRow.shardId());

        assertColumnEquals(
            "Failed for column: " + expectedColumn.name + " for value index: " + i,
            expectedColumn.mappedValue,
            sourceRow.getPayload().get(i));

        // Null value
        rs.next();
        sourceRow = mapper.mapRow(rs);

        assertNotNull(sourceRow);
        assertNull(sourceRow.getPayload().get(i));

        i++;
      }
    }
  }

  // Add test case for shard id
  @Test
  public void testMapRowException() {
    String testTable = "test_table";

    var testCols = mySQLColumns();
    var sourceTableSchemaBuilder =
        SourceTableSchema.builder(MapperType.MYSQL).setTableName(testTable);
    testCols.forEach(
        column ->
            sourceTableSchemaBuilder.addSourceColumnNameToSourceColumnType(
                column.name, column.sourceColumnType));
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
        SourceTableSchema.builder(MapperType.MYSQL)
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

  private static ImmutableList<Column> mySQLColumns() {
    // Unfortunately Derby supports only UTC.
    // TODO (vardhanvthigle): Verify time zone conversion on actual DB.
    java.sql.Timestamp timestamp = Timestamp.valueOf("2024-05-02 18:48:05.123456");

    return ImmutableList.<Column>builder()
        .add(
            Column.builder()
                .derbyColumnType("BIGINT")
                .sourceColumnType("BIGINT UNSIGNED", new Long[] {20L, 0L})
                .inputValue(12345L)
                .mappedValue(ByteBuffer.wrap(new byte[] {(byte) 0x30, (byte) 0x39}))
                .build())
        .add(
            Column.builder()
                .derbyColumnType("BIGINT")
                .sourceColumnType("BIGINT")
                .mappedValue(12345L)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("CHAR(4) FOR BIT DATA") // Derby mapping for Binary
                .sourceColumnType("BINARY", new Long[] {4L})
                .inputValue(new byte[] {0x65, 0x66})
                .mappedValue(
                    new String(
                        new char[] {'6', '5', '6', '6', /*space-padding*/ '2', '0', '2', '0'}))
                .build())
        .add(
            Column.builder()
                .derbyColumnType("CHAR(8) FOR BIT DATA") // Derby mapping for Bit
                .sourceColumnType("BIT")
                .inputValue(ByteBuffer.allocate(8).putLong(5L).array())
                .mappedValue(5L)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("BLOB")
                .sourceColumnType("BLOB", new Long[] {10L})
                .inputValue(new byte[] {0x65, 0x66})
                .mappedValue("6566")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("BOOLEAN")
                .sourceColumnType("BOOL", new Long[] {})
                .inputValue(true)
                .mappedValue(1)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("CHAR(10)")
                .sourceColumnType("CHAR", new Long[] {10L})
                .inputValue("test")
                .mappedValue("test      ")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("DATE")
                .sourceColumnType("DATE")
                .inputValue(java.sql.Date.valueOf("2024-05-02"))
                .mappedValue(java.sql.Date.valueOf("2024-05-02").getTime() * 1000)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("TIMESTAMP") // Derby mapping for datetime
                .sourceColumnType("DATETIME")
                .inputValue(timestamp)
                .mappedValue(
                    new GenericRecordBuilder(DateTime.SCHEMA)
                        .set(DateTime.DATE_FIELD_NAME, 19845)
                        .set(DateTime.TIME_FIELD_NAME, 67685123456L)
                        .build())
                .build())
        .add(
            Column.builder()
                .derbyColumnType("DECIMAL(8,2)")
                .sourceColumnType("DECIMAL", new Long[] {8L, 2L})
                .inputValue(123456.78)
                .mappedValue(ByteBuffer.allocate(4).putInt(12345678).rewind())
                .build())
        .add(
            Column.builder()
                .derbyColumnType("DOUBLE")
                .sourceColumnType("DOUBLE")
                .mappedValue(678.12)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(20)") // Derby does not support enum
                .sourceColumnType("ENUM")
                .mappedValue("Books")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("FLOAT")
                .sourceColumnType("FLOAT")
                .mappedValue(145.67f)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("INTEGER")
                .sourceColumnType("INTEGER")
                .mappedValue(42)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("INTEGER") // Derby does not support unsigned
                .sourceColumnType("INTEGER UNSIGNED")
                .inputValue(2_147_483_647)
                .mappedValue(2_147_483_647L)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)") // Derby does not support json
                .sourceColumnType("JSON")
                .mappedValue(
                    "{\"author\": \"Stephen Hawking\", \"title\": \"A Brief History of Time\", \"subject\": \"Physics\"}")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("BLOB")
                .sourceColumnType("LONGBLOB")
                .inputValue(new byte[] {0x65, 0x66})
                .mappedValue("6566")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(10)")
                .sourceColumnType("LONGTEXT")
                .mappedValue("test")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("BLOB")
                .sourceColumnType("MEDIUMBLOB", new Long[] {10L})
                .inputValue(new byte[] {0x65, 0x66})
                .mappedValue("6566")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("INT")
                .sourceColumnType("MEDIUMINT")
                .mappedValue(42)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(10)")
                .sourceColumnType("MEDIUMTEXT")
                .mappedValue("text")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(10)")
                .sourceColumnType("SET")
                .mappedValue("New")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("INT")
                .sourceColumnType("SMALLINT")
                .mappedValue(42)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(10)")
                .sourceColumnType("TEXT")
                .mappedValue("test")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("TIME")
                .sourceColumnType("TIME")
                .inputValue("23:09:02") // Derby supports only time of the day */
                .mappedValue(83342000000L)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("TIMESTAMP")
                .sourceColumnType("TIMESTAMP")
                .inputValue(timestamp)
                .mappedValue(1714675685123456L)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("BLOB")
                .sourceColumnType("TINYBLOB")
                .inputValue(new byte[] {0x65, 0x66})
                .mappedValue("6566")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("SMALLINT")
                .sourceColumnType("TINYINT")
                .mappedValue(42)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(10)")
                .sourceColumnType("TINYTEXT")
                .mappedValue("test")
                .build())
        .add(
            Column.builder()
                .name("varbinary_col")
                .derbyColumnType("VARCHAR(4) FOR BIT DATA") // Derby mapping for Binary
                .sourceColumnType("BINARY", new Long[] {4L})
                .inputValue(new byte[] {0x65, 0x66})
                .mappedValue("6566")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(4)")
                .sourceColumnType("VARCHAR", new Long[] {4L})
                .mappedValue("test")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("BIGINT")
                .sourceColumnType("YEAR")
                .mappedValue(2024)
                .build())
        .build();
  }

  private ImmutableList<Column> postgreSQLColumns() {
    return ImmutableList.<Column>builder()
        .add(
            Column.builder()
                .derbyColumnType("BIGINT")
                .sourceColumnType("BIGINT")
                .mappedValue(12345L)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("BIGINT")
                .sourceColumnType("BIGSERIAL")
                .mappedValue(1L)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("CHAR(8) FOR BIT DATA")
                .sourceColumnType("BIT")
                .mappedValue(ByteBuffer.allocate(8).putLong(Byte.MAX_VALUE).array())
                .build())
        .add(
            Column.builder()
                .derbyColumnType("CHAR(16) FOR BIT DATA")
                .sourceColumnType("BIT VARYING")
                .mappedValue(ByteBuffer.allocate(16).putLong(Short.MAX_VALUE).array())
                .build())
        .add(
            Column.builder()
                .derbyColumnType("BOOLEAN")
                .sourceColumnType("BOOL")
                .mappedValue(false)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("BOOLEAN")
                .sourceColumnType("BOOLEAN")
                .mappedValue(true)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("BOX")
                .mappedValue("(1, 2), (3, 4)")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("CHAR(64) FOR BIT DATA")
                .sourceColumnType("BYTEA")
                .mappedValue(ByteBuffer.allocate(64).putLong(Long.MAX_VALUE).array())
                .build())
        .add(
            Column.builder()
                .derbyColumnType("CHAR(1)")
                .sourceColumnType("CHAR")
                .mappedValue("a")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("CHAR(1)")
                .sourceColumnType("CHARACTER")
                .mappedValue("b")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("CHARACTER VARYING", new Long[] {100L})
                .mappedValue("character varying value")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("CIDR")
                .mappedValue("192.168.100.128/25")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("CIRCLE")
                .mappedValue("(1, 2), 3)")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("CITEXT")
                .mappedValue("ci text")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("DATE")
                .sourceColumnType("DATE")
                .inputValue(java.sql.Date.valueOf("2024-08-30"))
                .mappedValue((int) java.sql.Date.valueOf("2024-08-30").toLocalDate().toEpochDay())
                .build())
        .add(
            Column.builder()
                .derbyColumnType("DECIMAL(9,3)")
                .sourceColumnType("DECIMAL", new Long[] {9L, 3L})
                .inputValue(123456.789)
                .mappedValue("123456.789")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("DOUBLE")
                .sourceColumnType("DOUBLE PRECISION")
                .mappedValue(1.2345D)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("ENUM")
                .mappedValue("ENUM VALUE")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("DOUBLE")
                .sourceColumnType("FLOAT4")
                .mappedValue(1.23F)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("DOUBLE")
                .sourceColumnType("FLOAT8")
                .mappedValue(1.2345D)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("INET")
                .mappedValue("192.168.1.0/24")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("INTEGER")
                .sourceColumnType("INT")
                .mappedValue(Integer.MAX_VALUE)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("INTEGER")
                .sourceColumnType("INTEGER")
                .mappedValue(Integer.MAX_VALUE)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("SMALLINT")
                .sourceColumnType("INT2")
                .inputValue(Short.MAX_VALUE)
                .mappedValue((int) Short.MAX_VALUE)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("INTEGER")
                .sourceColumnType("INT4")
                .mappedValue(Integer.MAX_VALUE)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("BIGINT")
                .sourceColumnType("INT8")
                .mappedValue(Long.MAX_VALUE)
                .build())
        // TODO(thiagotnunes): INTERVAL
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("JSON")
                .mappedValue("{\"key\":\"value\"}")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("JSONB")
                .mappedValue("{\"key\":\"value\"}")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("LINE")
                .mappedValue("{1,2,3}")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("LSEG")
                .mappedValue("[(1,2),(3,4)]")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("MACADDR")
                .mappedValue("08:00:2b:01:02:03")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("MACADDR8")
                .mappedValue("08:00:2b:01:02:03:04:05")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("MONEY")
                .inputValue("1.23")
                .mappedValue(1.23D)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("DECIMAL(9,3)")
                .sourceColumnType("NUMERIC", new Long[] {9L, 3L})
                .inputValue(123456.789)
                .mappedValue("123456.789")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("BIGINT")
                .sourceColumnType("OID")
                .mappedValue(1000L)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("PATH")
                .mappedValue("[(1,2),(3,4),(5,6)]")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("PG_LSN")
                .mappedValue("123/0")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("PG_SNAPSHOT")
                .mappedValue("795:799:795,797")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("POINT")
                .mappedValue("(1,2)")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("POLYGON")
                .mappedValue("((1,2),(3,4))")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("DOUBLE")
                .sourceColumnType("REAL")
                .mappedValue(1.23F)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("INTEGER")
                .sourceColumnType("SERIAL")
                .mappedValue(Integer.MAX_VALUE)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("SMALLINT")
                .sourceColumnType("SERIAL2")
                .inputValue(Short.MAX_VALUE)
                .mappedValue((int) Short.MAX_VALUE)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("INTEGER")
                .sourceColumnType("SERIAL4")
                .mappedValue(Integer.MAX_VALUE)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("BIGINT")
                .sourceColumnType("SERIAL8")
                .mappedValue(Long.MAX_VALUE)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("SMALLINT")
                .sourceColumnType("SMALLINT")
                .inputValue(Short.MAX_VALUE)
                .mappedValue((int) Short.MAX_VALUE)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("SMALLINT")
                .sourceColumnType("SMALLSERIAL")
                .inputValue(Short.MAX_VALUE)
                .mappedValue((int) Short.MAX_VALUE)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("TEXT", new Long[] {100L})
                .mappedValue("text value")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("TIME")
                .inputValue("01:02:03.123456")
                .mappedValue(3723123456L)
                .build())
        .add(
            Column.builder()
                .name("time_00")
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("TIME")
                .inputValue("00:00:00")
                .mappedValue(0L)
                .build())
        .add(
            Column.builder()
                .name("time_24")
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("TIME")
                .inputValue("24:00:00")
                .mappedValue(0L)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("TIME WITHOUT TIME ZONE")
                .inputValue("01:02:03")
                .mappedValue(3723000000L)
                .build())
        .add(
            Column.builder()
                .name("timetz_hour_offset")
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("TIMETZ")
                .inputValue("01:02:03-05")
                .mappedValue(
                    new GenericRecordBuilder(TimeTz.SCHEMA)
                        .set(TimeTz.TIME_FIELD_NAME, 3723000000L)
                        .set(TimeTz.OFFSET_FIELD_NAME, -18000000L)
                        .build())
                .build())
        .add(
            Column.builder()
                .name("timetz_hour_minute_offset")
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("TIMETZ")
                .inputValue("01:02:03-05:00")
                .mappedValue(
                    new GenericRecordBuilder(TimeTz.SCHEMA)
                        .set(TimeTz.TIME_FIELD_NAME, 3723000000L)
                        .set(TimeTz.OFFSET_FIELD_NAME, -18000000L)
                        .build())
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("TIME WITH TIME ZONE")
                .inputValue("01:02:03.123456-05:00")
                .mappedValue(
                    new GenericRecordBuilder(TimeTz.SCHEMA)
                        .set(TimeTz.TIME_FIELD_NAME, 3723123456L)
                        .set(TimeTz.OFFSET_FIELD_NAME, -18000000L)
                        .build())
                .build())
        .add(
            Column.builder()
                .derbyColumnType("TIMESTAMP")
                .sourceColumnType("TIMESTAMP")
                .inputValue(Timestamp.valueOf("1970-01-02 01:02:03.123456"))
                .mappedValue(90123123456L)
                .build())
        .add(
            Column.builder()
                .derbyColumnType("TIMESTAMP")
                .sourceColumnType("TIMESTAMP WITHOUT TIME ZONE")
                .inputValue(Timestamp.valueOf("1970-01-02 01:02:03"))
                .mappedValue(90123000000L)
                .build())
        .add(
            Column.builder()
                .name("timestamptz_hour_offset")
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("TIMESTAMPTZ")
                .inputValue("1970-01-02 01:02:03.123456-05")
                .mappedValue(
                    new GenericRecordBuilder(TimeStampTz.SCHEMA)
                        .set(TimeStampTz.TIMESTAMP_FIELD_NAME, 108123123456L)
                        .set(TimeStampTz.OFFSET_FIELD_NAME, -18000000L)
                        .build())
                .build())
        .add(
            Column.builder()
                .name("timestamptz_hour_minute_offset")
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("TIMESTAMPTZ")
                .inputValue("1970-01-02 01:02:03.123456-05:00")
                .mappedValue(
                    new GenericRecordBuilder(TimeStampTz.SCHEMA)
                        .set(TimeStampTz.TIMESTAMP_FIELD_NAME, 108123123456L)
                        .set(TimeStampTz.OFFSET_FIELD_NAME, -18000000L)
                        .build())
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("TIMESTAMP WITH TIME ZONE")
                .inputValue("1970-01-02 01:02:03.12345+01:00")
                .mappedValue(
                    new GenericRecordBuilder(TimeStampTz.SCHEMA)
                        .set(TimeStampTz.TIMESTAMP_FIELD_NAME, 86523123450L)
                        .set(TimeStampTz.OFFSET_FIELD_NAME, 3600000L)
                        .build())
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("TSQUERY")
                .mappedValue("fat & rat")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("TSVECTOR")
                .mappedValue("a fat cat sat on a mat and ate a fat rat")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("TXID_SNAPSHOT")
                .mappedValue("10:30:10,14,15")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("UUID")
                .mappedValue("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("CHAR(32) FOR BIT DATA")
                .sourceColumnType("VARBIT")
                .mappedValue(ByteBuffer.allocate(32).putLong(Integer.MAX_VALUE).array())
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("VARCHAR", new Long[] {100L})
                .mappedValue("varchar value")
                .build())
        .add(
            Column.builder()
                .derbyColumnType("VARCHAR(100)")
                .sourceColumnType("XML")
                .mappedValue("<test>123</test>")
                .build())
        .build();
  }

  private void createTableFrom(String table, ImmutableList<Column> columns) throws SQLException {
    String columnsDdl =
        columns.stream()
            .map(c -> c.name + " " + c.derbyColumnType)
            .collect(Collectors.joining(", "));
    try (Statement statement = conn.createStatement()) {
      statement.executeUpdate("CREATE TABLE " + table + " (" + columnsDdl + ")");
      conn.commit();
    }
  }

  private void populateTable(String table, Column column) throws SQLException {
    String insertQuery = "INSERT INTO " + table + " (" + column.name + ") VALUES (?)";
    try (PreparedStatement statement = conn.prepareStatement(insertQuery)) {
      statement.setObject(1, column.inputValue);
      statement.executeUpdate();

      statement.setObject(1, null);
      statement.executeUpdate();
      conn.commit();
    }
  }

  private SourceTableSchema sourceTableSchemaFrom(
      String table, MapperType mapperType, ImmutableList<Column> columns) {
    SourceTableSchema.Builder builder = SourceTableSchema.builder(mapperType).setTableName(table);
    columns.forEach(
        column ->
            builder.addSourceColumnNameToSourceColumnType(column.name, column.sourceColumnType));
    return builder.build();
  }
}

class Column {

  final String name;
  final String derbyColumnType;
  final SourceColumnType sourceColumnType;
  final Object inputValue;
  final Object mappedValue;

  public static Builder builder() {
    return new Builder();
  }

  public Column(
      String name,
      String derbyColumnType,
      SourceColumnType sourceColumnType,
      Object inputValue,
      Object mappedValue) {
    this.name = name;
    this.derbyColumnType = derbyColumnType;
    this.sourceColumnType = sourceColumnType;
    this.inputValue = inputValue;
    this.mappedValue = mappedValue;
  }

  public static class Builder {

    private String name;
    private String derbyColumnType;
    private SourceColumnType sourceColumnType;
    private Object inputValue;
    private Object mappedValue;

    Builder name(String value) {
      this.name = value;
      return this;
    }

    Builder derbyColumnType(String value) {
      this.derbyColumnType = value;
      return this;
    }

    Builder sourceColumnType(String value) {
      return sourceColumnType(value, null);
    }

    Builder sourceColumnType(String type, Long[] mods) {
      this.sourceColumnType = new SourceColumnType(type, mods, null);
      return this;
    }

    Builder inputValue(Object value) {
      this.inputValue = value;
      return this;
    }

    Builder mappedValue(Object value) {
      this.mappedValue = value;
      return this;
    }

    Column build() {
      if (derbyColumnType == null) {
        throw new IllegalStateException("Derby column type must be defined for column");
      }
      if (sourceColumnType == null) {
        throw new IllegalStateException("Source column type must be defined for column");
      }
      if (mappedValue == null) {
        throw new IllegalStateException("Mapped value must be defined for column");
      }
      if (name == null) {
        name = String.join("_", sourceColumnType.getName().toLowerCase().split(" "));
        name += "_col";
      }
      if (inputValue == null) {
        inputValue = mappedValue;
      }
      return new Column(name, derbyColumnType, sourceColumnType, inputValue, mappedValue);
    }
  }
}
