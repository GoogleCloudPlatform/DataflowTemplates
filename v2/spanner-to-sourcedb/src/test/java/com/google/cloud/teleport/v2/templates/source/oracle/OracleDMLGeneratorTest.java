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
package com.google.cloud.teleport.v2.templates.source.oracle;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceDatabaseType;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.templates.exceptions.InvalidDMLGenerationException;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.source.oracle.OracleJdbcDao.OracleBindParameter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class OracleDMLGeneratorTest {

  private final OracleDMLGenerator generator = new OracleDMLGenerator();

  private static SourceColumn oracleCol(String name, String type, boolean isGenerated) {
    return SourceColumn.builder(SourceDatabaseType.ORACLE)
        .name(name)
        .type(type)
        .isNullable(true)
        .isGenerated(isGenerated)
        .build();
  }

  private static SourceSchema singleTableSchema(
      String tableName, List<SourceColumn> cols, List<String> pks) {
    SourceTable table =
        SourceTable.builder(SourceDatabaseType.ORACLE)
            .name(tableName)
            .schema("HR")
            .columns(ImmutableList.copyOf(cols))
            .primaryKeyColumns(ImmutableList.copyOf(pks))
            .build();
    return SourceSchema.builder(SourceDatabaseType.ORACLE)
        .databaseName("ORACLE")
        .tables(ImmutableMap.of(tableName, table))
        .build();
  }

  @Test
  public void testUpsertSqlIsIdenticalAcrossNonNullAndNullPermutations() {
    Ddl ddl =
        Ddl.builder()
            .createTable("EMP")
            .column("ID")
            .int64()
            .notNull()
            .endColumn()
            .column("NAME")
            .string()
            .max()
            .endColumn()
            .column("BIO")
            .string()
            .max()
            .endColumn()
            .column("AVATAR")
            .bytes()
            .max()
            .endColumn()
            .primaryKey()
            .asc("ID")
            .end()
            .endTable()
            .build();

    SourceSchema sourceSchema =
        singleTableSchema(
            "EMP",
            List.of(
                oracleCol("ID", "NUMBER(19)", false),
                oracleCol("NAME", "VARCHAR2(100 CHAR)", false),
                oracleCol("BIO", "CLOB", false),
                oracleCol("AVATAR", "BLOB", false)),
            List.of("ID"));
    ISchemaMapper mapper = new IdentityMapper(ddl);

    byte[] avatarBytes = "hello-blob".getBytes(StandardCharsets.UTF_8);
    String largeClob = "x".repeat(8000);

    JSONObject keys1 = new JSONObject().put("ID", "42");
    JSONObject vals1 =
        new JSONObject()
            .put("NAME", "O'Brien\u0000")
            .put("BIO", largeClob)
            .put("AVATAR", Base64.getEncoder().encodeToString(avatarBytes));

    DMLGeneratorResponse r1 =
        generator.getDMLStatement(
            new DMLGeneratorRequest.Builder("INSERT", "EMP", vals1, keys1, "+00:00")
                .setSchemaMapper(mapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());

    // Second row has every nullable column set to JSONObject.NULL
    JSONObject keys2 = new JSONObject().put("ID", "43");
    JSONObject vals2 =
        new JSONObject()
            .put("NAME", JSONObject.NULL)
            .put("BIO", JSONObject.NULL)
            .put("AVATAR", JSONObject.NULL);

    DMLGeneratorResponse r2 =
        generator.getDMLStatement(
            new DMLGeneratorRequest.Builder("UPDATE", "EMP", vals2, keys2, "+00:00")
                .setSchemaMapper(mapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());

    String expectedSql =
        "MERGE INTO \"EMP\" t "
            + "USING (SELECT TO_NUMBER(?) AS \"ID\", ? AS \"NAME\", ? AS \"BIO\", ? AS \"AVATAR\" FROM DUAL) s "
            + "ON (t.\"ID\" = s.\"ID\") "
            + "WHEN MATCHED THEN UPDATE SET t.\"NAME\" = s.\"NAME\", t.\"BIO\" = s.\"BIO\", t.\"AVATAR\" = s.\"AVATAR\" "
            + "WHEN NOT MATCHED THEN INSERT (\"ID\", \"NAME\", \"BIO\", \"AVATAR\") "
            + "VALUES (s.\"ID\", s.\"NAME\", s.\"BIO\", s.\"AVATAR\")";

    assertEquals(expectedSql, r1.getDmlStatement());
    assertEquals(expectedSql, r2.getDmlStatement());

    List<Object> p1 = r1.getPreparedStatementParameters();
    assertEquals(4, p1.size());
    assertEquals(new BigDecimal("42"), p1.get(0));
    assertEquals("O'Brien", p1.get(1)); // null byte stripped, apostrophe preserved raw for ? bind
    assertEquals(new OracleBindParameter(Types.CLOB, largeClob), p1.get(2));
    OracleBindParameter blobParam = (OracleBindParameter) p1.get(3);
    assertEquals(Types.BLOB, blobParam.sqlType());
    assertArrayEquals(avatarBytes, (byte[]) blobParam.value());

    List<Object> p2 = r2.getPreparedStatementParameters();
    assertEquals(4, p2.size());
    assertEquals(new BigDecimal("43"), p2.get(0));
    assertEquals(new OracleBindParameter(Types.VARCHAR, null), p2.get(1));
    assertEquals(new OracleBindParameter(Types.CLOB, null), p2.get(2));
    assertEquals(new OracleBindParameter(Types.BLOB, null), p2.get(3));
  }

  @Test
  public void testAllOracleDataTypesAndSpecialFloatValues() {
    Ddl ddl =
        Ddl.builder()
            .createTable("ALL_TYPES")
            .column("ID")
            .int64()
            .notNull()
            .endColumn()
            .column("NV_COL")
            .string()
            .max()
            .endColumn()
            .column("NC_COL")
            .string()
            .max()
            .endColumn()
            .column("BF_COL")
            .float32()
            .endColumn()
            .column("BD_COL")
            .float64()
            .endColumn()
            .column("BOOL_NUM")
            .bool()
            .endColumn()
            .column("BOOL_VC")
            .bool()
            .endColumn()
            .column("DT_COL")
            .date()
            .endColumn()
            .column("TS_COL")
            .timestamp()
            .endColumn()
            .column("TSTZ_COL")
            .timestamp()
            .endColumn()
            .column("TSLTZ_COL")
            .timestamp()
            .endColumn()
            .column("RAW_COL")
            .bytes()
            .max()
            .endColumn()
            .column("IYM_COL")
            .string()
            .max()
            .endColumn()
            .column("ARR_COL")
            .type(Type.array(Type.string()))
            .endColumn()
            .primaryKey()
            .asc("ID")
            .end()
            .endTable()
            .build();

    SourceSchema sourceSchema =
        singleTableSchema(
            "ALL_TYPES",
            List.of(
                oracleCol("ID", "NUMBER(19)", false),
                oracleCol("NV_COL", "NVARCHAR2(100)", false),
                oracleCol("NC_COL", "NCLOB", false),
                oracleCol("BF_COL", "BINARY_FLOAT", false),
                oracleCol("BD_COL", "BINARY_DOUBLE", false),
                oracleCol("BOOL_NUM", "NUMBER(1)", false),
                oracleCol("BOOL_VC", "VARCHAR2(5)", false),
                oracleCol("DT_COL", "DATE", false),
                oracleCol("TS_COL", "TIMESTAMP(6)", false),
                oracleCol("TSTZ_COL", "TIMESTAMP(6) WITH TIME ZONE", false),
                oracleCol("TSLTZ_COL", "TIMESTAMP(6) WITH LOCAL TIME ZONE", false),
                oracleCol("RAW_COL", "RAW(32767)", false),
                oracleCol("IYM_COL", "INTERVAL YEAR(9) TO MONTH", false),
                oracleCol("ARR_COL", "VARCHAR2(400)", false)),
            List.of("ID"));
    ISchemaMapper mapper = new IdentityMapper(ddl);

    byte[] rawPayload = new byte[5000];
    rawPayload[0] = 0x48;
    rawPayload[4999] = 0x6F;

    JSONObject keys = new JSONObject().put("ID", "1");
    JSONObject vals =
        new JSONObject()
            .put("NV_COL", "こんにちは")
            .put("NC_COL", "nclob-val")
            .put("BF_COL", "NaN")
            .put("BD_COL", "-Infinity")
            .put("BOOL_NUM", true)
            .put("BOOL_VC", false)
            .put("DT_COL", "2026-11-23")
            .put("TS_COL", "2026-11-23T07:00:00.123456Z")
            .put("TSTZ_COL", "2026-11-23T07:00:00.123456Z")
            .put("TSLTZ_COL", "2026-11-23T07:00:00.123456Z")
            .put("RAW_COL", Base64.getEncoder().encodeToString(rawPayload))
            .put("IYM_COL", "+000000002-06")
            .put("ARR_COL", new JSONArray(List.of("a", "b")));

    DMLGeneratorResponse resp =
        generator.getDMLStatement(
            new DMLGeneratorRequest.Builder("INSERT", "ALL_TYPES", vals, keys, "+05:30")
                .setSchemaMapper(mapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());

    String sql = resp.getDmlStatement();
    assertTrue(sql.contains("? AS \"NV_COL\""));
    assertTrue(sql.contains("? AS \"NC_COL\""));
    assertTrue(sql.contains("TO_BINARY_FLOAT(?) AS \"BF_COL\""));
    assertTrue(sql.contains("TO_BINARY_DOUBLE(?) AS \"BD_COL\""));
    assertTrue(sql.contains("TO_NUMBER(?) AS \"BOOL_NUM\""));
    assertTrue(sql.contains("? AS \"BOOL_VC\""));
    assertTrue(sql.contains("TO_DATE(?, 'YYYY-MM-DD') AS \"DT_COL\""));
    assertTrue(
        sql.contains(
            "CAST(FROM_TZ(TO_TIMESTAMP(?, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF\"Z\"'), 'UTC') AT TIME ZONE '+05:30' AS TIMESTAMP) AS \"TS_COL\""));
    // Time zone aware types keep the exact instant: the source offset is applied only to TIMESTAMP.
    assertTrue(
        sql.contains(
            ", FROM_TZ(TO_TIMESTAMP(?, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF\"Z\"'), 'UTC') AS \"TSTZ_COL\""));
    assertTrue(
        sql.contains(
            ", FROM_TZ(TO_TIMESTAMP(?, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF\"Z\"'), 'UTC') AS \"TSLTZ_COL\""));
    assertTrue(sql.contains("? AS \"RAW_COL\""));
    assertTrue(sql.contains("TO_YMINTERVAL(?) AS \"IYM_COL\""));

    List<Object> params = resp.getPreparedStatementParameters();
    assertEquals(14, params.size());
    assertEquals(new BigDecimal("1"), params.get(0));
    assertEquals(new OracleBindParameter(Types.NVARCHAR, "こんにちは"), params.get(1));
    assertEquals(new OracleBindParameter(Types.NCLOB, "nclob-val"), params.get(2));
    assertEquals("NaN", params.get(3));
    assertEquals("-INF", params.get(4));
    assertEquals(BigDecimal.ONE, params.get(5));
    assertEquals("false", params.get(6));
    assertEquals("2026-11-23", params.get(7));
    OracleBindParameter rawBind = (OracleBindParameter) params.get(11);
    assertEquals(Types.VARBINARY, rawBind.sqlType());
    assertArrayEquals(rawPayload, (byte[]) rawBind.value());
    assertEquals("+000000002-06", params.get(12));
    assertEquals("a,b", params.get(13));
  }

  @Test
  public void testDeleteCompositePrimaryKey() {
    Ddl ddl =
        Ddl.builder()
            .createTable("ORDER_ITEMS")
            .column("ORDER_ID")
            .int64()
            .notNull()
            .endColumn()
            .column("SKU")
            .string()
            .max()
            .notNull()
            .endColumn()
            .column("QTY")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("ORDER_ID")
            .asc("SKU")
            .end()
            .endTable()
            .build();

    SourceSchema sourceSchema =
        singleTableSchema(
            "ORDER_ITEMS",
            List.of(
                oracleCol("ORDER_ID", "NUMBER(19)", false),
                oracleCol("SKU", "VARCHAR2(64)", false),
                oracleCol("QTY", "NUMBER(10)", false)),
            List.of("ORDER_ID", "SKU"));

    JSONObject keys = new JSONObject().put("ORDER_ID", "500").put("SKU", "ABC-1");
    DMLGeneratorResponse deleteResp =
        generator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    "DELETE", "ORDER_ITEMS", new JSONObject().put("QTY", "10"), keys, "+00:00")
                .setSchemaMapper(new IdentityMapper(ddl))
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());

    assertEquals(
        "DELETE FROM \"ORDER_ITEMS\" WHERE  \"ORDER_ID\" = TO_NUMBER(?) AND  \"SKU\" = ?",
        deleteResp.getDmlStatement());
    assertEquals(
        List.of(new BigDecimal("500"), "ABC-1"), deleteResp.getPreparedStatementParameters());
  }

  @Test
  public void testGeneratedVirtualPkAndHybridCustomTransformation() {
    Ddl ddl =
        Ddl.builder()
            .createTable("GEN_TBL")
            .column("FIRST_NAME")
            .string()
            .max()
            .endColumn()
            .column("LEGACY_SYS")
            .string()
            .max()
            .endColumn()
            .column("CUSTOM_CLOB")
            .string()
            .max()
            .endColumn()
            .column("CUSTOM_BLOB")
            .bytes()
            .max()
            .endColumn()
            .column("CUSTOM_YEAR")
            .int64()
            .endColumn()
            .column("CUSTOM_DATE")
            .date()
            .endColumn()
            .column("CUSTOM_EXPR")
            .string()
            .max()
            .endColumn()
            .column("NON_PK_GEN")
            .string()
            .max()
            .endColumn()
            .column("GEN_PK")
            .string()
            .max()
            .notNull()
            .endColumn()
            .primaryKey()
            .asc("GEN_PK")
            .end()
            .endTable()
            .build();

    SourceSchema sourceSchema =
        singleTableSchema(
            "GEN_TBL",
            List.of(
                oracleCol("FIRST_NAME", "VARCHAR2(50)", false),
                oracleCol("LEGACY_SYS", "VARCHAR2(50)", false),
                oracleCol("CUSTOM_CLOB", "CLOB", false),
                oracleCol("CUSTOM_BLOB", "BLOB", false),
                oracleCol("CUSTOM_YEAR", "NUMBER(10)", false),
                oracleCol("CUSTOM_DATE", "DATE", false),
                oracleCol("CUSTOM_EXPR", "VARCHAR2(100)", false),
                oracleCol("NON_PK_GEN", "VARCHAR2(60)", true),
                oracleCol("GEN_PK", "VARCHAR2(100)", true)),
            List.of("GEN_PK"));

    JSONObject keys = new JSONObject().put("GEN_PK", "ALICE_PK");
    JSONObject vals = new JSONObject().put("FIRST_NAME", "Alice");
    Map<String, Object> customMap = new HashMap<>();
    // Custom transformation values are bound as returned, including SQL-like text.
    customMap.put("LEGACY_SYS", "Website_v1");
    customMap.put("CUSTOM_CLOB", "O'Brien_clob");
    customMap.put("CUSTOM_BLOB", "Hello".getBytes(StandardCharsets.UTF_8));
    customMap.put("CUSTOM_YEAR", 2026);
    customMap.put("CUSTOM_DATE", "2026-11-23");
    customMap.put("CUSTOM_EXPR", "'prefix_' || 'suffix'");

    DMLGeneratorResponse resp =
        generator.getDMLStatement(
            new DMLGeneratorRequest.Builder("INSERT", "GEN_TBL", vals, keys, "+00:00")
                .setSchemaMapper(new IdentityMapper(ddl))
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setCustomTransformationResponse(customMap)
                .build());

    String sql = resp.getDmlStatement();
    assertTrue(
        sql.contains(
            "USING (SELECT ? AS \"FIRST_NAME\", ? AS \"LEGACY_SYS\", ? AS \"CUSTOM_CLOB\", "
                + "? AS \"CUSTOM_BLOB\", TO_NUMBER(?) AS \"CUSTOM_YEAR\", "
                + "TO_DATE(?, 'YYYY-MM-DD') AS \"CUSTOM_DATE\", "
                + "? AS \"CUSTOM_EXPR\", ? AS \"GEN_PK\" FROM DUAL) s"));
    assertTrue(sql.contains("ON (t.\"GEN_PK\" = s.\"GEN_PK\")"));
    assertTrue(
        sql.contains(
            "WHEN NOT MATCHED THEN INSERT (\"FIRST_NAME\", \"LEGACY_SYS\", \"CUSTOM_CLOB\", \"CUSTOM_BLOB\", \"CUSTOM_YEAR\", \"CUSTOM_DATE\", \"CUSTOM_EXPR\") VALUES"));

    List<Object> params = resp.getPreparedStatementParameters();
    assertEquals(8, params.size());
    assertEquals("Alice", params.get(0));
    assertEquals("Website_v1", params.get(1));
    assertEquals(new OracleBindParameter(Types.CLOB, "O'Brien_clob"), params.get(2));
    OracleBindParameter customBlobBind = (OracleBindParameter) params.get(3);
    assertEquals(Types.BLOB, customBlobBind.sqlType());
    assertArrayEquals("Hello".getBytes(StandardCharsets.UTF_8), (byte[]) customBlobBind.value());
    assertEquals(new BigDecimal("2026"), params.get(4));
    assertEquals("2026-11-23", params.get(5));
    assertEquals("'prefix_' || 'suffix'", params.get(6));
    assertEquals("ALICE_PK", params.get(7));
  }

  @Test
  public void testEmptyLobAndBytesValuesBindAsTypedNull() {
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("LOB_EMPTY")
            .column("ID")
            .pgNumeric()
            .notNull()
            .endColumn()
            .column("B_COL")
            .pgBytea()
            .endColumn()
            .column("C_COL")
            .pgVarchar()
            .max()
            .endColumn()
            .column("R_COL")
            .pgBytea()
            .endColumn()
            .column("LR_COL")
            .pgBytea()
            .endColumn()
            .primaryKey()
            .asc("ID")
            .end()
            .endTable()
            .build();

    SourceSchema sourceSchema =
        singleTableSchema(
            "LOB_EMPTY",
            List.of(
                oracleCol("ID", "NUMBER", false),
                oracleCol("B_COL", "BLOB", false),
                oracleCol("C_COL", "CLOB", false),
                oracleCol("R_COL", "RAW(16)", false),
                oracleCol("LR_COL", "LONG RAW", false)),
            List.of("ID"));

    JSONObject keys = new JSONObject().put("ID", 10.5);
    JSONObject vals =
        new JSONObject().put("B_COL", "").put("C_COL", "").put("R_COL", "").put("LR_COL", "");

    DMLGeneratorResponse resp =
        generator.getDMLStatement(
            new DMLGeneratorRequest.Builder("INSERT", "LOB_EMPTY", vals, keys, "+00:00")
                .setSchemaMapper(new IdentityMapper(ddl))
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());

    List<Object> params = resp.getPreparedStatementParameters();
    assertEquals(new BigDecimal("10.5"), params.get(0));
    assertEquals(new OracleBindParameter(Types.BLOB, null), params.get(1));
    assertEquals(new OracleBindParameter(Types.CLOB, null), params.get(2));
    assertEquals(new OracleBindParameter(Types.VARBINARY, null), params.get(3));
    assertEquals(new OracleBindParameter(Types.VARBINARY, null), params.get(4));
  }

  @Test
  public void testValidationErrorsAndHelpers() {
    assertThrows(InvalidDMLGenerationException.class, () -> generator.getDMLStatement(null));

    assertEquals(
        "timestamp with local time zone",
        OracleDMLGenerator.normalizeSourceType("  TIMESTAMP(6)   WITH LOCAL TIME ZONE "));
    assertEquals(
        "TO_NUMBER(?)", OracleDMLGenerator.getColumnValueByType("DEC", "+00:00", "NUMERIC"));
    assertEquals(
        "TO_NUMBER(?)", OracleDMLGenerator.getColumnValueByType("REAL", "+00:00", "FLOAT64"));
    assertEquals(
        "TO_YMINTERVAL(?)",
        OracleDMLGenerator.getColumnValueByType("INTERVAL YEAR(9) TO MONTH", "+00:00", "STRING"));
    assertEquals(
        "TO_DSINTERVAL(?)",
        OracleDMLGenerator.getColumnValueByType(
            "INTERVAL DAY(2) TO SECOND(6)", "+00:00", "STRING"));
    assertEquals("?", OracleDMLGenerator.getColumnValueByType("ROWID", "+00:00", "STRING"));
    assertEquals("?", OracleDMLGenerator.getColumnValueByType("UROWID", "+00:00", "STRING"));
    assertEquals("?", OracleDMLGenerator.getColumnValueByType("LONG", "+00:00", "STRING"));
    assertEquals("HEXTORAW(?)", OracleDMLGenerator.getColumnValueByType("LONG", "+00:00", "BYTES"));
    String utcTimestamp = "FROM_TZ(TO_TIMESTAMP(?, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF\"Z\"'), 'UTC')";
    assertEquals(
        utcTimestamp,
        OracleDMLGenerator.getColumnValueByType("TIMESTAMP(6)", "+00:00", "TIMESTAMP"));
    assertEquals(
        utcTimestamp, OracleDMLGenerator.getColumnValueByType("TIMESTAMP(6)", "Z", "TIMESTAMP"));
    assertEquals(
        utcTimestamp, OracleDMLGenerator.getColumnValueByType("TIMESTAMP(6)", null, "TIMESTAMP"));
  }

  @Test
  public void testBooleanToUnlistedSourceTypeIsConvertedToNumber() {
    Ddl ddl =
        Ddl.builder()
            .createTable("FLAGS")
            .column("ID")
            .int64()
            .notNull()
            .endColumn()
            .column("FLAG")
            .bool()
            .endColumn()
            .primaryKey()
            .asc("ID")
            .end()
            .endTable()
            .build();
    SourceSchema sourceSchema =
        singleTableSchema(
            "FLAGS",
            List.of(oracleCol("ID", "NUMBER", false), oracleCol("FLAG", "bigint", false)),
            List.of("ID"));

    DMLGeneratorResponse resp =
        generator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    "INSERT",
                    "FLAGS",
                    new JSONObject().put("FLAG", true),
                    new JSONObject().put("ID", "7"),
                    "+00:00")
                .setSchemaMapper(new IdentityMapper(ddl))
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());

    // Unlisted source types keep the default true/false -> 1/0 conversion.
    assertEquals(List.of(new BigDecimal("7"), "1"), resp.getPreparedStatementParameters());
  }

  @Test
  public void testFixedCharAndNcharBindAsTypesCharAndNcharWithoutToNcharWrapper() {
    Ddl ddl =
        Ddl.builder()
            .createTable("CHAR_PK_TBL")
            .column("C_PK")
            .string()
            .size(2000)
            .notNull()
            .endColumn()
            .column("NC_PK")
            .string()
            .size(1000)
            .notNull()
            .endColumn()
            .column("NC_COL")
            .string()
            .size(1000)
            .endColumn()
            .primaryKey()
            .asc("C_PK")
            .asc("NC_PK")
            .end()
            .endTable()
            .build();

    SourceSchema sourceSchema =
        singleTableSchema(
            "CHAR_PK_TBL",
            List.of(
                oracleCol("C_PK", "CHAR(2000)", false),
                oracleCol("NC_PK", "NCHAR(1000)", false),
                oracleCol("NC_COL", "NATIONAL CHAR(1000)", false)),
            List.of("C_PK", "NC_PK"));

    JSONObject keys = new JSONObject().put("C_PK", "Hello").put("NC_PK", "世界");
    JSONObject vals = new JSONObject().put("NC_COL", "");

    DMLGeneratorResponse upsertResp =
        generator.getDMLStatement(
            new DMLGeneratorRequest.Builder("INSERT", "CHAR_PK_TBL", vals, keys, "+00:00")
                .setSchemaMapper(new IdentityMapper(ddl))
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    assertTrue(
        upsertResp
            .getDmlStatement()
            .contains("USING (SELECT ? AS \"C_PK\", ? AS \"NC_PK\", ? AS \"NC_COL\" FROM DUAL) s"));
    assertEquals(
        List.of(
            new OracleBindParameter(Types.CHAR, "Hello"),
            new OracleBindParameter(Types.NCHAR, "世界"),
            new OracleBindParameter(Types.NCHAR, null)),
        upsertResp.getPreparedStatementParameters());

    DMLGeneratorResponse deleteResp =
        generator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    "DELETE", "CHAR_PK_TBL", new JSONObject(), keys, "+00:00")
                .setSchemaMapper(new IdentityMapper(ddl))
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    assertEquals(
        "DELETE FROM \"CHAR_PK_TBL\" WHERE  \"C_PK\" = ? AND  \"NC_PK\" = ?",
        deleteResp.getDmlStatement());
    assertEquals(
        List.of(
            new OracleBindParameter(Types.CHAR, "Hello"),
            new OracleBindParameter(Types.NCHAR, "世界")),
        deleteResp.getPreparedStatementParameters());
  }

  @Test
  public void testCustomTransformationNullNumberAndBytesAreBoundAsReturned() {
    Ddl ddl =
        Ddl.builder()
            .createTable("CT")
            .column("ID")
            .int64()
            .notNull()
            .endColumn()
            .column("NAME")
            .string()
            .max()
            .endColumn()
            .column("QTY")
            .int64()
            .endColumn()
            .column("RAW_COL")
            .bytes()
            .max()
            .endColumn()
            .primaryKey()
            .asc("ID")
            .end()
            .endTable()
            .build();
    SourceSchema sourceSchema =
        singleTableSchema(
            "CT",
            List.of(
                oracleCol("ID", "NUMBER", false),
                oracleCol("NAME", "VARCHAR2(50)", false),
                oracleCol("QTY", "NUMBER(10)", false),
                oracleCol("RAW_COL", "RAW(16)", false),
                // Source-only columns (not in Spanner).
                oracleCol("LEGACY_DT", "DATE", false),
                oracleCol("LEGACY_BF", "BINARY_FLOAT", false)),
            List.of("ID"));
    Map<String, Object> customMap = new HashMap<>();
    customMap.put("NAME", null);
    customMap.put("QTY", 42L);
    customMap.put("RAW_COL", new byte[] {0x0A, 0x0B});
    customMap.put("LEGACY_DT", "2026-11-23");
    customMap.put("LEGACY_BF", Float.POSITIVE_INFINITY);

    DMLGeneratorResponse resp =
        generator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    "INSERT", "CT", new JSONObject(), new JSONObject().put("ID", "1"), "+00:00")
                .setSchemaMapper(new IdentityMapper(ddl))
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setCustomTransformationResponse(customMap)
                .build());

    assertTrue(
        resp.getDmlStatement()
            .contains(
                "USING (SELECT TO_NUMBER(?) AS \"ID\", ? AS \"NAME\", TO_NUMBER(?) AS \"QTY\", "
                    + "? AS \"RAW_COL\", TO_DATE(?, 'YYYY-MM-DD') AS \"LEGACY_DT\", "
                    + "TO_BINARY_FLOAT(?) AS \"LEGACY_BF\" FROM DUAL) s"));
    List<Object> params = resp.getPreparedStatementParameters();
    assertEquals(6, params.size());
    assertEquals(new BigDecimal("1"), params.get(0));
    assertEquals(new OracleBindParameter(Types.VARCHAR, null), params.get(1));
    assertEquals(new BigDecimal("42"), params.get(2));
    OracleBindParameter rawBind = (OracleBindParameter) params.get(3);
    assertEquals(Types.VARBINARY, rawBind.sqlType());
    assertArrayEquals(new byte[] {0x0A, 0x0B}, (byte[]) rawBind.value());
    assertEquals("2026-11-23", params.get(4));
    assertEquals("INF", params.get(5));
  }

  @Test
  public void testBytesIntoCharacterColumnsUseHexToRawForValuesAndNulls() {
    Ddl ddl =
        Ddl.builder()
            .createTable("BT")
            .column("ID")
            .int64()
            .notNull()
            .endColumn()
            .column("VC")
            .bytes()
            .max()
            .endColumn()
            .column("NV")
            .bytes()
            .max()
            .endColumn()
            .primaryKey()
            .asc("ID")
            .end()
            .endTable()
            .build();
    SourceSchema sourceSchema =
        singleTableSchema(
            "BT",
            List.of(
                oracleCol("ID", "NUMBER", false),
                oracleCol("VC", "VARCHAR2(100)", false),
                oracleCol("NV", "NVARCHAR2(100)", false)),
            List.of("ID"));
    String helloBase64 =
        Base64.getEncoder().encodeToString("Hello".getBytes(StandardCharsets.UTF_8));

    DMLGeneratorResponse withValues =
        generator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    "INSERT",
                    "BT",
                    new JSONObject().put("VC", helloBase64).put("NV", helloBase64),
                    new JSONObject().put("ID", "1"),
                    "+00:00")
                .setSchemaMapper(new IdentityMapper(ddl))
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    DMLGeneratorResponse withNulls =
        generator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    "INSERT",
                    "BT",
                    new JSONObject().put("VC", JSONObject.NULL).put("NV", JSONObject.NULL),
                    new JSONObject().put("ID", "2"),
                    "+00:00")
                .setSchemaMapper(new IdentityMapper(ddl))
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());

    assertTrue(
        withValues
            .getDmlStatement()
            .contains(
                "USING (SELECT TO_NUMBER(?) AS \"ID\", HEXTORAW(?) AS \"VC\", "
                    + "TO_NCLOB(UTL_RAW.CAST_TO_NVARCHAR2(HEXTORAW(?))) AS \"NV\" FROM DUAL) s"));
    assertEquals(withValues.getDmlStatement(), withNulls.getDmlStatement());
    assertEquals(
        List.of(new BigDecimal("1"), "48656c6c6f", "48656c6c6f"),
        withValues.getPreparedStatementParameters());
    assertEquals(
        List.of(
            new BigDecimal("2"),
            new OracleBindParameter(Types.VARCHAR, null),
            new OracleBindParameter(Types.VARCHAR, null)),
        withNulls.getPreparedStatementParameters());
  }

  @Test
  public void testIntervalLongRowidDecRealStringToBinaryAndInfinityAreBound() {
    Ddl ddl =
        Ddl.builder()
            .createTable("MISC")
            .column("ID")
            .int64()
            .notNull()
            .endColumn()
            .column("DS_COL")
            .string()
            .max()
            .endColumn()
            .column("LONG_COL")
            .string()
            .max()
            .endColumn()
            .column("RID_COL")
            .string()
            .max()
            .endColumn()
            .column("DEC_COL")
            .numeric()
            .endColumn()
            .column("REAL_COL")
            .float64()
            .endColumn()
            .column("BLOB_COL")
            .string()
            .max()
            .endColumn()
            .column("RAW_COL")
            .string()
            .max()
            .endColumn()
            .column("BF_COL")
            .float32()
            .endColumn()
            .primaryKey()
            .asc("ID")
            .end()
            .endTable()
            .build();
    SourceSchema sourceSchema =
        singleTableSchema(
            "MISC",
            List.of(
                oracleCol("ID", "NUMBER", false),
                oracleCol("DS_COL", "INTERVAL DAY(2) TO SECOND(6)", false),
                oracleCol("LONG_COL", "LONG", false),
                oracleCol("RID_COL", "ROWID", false),
                oracleCol("DEC_COL", "DEC", false),
                oracleCol("REAL_COL", "REAL", false),
                oracleCol("BLOB_COL", "BLOB", false),
                oracleCol("RAW_COL", "RAW(16)", false),
                oracleCol("BF_COL", "BINARY_FLOAT", false)),
            List.of("ID"));

    JSONObject vals =
        new JSONObject()
            .put("DS_COL", "2 03:04:05.000000")
            .put("LONG_COL", "long text")
            .put("RID_COL", "AAAR3sAAEAAAACXAAA")
            .put("DEC_COL", "12.50")
            .put("REAL_COL", JSONObject.NULL)
            .put("BLOB_COL", "IA==")
            .put("RAW_COL", "")
            .put("BF_COL", "Infinity");

    DMLGeneratorResponse resp =
        generator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    "INSERT", "MISC", vals, new JSONObject().put("ID", "1"), "+00:00")
                .setSchemaMapper(new IdentityMapper(ddl))
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());

    assertTrue(
        resp.getDmlStatement()
            .contains(
                "USING (SELECT TO_NUMBER(?) AS \"ID\", TO_DSINTERVAL(?) AS \"DS_COL\", "
                    + "? AS \"LONG_COL\", ? AS \"RID_COL\", TO_NUMBER(?) AS \"DEC_COL\", "
                    + "TO_NUMBER(?) AS \"REAL_COL\", ? AS \"BLOB_COL\", ? AS \"RAW_COL\", "
                    + "TO_BINARY_FLOAT(?) AS \"BF_COL\" FROM DUAL) s"));
    List<Object> params = resp.getPreparedStatementParameters();
    assertEquals(9, params.size());
    assertEquals("2 03:04:05.000000", params.get(1));
    assertEquals("long text", params.get(2));
    assertEquals("AAAR3sAAEAAAACXAAA", params.get(3));
    assertEquals(new BigDecimal("12.50"), params.get(4));
    // NULLs of numeric columns use the same bind type as the BigDecimal values.
    assertEquals(new OracleBindParameter(Types.NUMERIC, null), params.get(5));
    // Spanner STRING written to a BLOB / RAW column holds Base64 encoded bytes.
    OracleBindParameter blobBind = (OracleBindParameter) params.get(6);
    assertEquals(Types.BLOB, blobBind.sqlType());
    assertArrayEquals(new byte[] {0x20}, (byte[]) blobBind.value());
    assertEquals(new OracleBindParameter(Types.VARBINARY, null), params.get(7));
    assertEquals("INF", params.get(8));
  }
}
