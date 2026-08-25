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
package com.google.cloud.teleport.v2.templates.oracle;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.cloud.teleport.v2.templates.DataStreamToSpannerITBase;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.OracleSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class OracleDatastreamToSpannerDataTypesIT extends DataStreamToSpannerITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(OracleDatastreamToSpannerDataTypesIT.class);

  private static final String SPANNER_DDL_RESOURCE =
      "oracle/OracleDatastreamToSpannerDataTypesIT/oracle-google_standard_sql-spanner-schema.sql";
  private static final String TABLE_PREFIX = "T_SPRG_";
  private static final List<String> UNSUPPORTED_TYPE_TABLES =
      List.of(
          "T_SPRG_R27_INTERVAL_Y",
          "T_SPRG_R28_INTERVAL_D",
          "T_SPRG_R30_LONG_RAW",
          "T_SPRG_R34_BFILE",
          "T_SPRG_R35_LONG",
          "T_SPRG_R36_ROWID",
          "T_SPRG_R37_UROWID",
          "T_SPRG_R38_BOOLEAN",
          "T_SPRG_R39_JSON",
          "T_SPRG_R40_XMLTYPE",
          "T_SPRG_R41_SDO_GEOMET",
          "T_SPRG_R42_SDO_TOPO_G",
          "T_SPRG_R43_SDO_GEORAS",
          "T_SPRG_R44_VECTOR",
          "T_SPRG_R45_ANYDATA",
          "T_SPRG_R46_ANYTYPE",
          "T_SPRG_R47_ANYDATASET",
          "T_SPRG_R48_URITYPE",
          "T_SPRG_R49_DBURITYPE",
          "T_SPRG_R50_XDBURITYPE",
          "T_SPRG_R51_HTTPURITYP",
          "T_SPRG_R52_EXPRESSION",
          "T_SPRG_R53_ORDAUDIO",
          "T_SPRG_R54_ORDIMAGE",
          "T_SPRG_R55_ORDVIDEO",
          "T_SPRG_R56_ORDDOC",
          "T_SPRG_R57_SI_STILLIM",
          "T_SPRG_R58_SI_COLOR",
          "T_SPRG_R59_SI_AVERAGE",
          "T_SPRG_R60_SI_COLORHI",
          "T_SPRG_R61_SI_POSITIO",
          "T_SPRG_R62_SI_TEXTURE",
          "T_SPRG_R63_SI_FEATURE",
          "T_SPRG_R64_MLSLABEL",
          "T_SPRG_R65_REF",
          "T_SPRG_R66_REF_CURSOR",
          "T_SPRG_R67_VARRAY",
          "T_SPRG_R68_NESTED_TAB",
          "T_SPRG_R69_ASSOCIATIV",
          "T_SPRG_R70_OBJECT_TYP");

  private static boolean initialized = false;
  private static CloudOracleResourceManager oracleSysUser;
  private static CloudOracleResourceManager oracleResourceManager;
  private static SpannerResourceManager spannerResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private static DatastreamResourceManager datastreamResourceManager;

  private static HashSet<OracleDatastreamToSpannerDataTypesIT> testInstances = new HashSet<>();
  private static final String[] ORACLE_DDL_COMMANDS =
      new String[] {
        "CREATE TABLE T_SPRG_R0_VARCHAR2 (ID NUMBER PRIMARY KEY, COL_0 VARCHAR2(255))",
        "ALTER TABLE T_SPRG_R0_VARCHAR2 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R0_VARCHAR2 (ID, COL_0) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R1_VARCHAR (ID NUMBER PRIMARY KEY, COL_1 VARCHAR(255))",
        "ALTER TABLE T_SPRG_R1_VARCHAR ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R1_VARCHAR (ID, COL_1) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R2_CHAR (ID NUMBER PRIMARY KEY, COL_2 CHAR(255))",
        "ALTER TABLE T_SPRG_R2_CHAR ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R2_CHAR (ID, COL_2) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R3_CHARACTER (ID NUMBER PRIMARY KEY, COL_3 CHARACTER(255))",
        "ALTER TABLE T_SPRG_R3_CHARACTER ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R3_CHARACTER (ID, COL_3) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R4_NVARCHAR2 (ID NUMBER PRIMARY KEY, COL_4 NVARCHAR2(255))",
        "ALTER TABLE T_SPRG_R4_NVARCHAR2 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R4_NVARCHAR2 (ID, COL_4) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R5_NCHAR (ID NUMBER PRIMARY KEY, COL_5 NCHAR(255))",
        "ALTER TABLE T_SPRG_R5_NCHAR ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R5_NCHAR (ID, COL_5) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R6_NCHAR_VARY (ID NUMBER PRIMARY KEY, COL_6 NCHAR VARYING(255))",
        "ALTER TABLE T_SPRG_R6_NCHAR_VARY ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R6_NCHAR_VARY (ID, COL_6) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R7_NATIONAL_C (ID NUMBER PRIMARY KEY, COL_7 NATIONAL CHARACTER(255))",
        "ALTER TABLE T_SPRG_R7_NATIONAL_C ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R7_NATIONAL_C (ID, COL_7) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R8_NATIONAL_C (ID NUMBER PRIMARY KEY, COL_8 NATIONAL CHAR(255))",
        "ALTER TABLE T_SPRG_R8_NATIONAL_C ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R8_NATIONAL_C (ID, COL_8) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R9_NATIONAL_C (ID NUMBER PRIMARY KEY, COL_9 NATIONAL CHARACTER"
            + " VARYING(255))",
        "ALTER TABLE T_SPRG_R9_NATIONAL_C ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R9_NATIONAL_C (ID, COL_9) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R10_NATIONAL_C (ID NUMBER PRIMARY KEY, COL_10 NATIONAL CHAR"
            + " VARYING(255))",
        "ALTER TABLE T_SPRG_R10_NATIONAL_C ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R10_NATIONAL_C (ID, COL_10) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R11_NUMBER (ID NUMBER PRIMARY KEY, COL_11 NUMBER)",
        "ALTER TABLE T_SPRG_R11_NUMBER ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R11_NUMBER (ID, COL_11) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R12_NUMERIC (ID NUMBER PRIMARY KEY, COL_12 NUMERIC)",
        "ALTER TABLE T_SPRG_R12_NUMERIC ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R12_NUMERIC (ID, COL_12) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R13_DECIMAL (ID NUMBER PRIMARY KEY, COL_13 DECIMAL)",
        "ALTER TABLE T_SPRG_R13_DECIMAL ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R13_DECIMAL (ID, COL_13) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R14_DEC (ID NUMBER PRIMARY KEY, COL_14 DEC)",
        "ALTER TABLE T_SPRG_R14_DEC ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R14_DEC (ID, COL_14) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R15_FLOAT (ID NUMBER PRIMARY KEY, COL_15 FLOAT)",
        "ALTER TABLE T_SPRG_R15_FLOAT ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R15_FLOAT (ID, COL_15) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R16_DOUBLE_PRE (ID NUMBER PRIMARY KEY, COL_16 DOUBLE PRECISION)",
        "ALTER TABLE T_SPRG_R16_DOUBLE_PRE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R16_DOUBLE_PRE (ID, COL_16) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R17_REAL (ID NUMBER PRIMARY KEY, COL_17 REAL)",
        "ALTER TABLE T_SPRG_R17_REAL ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R17_REAL (ID, COL_17) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R18_BINARY_FLO (ID NUMBER PRIMARY KEY, COL_18 BINARY_FLOAT)",
        "ALTER TABLE T_SPRG_R18_BINARY_FLO ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R18_BINARY_FLO (ID, COL_18) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R19_BINARY_DOU (ID NUMBER PRIMARY KEY, COL_19 BINARY_DOUBLE)",
        "ALTER TABLE T_SPRG_R19_BINARY_DOU ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R19_BINARY_DOU (ID, COL_19) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R20_INTEGER (ID NUMBER PRIMARY KEY, COL_20 INTEGER)",
        "ALTER TABLE T_SPRG_R20_INTEGER ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R20_INTEGER (ID, COL_20) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R21_INT (ID NUMBER PRIMARY KEY, COL_21 INT)",
        "ALTER TABLE T_SPRG_R21_INT ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R21_INT (ID, COL_21) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R22_SMALLINT (ID NUMBER PRIMARY KEY, COL_22 SMALLINT)",
        "ALTER TABLE T_SPRG_R22_SMALLINT ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R22_SMALLINT (ID, COL_22) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R23_DATE (ID NUMBER PRIMARY KEY, COL_23 DATE)",
        "ALTER TABLE T_SPRG_R23_DATE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R23_DATE (ID, COL_23) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R24_TIMESTAMP (ID NUMBER PRIMARY KEY, COL_24 TIMESTAMP)",
        "ALTER TABLE T_SPRG_R24_TIMESTAMP ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R24_TIMESTAMP (ID, COL_24) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R25_TIMESTAMP_ (ID NUMBER PRIMARY KEY, COL_25 TIMESTAMP WITH TIME"
            + " ZONE)",
        "ALTER TABLE T_SPRG_R25_TIMESTAMP_ ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R25_TIMESTAMP_ (ID, COL_25) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R26_TIMESTAMP_ (ID NUMBER PRIMARY KEY, COL_26 TIMESTAMP WITH LOCAL"
            + " TIME ZONE)",
        "ALTER TABLE T_SPRG_R26_TIMESTAMP_ ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R26_TIMESTAMP_ (ID, COL_26) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R27_INTERVAL_Y (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R27_INTERVAL_Y ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R27_INTERVAL_Y (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R28_INTERVAL_D (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R28_INTERVAL_D ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R28_INTERVAL_D (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R29_RAW (ID NUMBER PRIMARY KEY, COL_29 RAW(255))",
        "ALTER TABLE T_SPRG_R29_RAW ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R29_RAW (ID, COL_29) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R30_LONG_RAW (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R30_LONG_RAW ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R30_LONG_RAW (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R31_BLOB (ID NUMBER PRIMARY KEY, COL_31 BLOB)",
        "ALTER TABLE T_SPRG_R31_BLOB ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R31_BLOB (ID, COL_31) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R32_CLOB (ID NUMBER PRIMARY KEY, COL_32 CLOB)",
        "ALTER TABLE T_SPRG_R32_CLOB ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R32_CLOB (ID, COL_32) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R33_NCLOB (ID NUMBER PRIMARY KEY, COL_33 NCLOB)",
        "ALTER TABLE T_SPRG_R33_NCLOB ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R33_NCLOB (ID, COL_33) VALUES (1, NULL)",
        "CREATE TABLE T_SPRG_R34_BFILE (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R34_BFILE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R34_BFILE (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R35_LONG (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R35_LONG ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R35_LONG (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R36_ROWID (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R36_ROWID ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R36_ROWID (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R37_UROWID (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R37_UROWID ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R37_UROWID (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R38_BOOLEAN (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R38_BOOLEAN ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R38_BOOLEAN (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R39_JSON (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R39_JSON ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R39_JSON (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R40_XMLTYPE (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R40_XMLTYPE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R40_XMLTYPE (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R41_SDO_GEOMET (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R41_SDO_GEOMET ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R41_SDO_GEOMET (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R42_SDO_TOPO_G (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R42_SDO_TOPO_G ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R42_SDO_TOPO_G (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R43_SDO_GEORAS (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R43_SDO_GEORAS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R43_SDO_GEORAS (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R44_VECTOR (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R44_VECTOR ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R44_VECTOR (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R45_ANYDATA (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R45_ANYDATA ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R45_ANYDATA (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R46_ANYTYPE (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R46_ANYTYPE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R46_ANYTYPE (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R47_ANYDATASET (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R47_ANYDATASET ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R47_ANYDATASET (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R48_URITYPE (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R48_URITYPE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R48_URITYPE (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R49_DBURITYPE (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R49_DBURITYPE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R49_DBURITYPE (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R50_XDBURITYPE (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R50_XDBURITYPE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R50_XDBURITYPE (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R51_HTTPURITYP (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R51_HTTPURITYP ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R51_HTTPURITYP (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R52_EXPRESSION (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R52_EXPRESSION ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R52_EXPRESSION (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R53_ORDAUDIO (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R53_ORDAUDIO ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R53_ORDAUDIO (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R54_ORDIMAGE (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R54_ORDIMAGE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R54_ORDIMAGE (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R55_ORDVIDEO (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R55_ORDVIDEO ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R55_ORDVIDEO (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R56_ORDDOC (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R56_ORDDOC ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R56_ORDDOC (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R57_SI_STILLIM (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R57_SI_STILLIM ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R57_SI_STILLIM (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R58_SI_COLOR (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R58_SI_COLOR ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R58_SI_COLOR (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R59_SI_AVERAGE (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R59_SI_AVERAGE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R59_SI_AVERAGE (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R60_SI_COLORHI (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R60_SI_COLORHI ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R60_SI_COLORHI (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R61_SI_POSITIO (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R61_SI_POSITIO ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R61_SI_POSITIO (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R62_SI_TEXTURE (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R62_SI_TEXTURE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R62_SI_TEXTURE (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R63_SI_FEATURE (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R63_SI_FEATURE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R63_SI_FEATURE (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R64_MLSLABEL (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R64_MLSLABEL ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R64_MLSLABEL (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R65_REF (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R65_REF ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R65_REF (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R66_REF_CURSOR (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R66_REF_CURSOR ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R66_REF_CURSOR (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R67_VARRAY (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R67_VARRAY ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R67_VARRAY (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R68_NESTED_TAB (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R68_NESTED_TAB ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R68_NESTED_TAB (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R69_ASSOCIATIV (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R69_ASSOCIATIV ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R69_ASSOCIATIV (ID, DUMMY) VALUES (1, 'test')",
        "CREATE TABLE T_SPRG_R70_OBJECT_TYP (ID NUMBER PRIMARY KEY, DUMMY VARCHAR2(10))",
        "ALTER TABLE T_SPRG_R70_OBJECT_TYP ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO T_SPRG_R70_OBJECT_TYP (ID, DUMMY) VALUES (1, 'test')"
      };

  private void setUpOracleUser(String user, String password) {
    oracleSysUser.runSQLUpdate(
        String.format("CREATE USER %s IDENTIFIED BY %s CONTAINER=ALL", user, password));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT EXECUTE_CATALOG_ROLE TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(String.format("GRANT CONNECT TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(String.format("GRANT CREATE SESSION TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$DATABASE TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$PDBS TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON DBA_SUPPLEMENTAL_LOGGING TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$ARCHIVED_LOG TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$LOGMNR_CONTENTS TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$LOG TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$LOGFILE TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$THREAD TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$PARAMETER TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$NLS_PARAMETERS TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$TIMEZONE_NAMES TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$LOGMNR_LOGS TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$ARCHIVE_DEST_STATUS TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.V_$TRANSACTION TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON SYS.DBA_REGISTRY TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(String.format("GRANT SELECT ON SYS.OBJ$ TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(String.format("GRANT SELECT ON SYS.ENC$ TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(String.format("GRANT CREATE TABLE TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT UNLIMITED TABLESPACE TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ANY DICTIONARY TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(String.format("GRANT SET CONTAINER TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(String.format("GRANT LOGMINING TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT EXECUTE ON DBMS_LOGMNR TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT EXECUTE ON DBMS_LOGMNR_D TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ANY TRANSACTION TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(String.format("GRANT SELECT ANY TABLE TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("GRANT SELECT ON DBA_EXTENTS TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(String.format("GRANT CREATE ANY TABLE TO %s CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(
        String.format("ALTER USER %s QUOTA 50m ON USERS CONTAINER=ALL", user));
    oracleSysUser.runSQLUpdate(String.format("GRANT ALTER SYSTEM TO %s CONTAINER=ALL", user));
  }

  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (OracleDatastreamToSpannerDataTypesIT.class) {
      testInstances.add(this);
      if (!initialized) {
        LOG.info("Setting up Oracle sys resource manager...");
        org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager.Builder sysBuilder =
            org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager.builder(testName);
        if (System.getProperty("hostIp") != null) {
          sysBuilder.setPassword(System.getProperty("cloudProxyPassword"));
          sysBuilder.setHost(System.getProperty("hostIp"));
          sysBuilder.setPort(1521);
          sysBuilder.setUsername("sys as sysdba");
          sysBuilder.setDatabaseName("XE");
        }
        oracleSysUser =
            (org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager)
                new SpannerOracleResourceManager(sysBuilder);

        String oracleUser = "C##U" + RandomStringUtils.randomAlphanumeric(10).toUpperCase();
        String oraclePassword = "A" + RandomStringUtils.randomAlphanumeric(10);

        LOG.info("Provisioning isolated user: " + oracleUser);
        setUpOracleUser(oracleUser, oraclePassword);

        // Build isolated RM
        org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager.Builder builder =
            org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager.builder(testName);
        if (System.getProperty("hostIp") != null) {
          builder.setPassword(oraclePassword);
          builder.setHost(System.getProperty("hostIp"));
          builder.setPort(1521);
          builder.setUsername(oracleUser);
          builder.setDatabaseName("/XEPDB1");
        }
        oracleResourceManager =
            (org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager)
                new SpannerOracleResourceManager(builder);

        LOG.info("Setting up Spanner resource manager...");
        spannerResourceManager = setUpSpannerResourceManager();

        LOG.info("Setting up GCS resource manager...");
        gcsResourceManager = setUpSpannerITGcsResourceManager();

        LOG.info("Setting up Pub/Sub resource manager...");
        pubsubResourceManager = setUpPubSubResourceManager();

        LOG.info("Setting up Datastream resource manager...");
        datastreamResourceManager =
            DatastreamResourceManager.builder(testName, PROJECT, REGION)
                .setCredentialsProvider(credentialsProvider)
                .setPrivateConnectivity("datastream-connect-2")
                .build();

        LOG.info("Executing Oracle DDL commands efficiently using a single JDBC connection...");
        try (java.sql.Connection conn =
                java.sql.DriverManager.getConnection(
                    oracleResourceManager.getUri(),
                    oracleResourceManager.getUsername(),
                    oracleResourceManager.getPassword());
            java.sql.Statement stmt = conn.createStatement()) {
          for (String cmd : ORACLE_DDL_COMMANDS) {
            stmt.execute(cmd);
          }
        } catch (Exception e) {
          throw new RuntimeException(
              "Failed to efficiently upload Oracle DDL via single connection", e);
        }

        initialized = true;
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    LOG.info("Cleaning up resources...");
    for (OracleDatastreamToSpannerDataTypesIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        oracleResourceManager,
        spannerResourceManager,
        gcsResourceManager,
        pubsubResourceManager,
        datastreamResourceManager);
  }

  @Test
  public void testOracleDataTypes() throws Exception {
    LOG.info("Creating Spanner DDL...");
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    Map<String, List<Map<String, Object>>> expectedData = getExpectedData();

    OracleSource oracleSource =
        OracleSource.builder(
                oracleResourceManager.getHost(),
                oracleResourceManager.getUsername(),
                oracleResourceManager.getPassword(),
                oracleResourceManager.getPort(),
                oracleResourceManager.getDatabaseName())
            .setAllowedTables(
                Map.of(
                    oracleResourceManager.getUsername().toUpperCase(),
                    getAllowedTables(expectedData)))
            .build();

    LOG.info("Launching Dataflow job...");
    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            "oracle-data-types",
            null,
            null,
            "oracle-datastream-to-spanner-data-types",
            spannerResourceManager,
            pubsubResourceManager,
            new HashMap<>(),
            null,
            null,
            gcsResourceManager,
            datastreamResourceManager,
            null,
            oracleSource);
    assertThatPipeline(jobInfo).isRunning();

    ConditionCheck condition = buildConditionCheck(spannerResourceManager, expectedData);
    LOG.info("Waiting for pipeline to process data...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(JOB_START_PROCESSING_WAIT_MINUTES)),
                condition);
    assertThatResult(result).meetsConditions();

    validateResult(spannerResourceManager, expectedData);
  }

  private void validateResult(
      SpannerResourceManager resourceManager, Map<String, List<Map<String, Object>>> expectedData) {
    for (Map.Entry<String, List<Map<String, Object>>> entry : expectedData.entrySet()) {
      String type = entry.getKey();
      String tableName = TABLE_PREFIX + type;
      LOG.info("Asserting type: {}", type);

      List<Struct> rows =
          resourceManager.readTableRecords(tableName, entry.getValue().get(0).keySet());
      SpannerAsserts.assertThatStructs(rows)
          .hasRecordsUnorderedCaseInsensitiveColumns(entry.getValue());
    }
  }

  private List<Map<String, Object>> createRows(String colName, Object... values) {
    List<Object> vals = Arrays.asList(values);
    List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < vals.size(); i++) {
      Map<String, Object> row = new HashMap<>();
      row.put("ID", i + 1L);
      row.put(colName, vals.get(i));
      rows.add(row);
    }
    return rows;
  }

  private List<String> getAllowedTables(Map<String, List<Map<String, Object>>> expectedData) {
    List<String> tableNames = new ArrayList<>(expectedData.size() + UNSUPPORTED_TYPE_TABLES.size());
    for (String tablePrefix : expectedData.keySet()) {
      tableNames.add(TABLE_PREFIX + tablePrefix);
    }
    tableNames.addAll(UNSUPPORTED_TYPE_TABLES);
    return tableNames;
  }

  private ConditionCheck buildConditionCheck(
      SpannerResourceManager resourceManager, Map<String, List<Map<String, Object>>> expectedData) {
    ConditionCheck combinedCondition = null;
    for (Map.Entry<String, List<Map<String, Object>>> entry : expectedData.entrySet()) {
      String tableName = TABLE_PREFIX + entry.getKey();
      int numRows = entry.getValue().size();
      ConditionCheck c =
          SpannerRowsCheck.builder(resourceManager, tableName).setMinRows(numRows).build();
      if (combinedCondition == null) {
        combinedCondition = c;
      } else {
        combinedCondition = combinedCondition.and(c);
      }
    }
    return combinedCondition;
  }

  private Map<String, List<Map<String, Object>>> getExpectedData() {
    Map<String, List<Map<String, Object>>> expectedData = new HashMap<>();
    expectedData.put("R0_VARCHAR2", createRows("COL_0", "NULL"));
    expectedData.put("R1_VARCHAR", createRows("COL_1", "NULL"));
    expectedData.put("R2_CHAR", createRows("COL_2", "NULL"));
    expectedData.put("R3_CHARACTER", createRows("COL_3", "NULL"));
    expectedData.put("R4_NVARCHAR2", createRows("COL_4", "NULL"));
    expectedData.put("R5_NCHAR", createRows("COL_5", "NULL"));
    expectedData.put("R6_NCHAR_VARY", createRows("COL_6", "NULL"));
    expectedData.put("R7_NATIONAL_C", createRows("COL_7", "NULL"));
    expectedData.put("R8_NATIONAL_C", createRows("COL_8", "NULL"));
    expectedData.put("R9_NATIONAL_C", createRows("COL_9", "NULL"));
    expectedData.put("R10_NATIONAL_C", createRows("COL_10", "NULL"));
    expectedData.put("R11_NUMBER", createRows("COL_11", "NULL"));
    expectedData.put("R12_NUMERIC", createRows("COL_12", "NULL"));
    expectedData.put("R13_DECIMAL", createRows("COL_13", "NULL"));
    expectedData.put("R14_DEC", createRows("COL_14", "NULL"));
    expectedData.put("R15_FLOAT", createRows("COL_15", "NULL"));
    expectedData.put("R16_DOUBLE_PRE", createRows("COL_16", "NULL"));
    expectedData.put("R17_REAL", createRows("COL_17", "NULL"));
    expectedData.put("R18_BINARY_FLO", createRows("COL_18", "NULL"));
    expectedData.put("R19_BINARY_DOU", createRows("COL_19", "NULL"));
    expectedData.put("R20_INTEGER", createRows("COL_20", "NULL"));
    expectedData.put("R21_INT", createRows("COL_21", "NULL"));
    expectedData.put("R22_SMALLINT", createRows("COL_22", "NULL"));
    expectedData.put("R23_DATE", createRows("COL_23", "NULL"));
    expectedData.put("R24_TIMESTAMP", createRows("COL_24", "NULL"));
    expectedData.put("R25_TIMESTAMP_", createRows("COL_25", "NULL"));
    expectedData.put("R26_TIMESTAMP_", createRows("COL_26", "NULL"));
    expectedData.put("R29_RAW", createRows("COL_29", "NULL"));
    expectedData.put("R31_BLOB", createRows("COL_31", "NULL"));
    expectedData.put("R32_CLOB", createRows("COL_32", "NULL"));
    expectedData.put("R33_NCLOB", createRows("COL_33", "NULL"));

    return expectedData;
  }
}
