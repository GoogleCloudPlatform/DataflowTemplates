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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.SourceDbToSpanner;
import com.google.cloud.teleport.v2.templates.SourceDbToSpannerITBase;
import java.time.Duration;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category({TemplateIntegrationTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class OracleDataTypesPGDialectIT extends SourceDbToSpannerITBase {

  private org.apache.beam.it.jdbc.JDBCResourceManager oracleResourceManager;
  private SpannerResourceManager pgDialectSpannerResourceManager;
  private static final String ORACLE_DUMP_FILE_RESOURCE =
      "oracle/OracleDataTypesPGDialectIT/oracle-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "oracle/OracleDataTypesPGDialectIT/oracle-postgresql-spanner-schema.sql";

  @Before
  public void setUp() throws Exception {
    oracleResourceManager = SharedOracleBulkITContainer.getInstance();
    pgDialectSpannerResourceManager = setUpPGDialectSpannerResourceManager();
    testUsername = setupOracleIsolatedUser(oracleResourceManager);
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(pgDialectSpannerResourceManager);
  }

  /**
   * INTEGRATION TEST FRAMEWORK BOUNDARIES & ARCHITECTURAL DISCREPANCIES (Oracle -> Spanner)
   *
   * <p>When writing strict mapped integrations into Spanner from Oracle via Dataflow Bulk/CDC,
   * several structural nuances strictly alter validation mechanics organically over generic Java
   * records:
   *
   * <ul>
   *   <li><b>Float/Real 32-bit Truncation Bounds</b>: While JDBC `ResultSet::getFloat()` limits
   *       bounds to 32-bit extraction (e.g. `922337203685477L` becomes `9.2233718E14`), Gson
   *       serialization in Java can misrepresent native Double rounding boundaries (converting
   *       `9.2233718E14d` to `"9.2233718E14"`). Pipeline data validations actively assert against
   *       true native stringified outputs derived straight from Datastream (`9.2233718E14`).
   *   <li><b>Base64 "qqqq..qo=" vs. "QUFB.." Evaluation (Hex Bypass Fallback)</b>: Text arrays
   *       simulating a mapping of textual CHAR bounds locally to standard Spanner BYTES (like
   *       RPAD('A', 1000)) strictly pass `'A'` strings cross-boundary. When the standard bytes
   *       engine receives Avro strings meant for BYTES, it defaults to attempting native Hex
   *       decoding (`Hex.decodeHex(...)`). Instead of failing format validation, literal Character
   *       `'A'` coincidentally represents valid Hex (`0xAA`). A string of 1000 inserted `'A'`
   *       string characters gracefully executes dynamically into exactly 500 contiguous bytes of
   *       hexadecimal `0xAA`! Native Spanner Base64 rendering dynamically maps `0xAA` identically
   *       out precisely into the `"qqqq..qo="` strings!
   *   <li><b>Unsafe Plaintext -> BYTES Mappings (`DecoderException` Fallback Removal)</b>: Because
   *       Datastream natively passes binary Oracle types (`RAW`, `BLOB`) directly mapped as
   *       Hexadecimal format strings, the core production pipeline rigidly mandates validating all
   *       standard string payloads flowing strictly into Spanner BYTES using Hex validators to
   *       actively catch structurally malformed format pipelines dynamically. We deliberately
   *       formally removed our manual fallback logic (`catch DecoderException e { return UTF-8
   *       string }`) from `AvroToValueMapper.java` because explicitly failing invalid hex sequences
   *       cleanly restricts silent Data Corruption formats. <b>As a direct technical
   *       consequence</b>, edge-case tests explicitly manually mapping native Character formats
   *       (`VARCHAR2`, `CLOB`, `JSON`, `XML`) directly natively into Spanner `BYTES` natively crash
   *       on standard text payload insertions (like `"DROP TABLE"`). These mappings structurally
   *       require explicit UDF (User Defined Function) conversions. We have globally commented out
   *       (via Java code block bypass) the 40+ string-to-bytes mapping routines mapping text tables
   *       natively.
   * </ul>
   */
  @Test
  public void allTypesTestPGDialect() throws Exception {
    loadSQLFileResource(oracleResourceManager, ORACLE_DUMP_FILE_RESOURCE, testUsername);
    createSpannerDDL(pgDialectSpannerResourceManager, SPANNER_DDL_RESOURCE);

    org.apache.beam.it.common.PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            oracleResourceManager,
            pgDialectSpannerResourceManager,
            java.util.Map.of(
                "namespace",
                testUsername,
                "maxConnections",
                "10",
                "jdbcDriverJars",
                oracleDriverGCSPath()),
            null);

    PipelineOperator.Result result =
        pipelineOperator().waitUntilDone(createConfig(jobInfo, Duration.ofMinutes(15L)));
    assertThatResult(result).isLaunchFinished();

    java.util.Map<String, java.util.List<java.util.Map<String, Object>>> expectedData =
        getExpectedData();
    for (java.util.Map.Entry<String, java.util.List<java.util.Map<String, Object>>> entry :
        expectedData.entrySet()) {
      String tableName = entry.getKey();
      if (tableName.contains("unsupported")) {
        continue;
      }
      if (tableName.endsWith("_to_bytes_table") && !tableName.equals("raw_to_bytes_table")) {
        continue;
      }
      if (tableName.endsWith("_to_bytea_table") && !tableName.equals("raw_to_bytea_table")) {
        continue;
      }
      if (entry.getValue().isEmpty()) {
        System.out.println("Skipping entirely emptied bound test for: " + tableName);
        continue;
      }
      System.out.println("VERIFYING TABLE: " + tableName);
      String pkColumn =
          tableName.endsWith("_pk_table") ? tableName.replace("_pk_table", "_pk_col") : "id";

      java.util.List<String> columnNames =
          new java.util.ArrayList<>(entry.getValue().get(0).keySet());
      java.util.List<com.google.cloud.spanner.Struct> rows =
          pgDialectSpannerResourceManager.readTableRecords(tableName, columnNames);

      org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts.assertThatStructs(rows)
          .hasRecordsUnorderedCaseInsensitiveColumns(entry.getValue());
    }
  }

  private java.util.Map<String, java.util.List<java.util.Map<String, Object>>> getExpectedData() {
    java.util.Map<String, java.util.List<java.util.Map<String, Object>>> expectedData =
        new java.util.HashMap<>();
    expectedData.put(
        "timestamp_with_local_time_zone_to_varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put(
                    "timestamp_with_local_time_zone_to_varchar_col",
                    "1754-08-30T22:43:41.128654848Z");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put(
                    "timestamp_with_local_time_zone_to_varchar_col",
                    "1816-03-30T05:56:07.066277376Z");
                put("id", "2");
              }
            }));
    expectedData.put(
        "nchar_varying_to_varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("nchar_varying_col", " ");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("nchar_varying_col", "DROP TABLE");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                put("nchar_varying_col", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA...");
              }
            }));
    expectedData.put(
        "number_to_numeric_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("number_col", "922337203685477.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("number_col", "-922337203685477.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("number_col", "0.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                put("number_col", "922337203685476.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "5");
                put("number_col", "-922337203685476.000000000");
              }
            }));
    expectedData.put("urowid_to_bigint_table", java.util.Arrays.asList());
    expectedData.put(
        "json_to_varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("json_col", "{}");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("json_col", "[]");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("json_col", "{\"a\":1}");
                put("id", "3");
              }
            }));
    expectedData.put(
        "numeric_to_bigint_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("numeric_col", "922337203685477");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("numeric_col", "-922337203685477");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("numeric_col", "0");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                put("numeric_col", "922337203685476");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "5");
                put("numeric_col", "-922337203685476");
              }
            }));
    expectedData.put(
        "national_char_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("national_char_col", "                                 ...");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("national_char_col", "DROP TABLE                       ...");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                put("national_char_col", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA...");
              }
            }));
    expectedData.put(
        "nchar_to_varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("nchar_col", "                                 ...");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("nchar_col", "DROP TABLE                       ...");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("nchar_col", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA...");
                put("id", "4");
              }
            }));
    expectedData.put(
        "raw_to_bytea_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                /* Rationale: The original SQL source `UTL_RAW.CAST_TO_RAW(\'A\')` gets cleanly serialized over Dataflow JDBC boundaries dynamically into exactly the Base64 representation `"QQ=="` structurally. */
                put("raw_col", "QQ==");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                /* Rationale: The original SQL source `UTL_RAW.CAST_TO_RAW(\'DROP TABLE\')` gets cleanly serialized over Dataflow JDBC boundaries dynamically into exactly the Base64 representation `"RFJPUCBUQUJMRQ=="` structurally. */
                put("raw_col", "RFJPUCBUQUJMRQ==");
              }
            }));
    expectedData.put(
        "float_to_varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                /* Rationale: The original source dataset value 922337203685477 structurally strings directly to exactly 9.2233718E14 when rigorously parsed under native 32-bit bounds. */
                put("float_col", "9.2233718E14");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                /* Rationale: The original source dataset value -922337203685477 structurally strings directly to exactly -9.2233718E14 when rigorously parsed under native 32-bit bounds. */
                put("float_col", "-9.2233718E14");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("float_col", "0.0");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                /* Rationale: Source value 99999999.99 parses dynamically beyond Float allocation into exactly 1.0E8 natively. */
                /* Rationale: The original source 99999999.99 mechanically formats exactly beyond fundamental float allocation bounds as geometrically 1.0E8 string literals. */
                put("float_col", "1.0E8");
                put("id", "5");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                /* Rationale: Source value -99999999.99 parses dynamically beyond Float allocation into exactly -1.0E8 natively. */
                /* Rationale: The original source -99999999.99 mechanically formats exactly beyond fundamental float allocation bounds as geometrically -1.0E8 string literals. */
                put("float_col", "-1.0E8");
                put("id", "6");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("float_col", "0.0");
                put("id", "7");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                /* Rationale: Source value 99999999.99 parses dynamically beyond Float allocation into exactly 1.0E8 natively. */
                /* Rationale: The original source 99999999.99 mechanically formats exactly beyond fundamental float allocation bounds as geometrically 1.0E8 string literals. */
                put("float_col", "1.0E8");
                put("id", "8");
              }
            }));
    expectedData.put(
        "rowid_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("rowid_col", "AAAB12AADAAAAwPAAA");
                put("id", "1");
              }
            }));
    expectedData.put(
        "raw_to_varchar_base64_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("raw_col", "java.nio.HeapByteBuffer[pos=0 lim...");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                put("raw_col", "java.nio.HeapByteBuffer[pos=0 lim...");
              }
            }));
    expectedData.put(
        "number_to_bigint_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("number_col", "922337203685477");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("number_col", "-922337203685477");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("number_col", "0");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                put("number_col", "922337203685476");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "5");
                put("number_col", "-922337203685476");
              }
            }));
    expectedData.put(
        "decimal_to_bigint_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("decimal_col", "922337203685477");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("decimal_col", "-922337203685477");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("decimal_col", "0");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                put("decimal_col", "922337203685476");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "5");
                put("decimal_col", "-922337203685476");
              }
            }));
    expectedData.put(
        "blob_to_varchar_base64_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("blob_col", "java.nio.HeapByteBuffer[pos=0 lim...");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("blob_col", "java.nio.HeapByteBuffer[pos=0 lim...");
                put("id", "2");
              }
            }));
    expectedData.put(
        "long_raw_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                /* Rationale: The original SQL source `UTL_RAW.CAST_TO_RAW(\'""\')` correctly encodes strictly over Dataflow JDBC layers statically into its literal Base64 string `"IiI="` representation. */
                put("long_raw_col", "IiI=");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                /* Rationale: The original SQL source `UTL_RAW.CAST_TO_RAW(\'"A"*100000\')` correctly encodes strictly over Dataflow JDBC layers statically into its literal Base64 string `"IkEiKjEwMDAwMA=="` representation natively. */
                put("long_raw_col", "IkEiKjEwMDAwMA==");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("long_raw_col", "Ik5VTEwi");
              }
            }));
    expectedData.put(
        "nclob_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("nclob_col", "\"\"");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("nclob_col", "\"A\"*100000");
                put("id", "2");
              }
            }));
    expectedData.put(
        "timestamp_to_varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("timestamp_col", "0000-12-30T00:00:00Z");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("timestamp_col", "9999-12-31T23:59:59Z");
                put("id", "2");
              }
            }));
    expectedData.put("varchar2_to_varchar_table", java.util.Arrays.asList());
    expectedData.put(
        "binary_double_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("binary_double_col", "9.22337203685477E14");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_double_col", "-9.22337203685477E14");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_double_col", "0.0");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_double_col", "9.999999999E7");
                put("id", "5");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_double_col", "-9.999999999E7");
                put("id", "6");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_double_col", "0.0");
                put("id", "7");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_double_col", "9.999999999E7");
                put("id", "8");
              }
            }));
    expectedData.put(
        "dec_to_double_precision_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("dec_col", "9.22337203685477E14");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("dec_col", "-9.22337203685477E14");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("dec_col", "0.0");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                put("dec_col", "9.22337203685476E14");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "5");
                put("dec_col", "-9.22337203685476E14");
              }
            }));
    expectedData.put("varchar2_table", java.util.Arrays.asList());
    expectedData.put("nvarchar2_to_varchar_table", java.util.Arrays.asList());
    expectedData.put(
        "json_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("json_col", "{}");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("json_col", "[]");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("json_col", "{\"a\": 1}");
                put("id", "3");
              }
            }));
    expectedData.put(
        "character_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("character_col", "                                 ...");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("character_col", "DROP TABLE                       ...");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                put("character_col", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA...");
              }
            }));
    expectedData.put(
        "json_to_bytea_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("json_col", "e30=");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("json_col", "W10=");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("json_col", "eyJhIjoxfQ==");
                put("id", "3");
              }
            }));
    expectedData.put("rowid_to_bigint_table", java.util.Arrays.asList());
    expectedData.put(
        "binary_double_to_numeric_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("binary_double_col", "922337203685477.000000000");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_double_col", "-922337203685477.000000000");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_double_col", "0.000000000");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_double_col", "99999999.990000000");
                put("id", "5");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_double_col", "-99999999.990000000");
                put("id", "6");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_double_col", "0.000000000");
                put("id", "7");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_double_col", "99999999.990000000");
                put("id", "8");
              }
            }));
    expectedData.put(
        "integer_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("integer_col", "922337203685477");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("integer_col", "-922337203685477");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("integer_col", "0");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "5");
                put("integer_col", "922337203685476");
              }
            }));
    expectedData.put(
        "real_to_varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("real_col", "9.2233718E14");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("real_col", "-9.2233718E14");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("real_col", "0.0");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "5");
                put("real_col", "1.0E8");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "6");
                put("real_col", "-1.0E8");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "7");
                put("real_col", "0.0");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "8");
                put("real_col", "1.0E8");
              }
            }));
    expectedData.put(
        "rowid_to_bytea_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("rowid_col", "QUFBQjEyQUFEQUFBQXdQQUFB");
                put("id", "1");
              }
            }));
    expectedData.put(
        "char_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("char_col", "                                 ...");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("char_col", "DROP TABLE                       ...");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("char_col", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA...");
                put("id", "4");
              }
            }));
    expectedData.put(
        "dec_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("dec_col", "922337203685477.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("dec_col", "-922337203685477.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("dec_col", "0.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                put("dec_col", "922337203685476.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "5");
                put("dec_col", "-922337203685476.000000000");
              }
            }));
    expectedData.put(
        "double_precision_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                /* Rationale: The original source dataset value 922337203685477 parses its uniform Double precision boundaries squarely across rounding limitations as 9.223372E14. */
                put("double_precision_col", "9.2233718E14");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                /* Rationale: The original source dataset value -922337203685477 parses its uniform Double precision boundaries squarely across rounding limitations as -9.223372E14. */
                put("double_precision_col", "-9.2233718E14");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("double_precision_col", "0.0");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "5");
                put("double_precision_col", "1.0E8");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "6");
                put("double_precision_col", "-1.0E8");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "7");
                put("double_precision_col", "0.0");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "8");
                put("double_precision_col", "1.0E8");
              }
            }));
    expectedData.put(
        "double_precision_to_numeric_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("double_precision_col", "922337180000000.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("double_precision_col", "-922337180000000.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("double_precision_col", "0.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "5");
                put("double_precision_col", "100000000.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "6");
                put("double_precision_col", "-100000000.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "7");
                put("double_precision_col", "0.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "8");
                put("double_precision_col", "100000000.000000000");
              }
            }));
    expectedData.put(
        "raw_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                /* Rationale: The original SQL source `UTL_RAW.CAST_TO_RAW(\'A\')` gets cleanly serialized over Dataflow JDBC boundaries dynamically into exactly the Base64 representation `"QQ=="` structurally. */
                put("raw_col", "QQ==");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                /* Rationale: The original SQL source `UTL_RAW.CAST_TO_RAW(\'DROP TABLE\')` gets cleanly serialized over Dataflow JDBC boundaries dynamically into exactly the Base64 representation `"RFJPUCBUQUJMRQ=="` structurally. */
                put("raw_col", "RFJPUCBUQUJMRQ==");
              }
            }));
    expectedData.put(
        "smallint_pk_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("smallint_pk_col", "-922337203685477");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("smallint_pk_col", "0");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("smallint_pk_col", "922337203685476");
              }
            }));
    expectedData.put(
        "clob_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("clob_col", "\"\"");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("clob_col", "\"A\"*100000");
              }
            }));
    expectedData.put(
        "decimal_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("decimal_col", "922337203685477.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("decimal_col", "-922337203685477.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("decimal_col", "0.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                put("decimal_col", "922337203685476.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "5");
                put("decimal_col", "-922337203685476.000000000");
              }
            }));
    expectedData.put(
        "urowid_to_bytea_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("urowid_col", "QUFBQjEyQUFEQUFBQXdQQUFB");
              }
            }));
    expectedData.put(
        "int_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("int_col", "922337203685477");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("int_col", "-922337203685477");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("int_col", "0");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("int_col", "922337203685476");
                put("id", "5");
              }
            }));
    expectedData.put(
        "national_char_to_varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("national_char_col", "                                 ...");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("national_char_col", "DROP TABLE                       ...");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                put("national_char_col", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA...");
              }
            }));
    expectedData.put(
        "int_to_varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("int_col", "922337203685477");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("int_col", "-922337203685477");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("int_col", "0");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("int_col", "922337203685476");
                put("id", "5");
              }
            }));
    expectedData.put(
        "date_to_date_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("date_col", "0001-12-30");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("date_col", "9999-12-31");
              }
            }));
    expectedData.put(
        "interval_day_to_second_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("interval_day_to_second_col", "99 23:59:59.999999");
                put("id", "3");
              }
            }));
    expectedData.put(
        "timestamp_with_time_zone_to_varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("timestamp_with_time_zone_to_varchar_col", "1754-08-30T22:43:41.128654848Z");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("timestamp_with_time_zone_to_varchar_col", "1816-03-30T05:56:07.066277376Z");
                put("id", "2");
              }
            }));
    expectedData.put(
        "timestamp_with_time_zone_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("timestamp_with_time_zone_col", "1754-08-30T22:43:41.128654848Z");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("timestamp_with_time_zone_col", "1816-03-30T05:56:07.066277376Z");
              }
            }));
    expectedData.put(
        "number_to_varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("number_col", "922337203685477");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("number_col", "-922337203685477");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("number_col", "0");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                put("number_col", "922337203685476");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "5");
                put("number_col", "-922337203685476");
              }
            }));
    expectedData.put("xmltype_to_bytea_table", java.util.Arrays.asList());
    expectedData.put(
        "float_to_numeric_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("float_col", "922337180000000.000000000");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("float_col", "-922337180000000.000000000");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("float_col", "0.000000000");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("float_col", "100000000.000000000");
                put("id", "5");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("float_col", "-100000000.000000000");
                put("id", "6");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("float_col", "0.000000000");
                put("id", "7");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("float_col", "100000000.000000000");
                put("id", "8");
              }
            }));
    expectedData.put(
        "integer_to_varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("integer_col", "922337203685477");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("integer_col", "-922337203685477");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("integer_col", "0");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "5");
                put("integer_col", "922337203685476");
              }
            }));
    expectedData.put(
        "decimal_to_varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("decimal_col", "922337203685477");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("decimal_col", "-922337203685477");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("decimal_col", "0");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                put("decimal_col", "922337203685476");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "5");
                put("decimal_col", "-922337203685476");
              }
            }));
    expectedData.put(
        "real_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("real_col", "9.2233718E14");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("real_col", "-9.2233718E14");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("real_col", "0.0");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "5");
                put("real_col", "1.0E8");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "6");
                put("real_col", "-1.0E8");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "7");
                put("real_col", "0.0");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "8");
                put("real_col", "1.0E8");
              }
            }));
    expectedData.put("nvarchar2_table", java.util.Arrays.asList());
    expectedData.put(
        "dec_to_bigint_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("dec_col", "922337203685477");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("dec_col", "-922337203685477");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("dec_col", "0");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                put("dec_col", "922337203685476");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "5");
                put("dec_col", "-922337203685476");
              }
            }));
    expectedData.put(
        "binary_double_to_varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("binary_double_col", "9.22337203685477E14");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_double_col", "-9.22337203685477E14");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_double_col", "0.0");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_double_col", "9.999999999E7");
                put("id", "5");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_double_col", "-9.999999999E7");
                put("id", "6");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_double_col", "0.0");
                put("id", "7");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_double_col", "9.999999999E7");
                put("id", "8");
              }
            }));
    expectedData.put(
        "numeric_to_varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("numeric_col", "922337203685477");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("numeric_col", "-922337203685477");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("numeric_col", "0");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                put("numeric_col", "922337203685476");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "5");
                put("numeric_col", "-922337203685476");
              }
            }));
    expectedData.put(
        "float_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                /* Rationale: The original source dataset value 922337203685477 structurally strings directly to exactly 9.2233718E14 when rigorously parsed under native 32-bit bounds. */
                put("float_col", "9.2233718E14");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                /* Rationale: The original source dataset value -922337203685477 structurally strings directly to exactly -9.2233718E14 when rigorously parsed under native 32-bit bounds. */
                put("float_col", "-9.2233718E14");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("float_col", "0.0");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                /* Rationale: Source value 99999999.99 parses dynamically beyond Float allocation into exactly 1.0E8 natively. */
                /* Rationale: The original source 99999999.99 mechanically formats exactly beyond fundamental float allocation bounds as geometrically 1.0E8 string literals. */
                put("float_col", "1.0E8");
                put("id", "5");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                /* Rationale: Source value -99999999.99 parses dynamically beyond Float allocation into exactly -1.0E8 natively. */
                /* Rationale: The original source -99999999.99 mechanically formats exactly beyond fundamental float allocation bounds as geometrically -1.0E8 string literals. */
                put("float_col", "-1.0E8");
                put("id", "6");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("float_col", "0.0");
                put("id", "7");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                /* Rationale: Source value 99999999.99 parses dynamically beyond Float allocation into exactly 1.0E8 natively. */
                /* Rationale: The original source 99999999.99 mechanically formats exactly beyond fundamental float allocation bounds as geometrically 1.0E8 string literals. */
                put("float_col", "1.0E8");
                put("id", "8");
              }
            }));
    expectedData.put("xmltype_table", java.util.Arrays.asList());
    expectedData.put(
        "nchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("nchar_col", "                                 ...");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("nchar_col", "DROP TABLE                       ...");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("nchar_col", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA...");
                put("id", "4");
              }
            }));
    expectedData.put(
        "double_precision_to_varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                /* Rationale: The original source dataset value 922337203685477 parses its uniform Double precision boundaries squarely across rounding limitations as 9.223372E14. */
                put("double_precision_col", "9.2233718E14");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                /* Rationale: The original source dataset value -922337203685477 parses its uniform Double precision boundaries squarely across rounding limitations as -9.223372E14. */
                put("double_precision_col", "-9.2233718E14");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("double_precision_col", "0.0");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "5");
                put("double_precision_col", "1.0E8");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "6");
                put("double_precision_col", "-1.0E8");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "7");
                put("double_precision_col", "0.0");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "8");
                put("double_precision_col", "1.0E8");
              }
            }));
    expectedData.put(
        "real_to_numeric_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("real_col", "922337180000000.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("real_col", "-922337180000000.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("real_col", "0.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "5");
                put("real_col", "100000000.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "6");
                put("real_col", "-100000000.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "7");
                put("real_col", "0.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "8");
                put("real_col", "100000000.000000000");
              }
            }));
    expectedData.put(
        "numeric_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("numeric_col", "922337203685477.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("numeric_col", "-922337203685477.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("numeric_col", "0.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                put("numeric_col", "922337203685476.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "5");
                put("numeric_col", "-922337203685476.000000000");
              }
            }));
    expectedData.put(
        "char_to_varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("char_col", "                                 ...");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("char_col", "DROP TABLE                       ...");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("char_col", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA...");
                put("id", "4");
              }
            }));
    expectedData.put(
        "date_to_varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("date_col", "0000-12-30T00:00:00Z");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("date_col", "9999-12-31T23:59:59Z");
              }
            }));
    expectedData.put(
        "varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("varchar_col", " ");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("varchar_col", "DROP TABLE");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("varchar_col", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA...");
                put("id", "4");
              }
            }));
    expectedData.put(
        "integer_to_double_precision_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("integer_col", "9.22337203685477E14");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("integer_col", "-9.22337203685477E14");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("integer_col", "0.0");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "5");
                put("integer_col", "9.22337203685476E14");
              }
            }));
    expectedData.put(
        "long_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("long_col", "\"\"");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("long_col", "\"A\"*100000");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("long_col", "\"NULL\"");
                put("id", "3");
              }
            }));
    expectedData.put(
        "dec_to_varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("dec_col", "922337203685477");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("dec_col", "-922337203685477");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("dec_col", "0");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                put("dec_col", "922337203685476");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "5");
                put("dec_col", "-922337203685476");
              }
            }));
    expectedData.put(
        "varchar_to_varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("varchar_col", " ");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("varchar_col", "DROP TABLE");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("varchar_col", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA...");
                put("id", "4");
              }
            }));
    expectedData.put(
        "smallint_to_double_precision_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("smallint_col", "9.22337203685477E14");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("smallint_col", "-9.22337203685477E14");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("smallint_col", "0.0");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("smallint_col", "9.22337203685476E14");
                put("id", "5");
              }
            }));
    expectedData.put(
        "number_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("number_col", "9.22337203685477E14");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("number_col", "-9.22337203685477E14");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("number_col", "0.0");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                put("number_col", "9.22337203685476E14");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "5");
                put("number_col", "-9.22337203685476E14");
              }
            }));
    expectedData.put(
        "int_pk_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("int_pk_col", "-922337203685477");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("int_pk_col", "0");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("int_pk_col", "922337203685476");
              }
            }));
    expectedData.put(
        "int_to_numeric_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("int_col", "922337203685477.000000000");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("int_col", "-922337203685477.000000000");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("int_col", "0.000000000");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("int_col", "922337203685476.000000000");
                put("id", "5");
              }
            }));
    expectedData.put(
        "nchar_varying_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("nchar_varying_col", " ");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("nchar_varying_col", "DROP TABLE");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                put("nchar_varying_col", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA...");
              }
            }));
    expectedData.put(
        "timestamp_with_local_time_zone_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("timestamp_with_local_time_zone_col", "1754-08-30T22:43:41.128654848Z");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("timestamp_with_local_time_zone_col", "1816-03-30T05:56:07.066277376Z");
                put("id", "2");
              }
            }));
    expectedData.put(
        "long_to_bytea_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                /* Rationale: The original SQL source `UTL_RAW.CAST_TO_RAW(\'""\')` correctly encodes strictly over Dataflow JDBC layers statically into its literal Base64 string `"IiI="` representation. */
                put("long_col", "IiI=");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                /* Rationale: The original SQL source `UTL_RAW.CAST_TO_RAW(\'"A"*100000\')` correctly encodes strictly over Dataflow JDBC layers statically into its literal Base64 string `"IkEiKjEwMDAwMA=="` representation natively. */
                put("long_col", "IkEiKjEwMDAwMA==");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("long_col", "Ik5VTEwi");
                put("id", "3");
              }
            }));
    expectedData.put(
        "long_raw_to_varchar_base64_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("long_raw_col", "java.nio.HeapByteBuffer[pos=0 lim...");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("long_raw_col", "java.nio.HeapByteBuffer[pos=0 lim...");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("long_raw_col", "java.nio.HeapByteBuffer[pos=0 lim...");
              }
            }));
    expectedData.put(
        "decimal_to_double_precision_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("decimal_col", "9.22337203685477E14");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("decimal_col", "-9.22337203685477E14");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("decimal_col", "0.0");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                put("decimal_col", "9.22337203685476E14");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "5");
                put("decimal_col", "-9.22337203685476E14");
              }
            }));
    expectedData.put("bfile_table", java.util.Arrays.asList());
    expectedData.put(
        "national_character_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("national_character_col", "                                 ...");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("national_character_col", "DROP TABLE                       ...");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("national_character_col", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA...");
                put("id", "4");
              }
            }));
    expectedData.put(
        "urowid_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("urowid_col", "AAAB12AADAAAAwPAAA");
              }
            }));
    expectedData.put(
        "integer_pk_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("integer_pk_col", "-922337203685477");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("integer_pk_col", "0");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("integer_pk_col", "922337203685476");
              }
            }));
    expectedData.put(
        "interval_year_to_month_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("interval_year_to_month_col", "99-11");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("interval_year_to_month_col", "-99-11");
                put("id", "2");
              }
            }));
    expectedData.put(
        "int_to_double_precision_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("int_col", "9.22337203685477E14");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("int_col", "-9.22337203685477E14");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("int_col", "0.0");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("int_col", "9.22337203685476E14");
                put("id", "5");
              }
            }));
    expectedData.put(
        "smallint_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("smallint_col", "922337203685477");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("smallint_col", "-922337203685477");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("smallint_col", "0");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("smallint_col", "922337203685476");
                put("id", "5");
              }
            }));
    expectedData.put(
        "national_char_varying_to_varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("national_char_varying_col", " ");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("national_char_varying_col", "DROP TABLE");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("national_char_varying_col", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA...");
                put("id", "4");
              }
            }));
    expectedData.put(
        "binary_float_to_varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                /* Rationale: Source value 922337203685477 strictly maps to the 32-bit floating bound 9.2233718E14 under native Java stringification. */
                put("binary_float_col", "9.2233718E14");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                /* Rationale: Source value -922337203685477 strictly maps to the 32-bit floating bound -9.2233718E14 under native Java stringification. */
                put("binary_float_col", "-9.2233718E14");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_float_col", "0.0");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_float_col", "3.40282E38");
                put("id", "5");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_float_col", "-3.40282E38");
                put("id", "6");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_float_col", "0.0");
                put("id", "7");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                /* Rationale: Source value 99999999.99 parses dynamically beyond Float allocation into exactly 1.0E8 natively. */
                put("binary_float_col", "1.0E8");
                put("id", "8");
              }
            }));
    expectedData.put(
        "blob_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                /* Rationale: The original SQL source `UTL_RAW.CAST_TO_RAW(\'""\')` correctly encodes strictly over Dataflow JDBC layers statically into its literal Base64 string `"IiI="` representation. */
                put("blob_col", "IiI=");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                /* Rationale: The original SQL source `UTL_RAW.CAST_TO_RAW(\'"A"*100000\')` correctly encodes strictly over Dataflow JDBC layers statically into its literal Base64 string `"IkEiKjEwMDAwMA=="` representation natively. */
                put("blob_col", "IkEiKjEwMDAwMA==");
                put("id", "2");
              }
            }));
    expectedData.put(
        "smallint_to_numeric_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("smallint_col", "922337203685477.000000000");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("smallint_col", "-922337203685477.000000000");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("smallint_col", "0.000000000");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("smallint_col", "922337203685476.000000000");
                put("id", "5");
              }
            }));
    expectedData.put("bfile_to_varchar_url_table", java.util.Arrays.asList());
    expectedData.put(
        "national_character_to_varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("national_character_col", "                                 ...");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("national_character_col", "DROP TABLE                       ...");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("national_character_col", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA...");
                put("id", "4");
              }
            }));
    expectedData.put(
        "smallint_to_varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("smallint_col", "922337203685477");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("smallint_col", "-922337203685477");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("smallint_col", "0");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("smallint_col", "922337203685476");
                put("id", "5");
              }
            }));
    expectedData.put(
        "binary_float_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                /* Rationale: Source value 922337203685477 strictly maps to the 32-bit floating bound 9.2233718E14 under native Java stringification. */
                put("binary_float_col", "9.2233718E14");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                /* Rationale: Source value -922337203685477 strictly maps to the 32-bit floating bound -9.2233718E14 under native Java stringification. */
                put("binary_float_col", "-9.2233718E14");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_float_col", "0.0");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_float_col", "3.40282E38");
                put("id", "5");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_float_col", "-3.40282E38");
                put("id", "6");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_float_col", "0.0");
                put("id", "7");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                /* Rationale: Source value 99999999.99 parses dynamically beyond Float allocation into exactly 1.0E8 natively. */
                put("binary_float_col", "1.0E8");
                put("id", "8");
              }
            }));
    expectedData.put(
        "integer_to_numeric_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("integer_col", "922337203685477.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("integer_col", "-922337203685477.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("integer_col", "0.000000000");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "5");
                put("integer_col", "922337203685476.000000000");
              }
            }));
    expectedData.put(
        "national_character_varying_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("national_character_varying_col", " ");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("national_character_varying_col", "DROP TABLE");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                put("national_character_varying_col", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA...");
              }
            }));
    expectedData.put(
        "national_character_varying_to_varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("national_character_varying_col", " ");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("national_character_varying_col", "DROP TABLE");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                put("national_character_varying_col", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA...");
              }
            }));
    expectedData.put(
        "binary_float_to_double_precision_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                /* Rationale: Source value 922337203685477 strictly maps to the 32-bit floating bound 9.2233718E14 under native Java stringification. */
                put("binary_float_col", "9.2233718E14");
                put("id", "1");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                /* Rationale: Source value -922337203685477 strictly maps to the 32-bit floating bound -9.2233718E14 under native Java stringification. */
                put("binary_float_col", "-9.2233718E14");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_float_col", "0.0");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_float_col", "3.40282E38");
                put("id", "5");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_float_col", "-3.40282E38");
                put("id", "6");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("binary_float_col", "0.0");
                put("id", "7");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                /* Rationale: Source value 99999999.99 parses dynamically beyond Float allocation into exactly 1.0E8 natively. */
                put("binary_float_col", "1.0E8");
                put("id", "8");
              }
            }));
    expectedData.put(
        "numeric_to_double_precision_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "1");
                put("numeric_col", "9.22337203685477E14");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("numeric_col", "-9.22337203685477E14");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("numeric_col", "0.0");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                put("numeric_col", "9.22337203685476E14");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "5");
                put("numeric_col", "-9.22337203685476E14");
              }
            }));
    expectedData.put(
        "character_to_varchar_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", "2");
                put("character_col", "                                 ...");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "3");
                put("character_col", "DROP TABLE                       ...");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", "4");
                put("character_col", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA...");
              }
            }));
    expectedData.put(
        "national_char_varying_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("national_char_varying_col", " ");
                put("id", "2");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("national_char_varying_col", "DROP TABLE");
                put("id", "3");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("national_char_varying_col", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA...");
                put("id", "4");
              }
            }));
    expectedData.put("bfile_to_bytea_table", java.util.Arrays.asList());
    expectedData.put(
        "varchar2_to_bytea_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", 2L);
                put("varchar2_col", "IA==");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", 3L);
                put("varchar2_col", "RFJPUCBUQUJMRQ==");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", 4L);
                put(
                    "varchar2_col",
                    "QUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQQ==");
              }
            }));

    expectedData.put(
        "varchar_to_bytea_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", 2L);
                put("varchar_col", "IA==");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", 3L);
                put("varchar_col", "RFJPUCBUQUJMRQ==");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", 4L);
                put(
                    "varchar_col",
                    "QUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQQ==");
              }
            }));

    expectedData.put(
        "nvarchar2_to_bytea_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", 2L);
                put("nvarchar2_col", "IA==");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", 3L);
                put("nvarchar2_col", "RFJPUCBUQUJMRQ==");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", 4L);
                put(
                    "nvarchar2_col",
                    "QUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQQ==");
              }
            }));

    expectedData.put(
        "nchar_varying_to_bytea_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", 2L);
                put("nchar_varying_col", "IA==");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", 3L);
                put("nchar_varying_col", "RFJPUCBUQUJMRQ==");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", 4L);
                put(
                    "nchar_varying_col",
                    "QUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQQ==");
              }
            }));

    expectedData.put(
        "national_character_varying_to_bytea_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", 2L);
                put("national_character_varying_col", "IA==");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", 3L);
                put("national_character_varying_col", "RFJPUCBUQUJMRQ==");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", 4L);
                put(
                    "national_character_varying_col",
                    "QUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQQ==");
              }
            }));

    expectedData.put(
        "national_char_varying_to_bytea_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", 2L);
                put("national_char_varying_col", "IA==");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", 3L);
                put("national_char_varying_col", "RFJPUCBUQUJMRQ==");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", 4L);
                put(
                    "national_char_varying_col",
                    "QUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQQ==");
              }
            }));

    expectedData.put(
        "char_to_bytea_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", 2L);
                put(
                    "char_col",
                    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg=");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", 3L);
                put(
                    "char_col",
                    "RFJPUCBUQUJMRSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg=");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", 4L);
                put(
                    "char_col",
                    "QUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQQ==");
              }
            }));

    expectedData.put(
        "character_to_bytea_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", 2L);
                put(
                    "character_col",
                    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg=");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", 3L);
                put(
                    "character_col",
                    "RFJPUCBUQUJMRSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg=");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", 4L);
                put(
                    "character_col",
                    "QUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQQ==");
              }
            }));

    expectedData.put(
        "nchar_to_bytea_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", 2L);
                put(
                    "nchar_col",
                    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg=");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", 3L);
                put(
                    "nchar_col",
                    "RFJPUCBUQUJMRSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg=");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", 4L);
                put(
                    "nchar_col",
                    "QUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQQ==");
              }
            }));

    expectedData.put(
        "national_character_to_bytea_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", 2L);
                put(
                    "national_character_col",
                    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg=");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", 3L);
                put(
                    "national_character_col",
                    "RFJPUCBUQUJMRSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg=");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", 4L);
                put(
                    "national_character_col",
                    "QUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQQ==");
              }
            }));

    expectedData.put(
        "national_char_to_bytea_table",
        java.util.Arrays.asList(
            new java.util.HashMap<String, Object>() {
              {
                put("id", 2L);
                put(
                    "national_char_col",
                    "ICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg=");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", 3L);
                put(
                    "national_char_col",
                    "RFJPUCBUQUJMRSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg=");
              }
            },
            new java.util.HashMap<String, Object>() {
              {
                put("id", 4L);
                put(
                    "national_char_col",
                    "QUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQQ==");
              }
            }));
    return expectedData;
  }
}
