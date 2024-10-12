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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An integration test for {@link SourceDbToSpanner} Flex template which tests migrations using a
 * custom transformation jar.
 */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class CustomTransformationsNonShardedIT extends SourceDbToSpannerITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(CustomTransformationsNonShardedIT.class);
  private static final HashSet<IdentitySchemaMapperIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;

  public static MySQLResourceManager mySQLResourceManager;
  public static SpannerResourceManager spannerResourceManager;

  private static final String MYSQL_DDL_RESOURCE =
      "CustomTransformationsNonShardedIT/mysql-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "CustomTransformationsNonShardedIT/spanner-schema.sql";

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class. \
   */
  @Before
  public void setUp() {
    mySQLResourceManager = setUpMySQLResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
  }

  /** Cleanup dataflow job and all the resources and resource managers. */
  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, mySQLResourceManager);
  }

  @Test
  public void simpleTest() throws Exception {
    loadSQLFileResource(mySQLResourceManager, MYSQL_DDL_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    createAndUploadJarToGcs("CustomTransformationAllTypes");
    CustomTransformation customTransformation =
        CustomTransformation.builder(
                "customTransformation.jar", "com.custom.CustomTransformationWithShardForBulkIT")
            .build();
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            "CustomTransformationAllTypes",
            mySQLResourceManager,
            spannerResourceManager,
            null,
            customTransformation);
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));

    List<Map<String, Object>> events = new ArrayList<>();
    Map<String, Object> row = new HashMap<>();
    row.put("varchar_column", "id1");
    row.put("tinyint_column", 13);
    row.put("text_column", "This is a text value append");
    row.put("date_column", "2024-06-22");
    row.put("int_column", 101);
    row.put("bigint_column", 134567891);
    row.put("float_column", 4.14159);
    row.put("double_column", 3.71828);
    row.put("decimal_column", 12346.6789);
    row.put("datetime_column", "2024-06-21T17:10:00Z");
    row.put("timestamp_column", "2022-12-31T23:59:57Z");
    // TODO (b/349257952): update once TIME handling is made consistent for bulk and live.
    // row.put("time_column", "43200000000");
    row.put("year_column", "2025");
    row.put("blob_column", "V29ybWQ=");
    row.put("enum_column", "1");
    row.put("bool_column", true);
    row.put("varbinary_column", "AQIDBAUGBwgJCgsMDQ4PEBESExQ=");
    row.put("bit_column", "Ew==");
    row.put("binary_column", "AQIDBAUGBwgJCgsMDQ4PEBESExQ=");
    row.put("char_column", "newchar");
    row.put("longblob_column", "V29ybWQ=");
    row.put("longtext_column", "This is longtext append");
    row.put("mediumblob_column", "V29ybWQ=");
    row.put("mediumint_column", 2001);
    row.put("mediumtext_column", "This is mediumtext append");
    row.put("set_column", "v3");
    row.put("smallint_column", 11);
    row.put("tinyblob_column", "V29ybWQ=");
    row.put("tinytext_column", "This is tinytext append");
    row.put("json_column", "{\"k1\":\"v1\",\"k2\":\"v2\"}");

    events.add(row);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery(
                "SELECT varchar_column, tinyint_column, text_column, date_column, int_column, bigint_column, float_column, double_column, decimal_column, datetime_column, timestamp_column, year_column, blob_column, enum_column, bool_column, varbinary_column, bit_column, binary_column, char_column, longblob_column,"
                    + "longtext_column, mediumblob_column, mediumint_column, mediumtext_column, set_column, smallint_column,"
                    + "tinyblob_column, tinytext_column, json_column FROM AllDatatypeTransformation"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }
}
