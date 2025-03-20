/*
 * Copyright (C) 2025 Google LLC
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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MySQLSourceDbToSpannerWideRowMaxSizeTableKeyIT extends SourceDbToSpannerITBase {

  private static PipelineLauncher.LaunchInfo jobInfo;
  private static final String TABLE_NAME = "test_key_size";
  private static final Integer MAX_CHARACTER_SIZE = 150;
  private static MySQLResourceManager mySQLResourceManager;
  private static SpannerResourceManager spannerResourceManager;
  private static final String MYSQL_DDL_RESOURCE =
      "WideRow/SourceDbToSpannerMaxSizeTableKey/mysql-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "WideRow/SourceDbToSpannerMaxSizeTableKey/spanner-schema.sql";

  private List<Map<String, Object>> getMySQLData() {
    List<Map<String, Object>> data = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Map<String, Object> values = new HashMap<>();
      values.put("id", i);
      values.put("col1", RandomStringUtils.randomAlphabetic(MAX_CHARACTER_SIZE));
      values.put("col2", RandomStringUtils.randomAlphabetic(MAX_CHARACTER_SIZE));
      values.put("col3", RandomStringUtils.randomAlphabetic(MAX_CHARACTER_SIZE));
      values.put("col4", RandomStringUtils.randomAlphabetic(MAX_CHARACTER_SIZE));
      values.put("col5", RandomStringUtils.randomAlphabetic(MAX_CHARACTER_SIZE));
      data.add(values);
    }
    return data;
  }

  @Before
  public void setUp() {
    mySQLResourceManager = setUpMySQLResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, mySQLResourceManager);
  }

  @Test
  public void maxSizeTableKeyTest() throws Exception {
    loadSQLFileResource(mySQLResourceManager, MYSQL_DDL_RESOURCE);
    mySQLResourceManager.write(TABLE_NAME, getMySQLData());
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            mySQLResourceManager,
            spannerResourceManager,
            null,
            null);

    PipelineOperator.Result result =
        pipelineOperator().waitUntilDone(createConfig(jobInfo, Duration.ofMinutes(35L)));
    assertThatResult(result).isLaunchFinished();

    SpannerAsserts.assertThatStructs(spannerResourceManager.readTableRecords(TABLE_NAME))
        .hasRecordsUnorderedCaseInsensitiveColumns(getMySQLData());
  }
}
