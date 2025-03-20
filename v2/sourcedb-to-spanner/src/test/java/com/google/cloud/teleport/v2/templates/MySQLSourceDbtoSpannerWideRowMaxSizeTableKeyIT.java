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
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MySQLSourceDbtoSpannerWideRowMaxSizeTableKeyIT extends SourceDbToSpannerITBase {
  /**
   * CREATE TABLE test_key_size ( id INT AUTO_INCREMENT PRIMARY KEY, col1 VARCHAR(150) NOT NULL,
   * col2 VARCHAR(150) NOT NULL, col3 VARCHAR(150) NOT NULL, col4 VARCHAR(150) NOT NULL, col5
   * VARCHAR(150) NOT NULL, UNIQUE KEY idx_large (col1, col2, col3, col4,col5) )
   */
  private static PipelineLauncher.LaunchInfo jobInfo;

  private static final Integer MAX_CHARACTER_SIZE = 150;
  private static final String TABLENAME = "test_key_size";
  private static MySQLResourceManager mySQLResourceManager;
  private static SpannerResourceManager spannerResourceManager;
  private static String MYSQL_DDL_RESOURCE = "";
  private static String SPANNER_DDL_RESOURCE = "";

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
  }
}
