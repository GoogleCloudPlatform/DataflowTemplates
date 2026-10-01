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

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * An integration test for {@link SourceDbToSpanner} Flex template which tests all data types
 * migration against MySQL 5.7.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class MySQL57DataTypesIT extends MySQLDataTypesIT {

  private static final String MYSQL_5_7_CONTAINER_TAG = "5.7.44";

  @Before
  @Override
  public void setUp() throws Exception {
    MySQLResourceManager.Builder builder = MySQLResourceManager.builder(testName);
    builder.setContainerImageTag(MYSQL_5_7_CONTAINER_TAG);
    mySQLResourceManager = builder.build();
    spannerResourceManager = setUpSpannerResourceManager();
  }
}
