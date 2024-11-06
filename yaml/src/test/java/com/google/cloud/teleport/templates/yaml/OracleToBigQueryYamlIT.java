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
package com.google.cloud.teleport.templates.yaml;

import com.google.cloud.teleport.metadata.YAMLTemplateIntegrationTest;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.OracleResourceManager;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(YAMLTemplateIntegrationTest.class)
@RunWith(JUnit4.class)
public class OracleToBigQueryYamlIT extends JdbcToBigQueryYamlBase {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcToBigQueryYamlIT.class);

  @Test
  public void testOracleToBigQueryYaml() throws IOException {
    // Oracle image does not work on M1
    if (System.getProperty("testOnM1") != null) {
      LOG.info("M1 is being used, Oracle tests are not being executed.");
      return;
    }

    // Create oracle Resource manager
    oracleResourceManager = OracleResourceManager.builder(testName).build();

    // Arrange oracle-compatible schema
    JDBCResourceManager.JDBCSchema schema = getOracleSchema();

    // Run a simple IT
    simpleJdbcToBigQueryTest(
        schema, oracleResourceManager, true, Map.of("query", getQueryString()));
  }
}
