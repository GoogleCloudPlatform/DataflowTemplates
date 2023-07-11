/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.templates;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.neo4j.model.InputValidator;
import com.google.cloud.teleport.v2.neo4j.model.connection.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.model.helpers.JobSpecMapper;
import com.google.cloud.teleport.v2.neo4j.model.job.JobSpec;
import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.model.job.Source;
import com.google.cloud.teleport.v2.neo4j.providers.Provider;
import com.google.cloud.teleport.v2.neo4j.providers.ProviderFactory;
import com.google.cloud.teleport.v2.neo4j.providers.text.TextImpl;
import com.google.cloud.teleport.v2.neo4j.utils.ModelUtils;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unit tests for {@link GoogleCloudToNeo4j}. */
@RunWith(JUnit4.class)
public class GoogleToNeo4jTest {

  private static final Logger LOG = LoggerFactory.getLogger(GoogleToNeo4jTest.class);

  private static Provider providerImpl;
  private static ConnectionParams neo4jConnection;
  private static JobSpec jobSpec;
  private static OptionsParams optionsParams;

  @BeforeClass
  public static void setUp() {
    LOG.info("Initializing...");
    neo4jConnection =
        new ConnectionParams("src/test/resources/testing-specs/auradb-free-connection.json");
    jobSpec = JobSpecMapper.fromUri("src/test/resources/testing-specs/text-northwind-jobspec.json");
    providerImpl = ProviderFactory.of(jobSpec.getSourceList().get(0).getSourceType());
    optionsParams = new OptionsParams();
    optionsParams.overlayTokens("{\"limit\":7}");
    providerImpl.configure(optionsParams, jobSpec);
  }

  @Test
  public void testValidateSourceType() {
    assertThat(providerImpl.getClass()).isEqualTo(TextImpl.class);
  }

  @Test
  public void testValidJobSpec() {
    List<String> sourceValidationMessages = providerImpl.validateJobSpec();
    assertThat(sourceValidationMessages).isEmpty();
  }

  @Test
  public void testResolvedVariable() {
    assertThat(optionsParams.getTokenMap().get("limit")).isEqualTo("7");
  }

  @Test
  public void testResolvedSqlVariable() {
    String uri = "SELECT * FROM TEST LIMIT $limit";
    String uriReplaced = ModelUtils.replaceVariableTokens(uri, optionsParams.getTokenMap());
    LOG.info("uri: {}, uri_replaced: {}", uri, uriReplaced);
    assertThat(uriReplaced).contains("LIMIT 7");
  }

  @Test
  public void testGetInvalidOrderQuery() {
    Source source = jobSpec.getSourceList().get(0);
    source.setQuery("SELECT * FROM FOO ORDER BY X");
    List<String> messages = InputValidator.validateJobSpec(jobSpec);
    assertThat(messages).hasSize(1);
    assertThat(messages.get(0)).contains("SQL contains ORDER BY which is not supported");
  }
}
