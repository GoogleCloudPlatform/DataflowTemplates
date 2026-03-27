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

import com.google.cloud.teleport.v2.neo4j.model.helpers.OverlayTokenParser;
import com.google.cloud.teleport.v2.neo4j.model.helpers.StepSequence;
import com.google.cloud.teleport.v2.neo4j.model.job.OverlayTokens;
import com.google.cloud.teleport.v2.neo4j.model.sources.InlineTextSource;
import com.google.cloud.teleport.v2.neo4j.providers.SourceProvider;
import com.google.cloud.teleport.v2.neo4j.providers.SourceProviderFactory;
import com.google.cloud.teleport.v2.neo4j.providers.text.TextProvider;
import com.google.cloud.teleport.v2.neo4j.utils.ModelUtils;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link GoogleCloudToNeo4j}. */
@RunWith(JUnit4.class)
public class GoogleToNeo4jTest {

  private static SourceProvider providerImpl;
  private static OverlayTokens overlayTokens;

  @BeforeClass
  public static void setUp() {
    providerImpl =
        SourceProviderFactory.of(
            new InlineTextSource(
                "a-text-source",
                List.of(List.of("v1", "v2"), List.of("v3", "v4")),
                List.of("column1", "column2")),
            new StepSequence());
    overlayTokens = OverlayTokenParser.parse("{\"limit\":7}");
    providerImpl.configure(overlayTokens);
  }

  @Test
  public void validates_source_type() {
    assertThat(providerImpl.getClass()).isEqualTo(TextProvider.class);
  }

  @Test
  public void resolves_variable() {
    assertThat(overlayTokens.tokens().get("limit")).isEqualTo("7");
  }

  @Test
  public void resolves_sql_variable() {
    String uri = "SELECT * FROM TEST LIMIT $limit";
    String uriReplaced = ModelUtils.replaceVariableTokens(uri, overlayTokens.tokens());
    assertThat(uriReplaced).contains("LIMIT 7");
  }
}
