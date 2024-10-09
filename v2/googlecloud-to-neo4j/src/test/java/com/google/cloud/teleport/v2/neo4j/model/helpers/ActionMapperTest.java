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
package com.google.cloud.teleport.v2.neo4j.model.helpers;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.junit.Test;
import org.neo4j.importer.v1.actions.Action;
import org.neo4j.importer.v1.actions.ActionStage;
import org.neo4j.importer.v1.actions.BigQueryAction;
import org.neo4j.importer.v1.actions.CypherAction;
import org.neo4j.importer.v1.actions.CypherExecutionMode;
import org.neo4j.importer.v1.actions.HttpAction;
import org.neo4j.importer.v1.actions.HttpMethod;

@SuppressWarnings("deprecation")
public class ActionMapperTest {

  @Test
  public void parses_minimal_HTTP_GET_action() {
    var json =
        new JSONArray(
            List.of(
                Map.of(
                    "type", "http_get",
                    "name", "an-action",
                    "options", List.of(Map.of("url", "https://example.com")))));

    List<Action> actions = ActionMapper.parse(json, new OptionsParams());

    var expectedAction =
        new HttpAction(
            true, "an-action", ActionStage.END, "https://example.com", HttpMethod.GET, null);
    assertThat(actions).isEqualTo(List.of(expectedAction));
  }

  @Test
  public void parses_HTTP_GET_action() {
    var json =
        new JSONArray(
            List.of(
                Map.of(
                    "type",
                    "HTTP_GET",
                    "name",
                    "an-action",
                    "options",
                    List.of(Map.of("url", "https://example.$ext")),
                    "headers",
                    List.of(Map.of("header1", "$secret"), Map.of("header2", "another-value")))));
    OptionsParams options = new OptionsParams();
    options.overlayTokens("{\"ext\": \"com\", \"secret\": \"a-secret-value\"}");

    List<Action> actions = ActionMapper.parse(json, options);

    var expectedAction =
        new HttpAction(
            true,
            "an-action",
            ActionStage.END,
            "https://example.com",
            HttpMethod.GET,
            Map.of("header1", "a-secret-value", "header2", "another-value"));
    assertThat(actions).isEqualTo(List.of(expectedAction));
  }

  @Test
  public void parses_minimal_HTTP_POST_action() {
    var json =
        new JSONArray(
            List.of(
                Map.of(
                    "type", "http_post",
                    "name", "an-action",
                    "options", List.of(Map.of("url", "https://example.com")))));

    List<Action> actions = ActionMapper.parse(json, new OptionsParams());

    var expectedAction =
        new HttpAction(
            true, "an-action", ActionStage.END, "https://example.com", HttpMethod.POST, null);
    assertThat(actions).isEqualTo(List.of(expectedAction));
  }

  @Test
  public void parses_HTTP_POST_action() {
    var json =
        new JSONArray(
            List.of(
                Map.of(
                    "active",
                    false,
                    "type",
                    "HTTP_POST",
                    "name",
                    "an-action",
                    "options",
                    List.of(Map.of("url", "https://example.$ext")),
                    "headers",
                    List.of(Map.of("header1", "$secret"), Map.of("header2", "another-value")))));
    OptionsParams options = new OptionsParams();
    options.overlayTokens("{\"ext\": \"com\", \"secret\": \"a-secret-value\"}");

    List<Action> actions = ActionMapper.parse(json, options);

    var expectedAction =
        new HttpAction(
            false,
            "an-action",
            ActionStage.END,
            "https://example.com",
            HttpMethod.POST,
            Map.of("header1", "a-secret-value", "header2", "another-value"));
    assertThat(actions).isEqualTo(List.of(expectedAction));
  }

  @Test
  public void parses_minimal_BigQuery_action() {
    var json =
        new JSONArray(
            List.of(
                Map.of(
                    "type", "bigquery",
                    "name", "an-action",
                    "options",
                        List.of(
                            Map.of(
                                "sql",
                                "SELECT name, description FROM placeholder_table LIMIT 42")))));

    List<Action> actions = ActionMapper.parse(json, new OptionsParams());

    var expectedAction =
        new BigQueryAction(
            true,
            "an-action",
            ActionStage.END,
            "SELECT name, description FROM placeholder_table LIMIT 42");
    assertThat(actions).isEqualTo(List.of(expectedAction));
  }

  @Test
  public void parses_BigQuery_action() {
    var json =
        new JSONArray(
            List.of(
                Map.of(
                    "active",
                    false,
                    "type",
                    "BIGQUERY",
                    "name",
                    "an-action",
                    "options",
                    List.of(Map.of("sql", "SELECT name, description FROM $table LIMIT $limit")))));
    OptionsParams options = new OptionsParams();
    options.overlayTokens("{\"table\": \"placeholder_table\", \"limit\": 42}");

    List<Action> actions = ActionMapper.parse(json, options);

    var expectedAction =
        new BigQueryAction(
            false,
            "an-action",
            ActionStage.END,
            "SELECT name, description FROM placeholder_table LIMIT 42");
    assertThat(actions).isEqualTo(List.of(expectedAction));
  }

  @Test
  public void parses_minimal_Cypher_action() {
    var json =
        new JSONArray(
            List.of(
                Map.of(
                    "type", "cypher",
                    "name", "an-action",
                    "options",
                        List.of(
                            Map.of(
                                "cypher",
                                "MATCH (p:Placeholder) RETURN p.name, p.description LIMIT 42")))));

    List<Action> actions = ActionMapper.parse(json, new OptionsParams());

    var expectedAction =
        new CypherAction(
            true,
            "an-action",
            ActionStage.END,
            "MATCH (p:Placeholder) RETURN p.name, p.description LIMIT 42",
            CypherExecutionMode.AUTOCOMMIT);
    assertThat(actions).isEqualTo(List.of(expectedAction));
  }

  @Test
  public void parses_Cypher_action() {
    var json =
        new JSONArray(
            List.of(
                Map.of(
                    "active",
                    false,
                    "type",
                    "CYPHER",
                    "name",
                    "an-action",
                    "options",
                    List.of(
                        Map.of(
                            "cypher",
                            "MATCH (p:$label) RETURN p.name, p.description LIMIT $limit")))));
    OptionsParams options = new OptionsParams();
    options.overlayTokens("{\"label\": \"Placeholder\", \"limit\": 42}");

    List<Action> actions = ActionMapper.parse(json, options);

    var expectedAction =
        new CypherAction(
            false,
            "an-action",
            ActionStage.END,
            "MATCH (p:Placeholder) RETURN p.name, p.description LIMIT 42",
            CypherExecutionMode.AUTOCOMMIT);
    assertThat(actions).isEqualTo(List.of(expectedAction));
  }
}
