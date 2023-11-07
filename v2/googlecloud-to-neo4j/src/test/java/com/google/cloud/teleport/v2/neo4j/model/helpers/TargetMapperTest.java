/*
 * Copyright (C) 2023 Google LLC
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
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.neo4j.model.enums.ActionExecuteAfter;
import com.google.cloud.teleport.v2.neo4j.model.enums.EdgeNodesSaveMode;
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import java.util.UUID;
import org.json.JSONObject;
import org.junit.Test;

public class TargetMapperTest {

  @Test
  public void parsesCustomQueryTarget() {
    JSONObject jsonTarget = jsonTargetOfType("custom_query");
    JSONObject customObject = jsonTarget.getJSONObject("custom_query");
    customObject.put("query", "UNWIND $rows AS row CREATE (:Node {prop: row.prop})");
    JSONObject mappings = new JSONObject();
    mappings.put("labels", "\"Ignored\"");
    customObject.put("mappings", mappings); // ignored
    JSONObject transform = new JSONObject();
    transform.put("group", true);
    customObject.put("transform", transform); // ignored

    Target target = TargetMapper.fromJson(jsonTarget);

    assertThat(target.getType()).isEqualTo(TargetType.custom_query);
    assertThat(target.getCustomQuery())
        .isEqualTo("UNWIND $rows AS row CREATE (:Node {prop: row.prop})");
    assertThat(target.getExecuteAfter()).isEqualTo(ActionExecuteAfter.edges);
    assertThat(target.getMappings()).isEmpty();
    assertThat(target.getTransform().isDefault()).isTrue();
  }

  @Test
  public void setsSpecifiedMatchModeForEdgeTargets() {
    JSONObject object = jsonTargetOfType("edge");
    JSONObject edgeObject = object.getJSONObject("edge");
    edgeObject.put("mode", "merge");
    edgeObject.put("edge_nodes_match_mode", "merge");

    Target target = TargetMapper.fromJson(object);

    assertThat(target.getEdgeNodesMatchMode()).isEqualTo(EdgeNodesSaveMode.merge);
  }

  @Test
  public void setsSpecifiedMatchModeForEdgeTargetsInAppendMode() {
    JSONObject object = jsonTargetOfType("edge");
    JSONObject edgeObject = object.getJSONObject("edge");
    edgeObject.put("mode", "append");
    edgeObject.put("edge_nodes_match_mode", "merge");

    Target target = TargetMapper.fromJson(object);

    assertThat(target.getEdgeNodesMatchMode()).isEqualTo(EdgeNodesSaveMode.merge);
  }

  @Test
  public void ignoresSpecifiedMatchModeForNodeTargets() {
    JSONObject object = jsonTargetOfType("node");
    JSONObject nodeObject = object.getJSONObject("node");
    nodeObject.put("mode", "merge");
    nodeObject.put("edge_nodes_match_mode", "match");

    Target target = TargetMapper.fromJson(object);

    assertThat(target.getEdgeNodesMatchMode()).isNull();
  }

  @Test
  public void setsDefaultNodeMatchModeForEdgeTargetsToMerge() {
    JSONObject object = jsonTargetOfType("edge");
    object.getJSONObject("edge").put("mode", "merge");

    Target target = TargetMapper.fromJson(object);

    assertThat(target.getEdgeNodesMatchMode()).isEqualTo(EdgeNodesSaveMode.match);
  }

  @Test
  public void setsDefaultNodeMatchModeForEdgeTargetsToCreate() {
    JSONObject object = jsonTargetOfType("edge");
    object.getJSONObject("edge").put("mode", "append");

    Target target = TargetMapper.fromJson(object);

    assertThat(target.getEdgeNodesMatchMode()).isEqualTo(EdgeNodesSaveMode.create);
  }

  @Test
  public void doesNotSetDefaultMatchModeForNodeTargets() {
    JSONObject object = jsonTargetOfType("node");
    object.getJSONObject("node").put("mode", "merge");

    Target target = TargetMapper.fromJson(object);

    assertThat(target.getEdgeNodesMatchMode()).isNull();
  }

  @Test
  public void rejectsInvalidTarget() {
    JSONObject invalidObject = jsonTargetOfType("invalid");

    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> TargetMapper.fromJson(invalidObject));
    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Expected target JSON to have one of: \"node\", \"edge\", \"custom_query\" as top-level field, but only found fields: \"invalid\"");
  }

  private static JSONObject jsonTargetOfType(String type) {
    JSONObject target = new JSONObject();
    target.put("name", UUID.randomUUID().toString());
    target.put("mappings", new JSONObject());
    JSONObject topLevelObject = new JSONObject();
    topLevelObject.put(type, target);
    return topLevelObject;
  }
}
