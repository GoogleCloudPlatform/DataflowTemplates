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

import com.google.cloud.teleport.v2.neo4j.model.enums.EdgeNodesMatchMode;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import java.util.UUID;
import org.json.JSONObject;
import org.junit.Test;

public class TargetMapperTest {

  @Test
  public void setsSpecifiedMatchModeForEdgeTargets() {
    JSONObject object = targetOfType("edge");
    JSONObject edgeObject = object.getJSONObject("edge");
    edgeObject.put("mode", "merge");
    edgeObject.put("edge_nodes_match_mode", "merge");

    Target target = TargetMapper.fromJson(object);

    assertThat(target.getEdgeNodesMatchMode()).isEqualTo(EdgeNodesMatchMode.merge);
  }

  @Test
  public void setsSpecifiedMatchModeForEdgeTargetsInAppendMode() {
    JSONObject object = targetOfType("edge");
    JSONObject edgeObject = object.getJSONObject("edge");
    edgeObject.put("mode", "append");
    edgeObject.put("edge_nodes_match_mode", "merge");

    Target target = TargetMapper.fromJson(object);

    assertThat(target.getEdgeNodesMatchMode()).isEqualTo(EdgeNodesMatchMode.merge);
  }

  @Test
  public void ignoresSpecifiedMatchModeForNodeTargets() {
    JSONObject object = targetOfType("node");
    object.put("edge_nodes_match_mode", "match");

    Target target = TargetMapper.fromJson(object);

    assertThat(target.getEdgeNodesMatchMode()).isNull();
  }

  @Test
  public void setsDefaultMatchModeForEdgeTargets() {
    JSONObject object = targetOfType("edge");

    Target target = TargetMapper.fromJson(object);

    assertThat(target.getEdgeNodesMatchMode()).isEqualTo(EdgeNodesMatchMode.match);
  }

  @Test
  public void doesNotSetDefaultMatchModeForNodeTargets() {
    JSONObject object = targetOfType("node");

    Target target = TargetMapper.fromJson(object);

    assertThat(target.getEdgeNodesMatchMode()).isNull();
  }

  @Test
  public void rejectsInvalidTarget() {
    JSONObject invalidObject = targetOfType("invalid");

    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> TargetMapper.fromJson(invalidObject));
    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Expected target JSON to have top-level \"node\" or \"edge\" field, but found fields: \"invalid\"");
  }

  private static JSONObject targetOfType(String type) {
    JSONObject target = new JSONObject();
    target.put("name", UUID.randomUUID().toString());
    target.put("mode", "merge");
    target.put("mappings", new JSONObject());
    JSONObject topLevelObject = new JSONObject();
    topLevelObject.put(type, target);
    return topLevelObject;
  }
}
