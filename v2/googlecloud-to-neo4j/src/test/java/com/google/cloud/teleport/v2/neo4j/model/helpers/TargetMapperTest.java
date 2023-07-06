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

import com.google.cloud.teleport.v2.neo4j.model.enums.EdgeNodesMatchMode;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import java.util.List;
import java.util.UUID;
import org.json.JSONObject;
import org.junit.Test;

public class TargetMapperTest {

  @Test
  public void setsSpecifiedMatchModeForEdgeTargets() {
    JSONObject object = targetObject("edge", "merge");

    Target target = TargetMapper.fromJson(object);

    assertThat(target.getEdgeNodesMatchMode()).isEqualTo(EdgeNodesMatchMode.merge);
  }

  @Test // TODO: discuss this - set value and reject when validation kicks in on the job spec?
  public void ignoresSpecifiedMatchModeForEdgeTargetsInAppendMode() {
    JSONObject object = targetObject("node", "match");
    object.put("mode", "append");

    Target target = TargetMapper.fromJson(object);

    assertThat(target.getEdgeNodesMatchMode()).isNull();
  }

  @Test // TODO: discuss this - set value and reject when validation kicks in on the job spec?
  public void ignoresSpecifiedMatchModeForNodeTargets() {
    JSONObject object = targetObject("node", "match");

    Target target = TargetMapper.fromJson(object);

    assertThat(target.getEdgeNodesMatchMode()).isNull();
  }

  @Test
  public void setsDefaultMatchModeForEdgeTargets() {
    JSONObject object = targetObject("edge");

    Target target = TargetMapper.fromJson(object);

    assertThat(target.getEdgeNodesMatchMode()).isEqualTo(EdgeNodesMatchMode.match);
  }

  @Test
  public void doesNotSetDefaultMatchModeForNodeTargets() {
    JSONObject object = targetObject("node");

    Target target = TargetMapper.fromJson(object);

    assertThat(target.getEdgeNodesMatchMode()).isNull();
  }

  private static JSONObject targetObject(String type, String edgeNodesMatchMode) {
    JSONObject object = targetObject(type);
    object.put("edgeNodesMatchMode", edgeNodesMatchMode);
    return object;
  }

  private static JSONObject targetObject(String type) {
    JSONObject object = new JSONObject();
    object.put("name", UUID.randomUUID().toString());
    object.put("mode", "merge");
    object.put("type", type);
    object.put("mappings", List.of());
    return object;
  }
}
