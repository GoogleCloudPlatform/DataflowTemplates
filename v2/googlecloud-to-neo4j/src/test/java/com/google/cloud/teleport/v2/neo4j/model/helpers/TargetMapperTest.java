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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.neo4j.importer.v1.targets.NodeMatchMode;
import org.neo4j.importer.v1.targets.Order;
import org.neo4j.importer.v1.targets.OrderBy;
import org.neo4j.importer.v1.targets.Targets;

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

    Targets targets = TargetMapper.fromJson(arrayOf(jsonTarget));

    assertThat(targets.getNodes()).isEmpty();
    assertThat(targets.getRelationships()).isEmpty();
    assertThat(targets.getCustomQueries()).hasSize(1);
    var target = targets.getCustomQueries().get(0);
    assertThat(target.getQuery()).isEqualTo("UNWIND $rows AS row CREATE (:Node {prop: row.prop})");
  }

  @Test
  public void setsSpecifiedMatchModeForEdgeTargets() {
    JSONObject object = jsonTargetOfType("edge");
    JSONObject edgeObject = object.getJSONObject("edge");
    edgeObject.put("mode", "merge");
    edgeObject.put("edge_nodes_match_mode", "merge");

    Targets targets = TargetMapper.fromJson(arrayOf(object));

    assertThat(targets.getRelationships()).hasSize(1);
    assertThat(targets.getRelationships().get(0).getNodeMatchMode()).isEqualTo(NodeMatchMode.MERGE);
  }

  @Test
  public void setsSpecifiedMatchModeForEdgeTargetsInAppendMode() {
    JSONObject object = jsonTargetOfType("edge");
    JSONObject edgeObject = object.getJSONObject("edge");
    edgeObject.put("mode", "append");
    edgeObject.put("edge_nodes_match_mode", "merge");

    Targets targets = TargetMapper.fromJson(arrayOf(object));

    assertThat(targets.getRelationships()).hasSize(1);
    assertThat(targets.getRelationships().get(0).getNodeMatchMode()).isEqualTo(NodeMatchMode.MERGE);
  }

  @Test
  public void setsDefaultNodeMatchModeForEdgeTargetsToMerge() {
    JSONObject object = jsonTargetOfType("edge");
    object.getJSONObject("edge").put("mode", "merge");

    Targets targets = TargetMapper.fromJson(arrayOf(object));

    assertThat(targets.getRelationships()).hasSize(1);
    assertThat(targets.getRelationships().get(0).getNodeMatchMode()).isEqualTo(NodeMatchMode.MATCH);
  }

  @Test
  public void setsDefaultNodeMatchModeForEdgeTargetsToCreate() {
    JSONObject object = jsonTargetOfType("edge");
    object.getJSONObject("edge").put("mode", "append");

    Targets targets = TargetMapper.fromJson(arrayOf(object));

    assertThat(targets.getRelationships()).hasSize(1);
    assertThat(targets.getRelationships().get(0).getNodeMatchMode())
        .isEqualTo(NodeMatchMode.CREATE);
  }

  @Test
  public void rejectsInvalidTarget() {
    JSONObject invalidObject = jsonTargetOfType("invalid");

    Exception exception =
        assertThrows(
            IllegalArgumentException.class, () -> TargetMapper.fromJson(arrayOf(invalidObject)));
    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Expected target JSON to have one of: \"node\", \"edge\", \"custom_query\" as top-level field, but only found fields: \"invalid\"");
  }

  @Test
  public void parsesSimpleOrderBy() {
    JSONObject orderBy = new JSONObject(Map.of("order_by", "col"));

    List<OrderBy> clauses = TargetMapper.parseOrderBy(orderBy);

    assertThat(clauses).isEqualTo(List.of(new OrderBy("col", null)));
  }

  @Test
  public void parsesSimpleListOfOrderBy() {
    JSONObject orderBy = new JSONObject(Map.of("order_by", "col, col2"));

    List<OrderBy> clauses = TargetMapper.parseOrderBy(orderBy);

    assertThat(clauses).isEqualTo(List.of(new OrderBy("col", null), new OrderBy("col2", null)));
  }

  @Test
  public void parsesOrderByWithDirection() {
    JSONObject orderBy = new JSONObject(Map.of("order_by", "col DESC"));

    List<OrderBy> clauses = TargetMapper.parseOrderBy(orderBy);

    assertThat(clauses).isEqualTo(List.of(new OrderBy("col", Order.DESC)));
  }

  @Test
  public void parsesOrderByListWithDirection() {
    JSONObject orderBy = new JSONObject(Map.of("order_by", "col DESC, col2 ASC"));

    List<OrderBy> clauses = TargetMapper.parseOrderBy(orderBy);

    assertThat(clauses)
        .isEqualTo(List.of(new OrderBy("col", Order.DESC), new OrderBy("col2", Order.ASC)));
  }

  private static JSONObject jsonTargetOfType(String type) {
    JSONObject target = new JSONObject();
    target.put("name", UUID.randomUUID().toString());
    target.put("mappings", new JSONObject());
    JSONObject topLevelObject = new JSONObject();
    topLevelObject.put(type, target);
    return topLevelObject;
  }

  private static JSONArray arrayOf(JSONObject jsonTarget) {
    return new JSONArray(List.of(jsonTarget));
  }
}
