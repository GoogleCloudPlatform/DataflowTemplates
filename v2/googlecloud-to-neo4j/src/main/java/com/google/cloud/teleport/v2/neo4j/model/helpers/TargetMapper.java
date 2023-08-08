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
package com.google.cloud.teleport.v2.neo4j.model.helpers;

import com.google.cloud.teleport.v2.neo4j.model.enums.ActionExecuteAfter;
import com.google.cloud.teleport.v2.neo4j.model.enums.EdgeNodesMatchMode;
import com.google.cloud.teleport.v2.neo4j.model.enums.SaveMode;
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.job.Aggregation;
import com.google.cloud.teleport.v2.neo4j.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import com.google.cloud.teleport.v2.neo4j.model.job.Transform;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.json.JSONArray;
import org.json.JSONObject;

/** Helper class for parsing json into Target model object. */
public class TargetMapper {

  public static Target fromJson(JSONObject targetObj) {
    Target target = new Target();
    if (targetObj.has("node")) {
      target.setType(TargetType.node);
      parseNodeMappingsObject(target, targetObj.getJSONObject("node"));
      return target;
    }
    if (targetObj.has("edge")) {
      target.setType(TargetType.edge);
      parseEdgeMappingsObject(target, targetObj.getJSONObject("edge"));
      return target;
    }
    if (targetObj.has("custom")) {
      target.setType(TargetType.custom);
      parseCustomQueryMappingsObject(target, targetObj.getJSONObject("custom"));
      return target;
    }
    String error =
        String.format(
            "Expected target JSON to have one of: \"%s\" as top-level field, but only found fields: \"%s\"",
            Arrays.stream(TargetType.values())
                .map(TargetType::name)
                .collect(Collectors.joining("\", \"")),
            String.join("\", \"", targetObj.keySet()));
    throw new IllegalArgumentException(error);
  }

  private static void parseNodeMappingsObject(Target target, JSONObject json) {
    parseCommonHeader(target, json);
    parseAggregations(target, json);
    target.setExecuteAfter(
        !json.has("execute_after")
            ? ActionExecuteAfter.sources
            : ActionExecuteAfter.valueOf(json.getString("execute_after")));
    target.setSaveMode(SaveMode.valueOf(json.getString("mode")));
    addMappings(target, json.getJSONObject("mappings"));
  }

  private static void parseEdgeMappingsObject(Target target, JSONObject json) {
    parseCommonHeader(target, json);
    parseAggregations(target, json);
    target.setExecuteAfter(
        !json.has("execute_after")
            ? ActionExecuteAfter.nodes
            : ActionExecuteAfter.valueOf(json.getString("execute_after")));
    target.setSaveMode(SaveMode.valueOf(json.getString("mode")));
    target.setEdgeNodesMatchMode(
        !json.has("edge_nodes_match_mode")
            ? EdgeNodesMatchMode.match
            : EdgeNodesMatchMode.valueOf(json.getString("edge_nodes_match_mode")));
    addMappings(target, json.getJSONObject("mappings"));
  }

  private static void parseCustomQueryMappingsObject(Target target, JSONObject json) {
    parseCommonHeader(target, json);
    target.setExecuteAfter(
        !json.has("execute_after")
            ? ActionExecuteAfter.edges
            : ActionExecuteAfter.valueOf(json.getString("execute_after")));
    target.setCustomQuery(json.getString("query"));
  }

  private static void addMappings(Target target, JSONObject jsonMappings) {
    List<Mapping> mappings = MappingMapper.parseMappings(target, jsonMappings);
    for (Mapping mapping : mappings) {
      addMapping(target, mapping);
    }
  }

  private static void addMapping(Target target, Mapping mapping) {
    target.getMappings().add(mapping);
    if (mapping.getField() != null) {
      target.getMappingByFieldMap().put(mapping.getField(), mapping);
      target.getFieldNames().add(mapping.getField());
    }
  }

  private static void parseCommonHeader(Target target, JSONObject json) {
    target.setName(json.getString("name"));
    target.setActive(!json.has("active") || json.getBoolean("active"));
    target.setSource(json.has("source") ? json.getString("source") : "");
    target.setExecuteAfterName(
        json.has("execute_after_name") ? json.getString("execute_after_name") : "");
  }

  private static void parseAggregations(Target target, JSONObject json) {
    if (!json.has("transform")) {
      return;
    }
    JSONObject jsonTransform = json.getJSONObject("transform");
    Transform transform = target.getTransform();
    transform.setGroup(jsonTransform.has("group") && jsonTransform.getBoolean("group"));
    transform.setOrderBy(jsonTransform.has("order_by") ? jsonTransform.getString("order_by") : "");
    transform.setLimit(jsonTransform.has("limit") ? jsonTransform.getInt("limit") : -1);
    transform.setWhere(jsonTransform.has("where") ? jsonTransform.getString("where") : "");
    if (!jsonTransform.has("aggregations")) {
      return;
    }
    List<Aggregation> aggregations = new ArrayList<>();
    JSONArray aggregationsArray = jsonTransform.getJSONArray("aggregations");
    for (int i = 0; i < aggregationsArray.length(); i++) {
      JSONObject aggregationObj = aggregationsArray.getJSONObject(i);
      Aggregation agg = new Aggregation();
      agg.setExpression(aggregationObj.getString("expr"));
      agg.setField(aggregationObj.getString("field"));
      aggregations.add(agg);
    }
    transform.setAggregations(aggregations);
  }
}
