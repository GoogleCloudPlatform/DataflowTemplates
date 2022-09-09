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
import com.google.cloud.teleport.v2.neo4j.model.enums.SaveMode;
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.job.Aggregation;
import com.google.cloud.teleport.v2.neo4j.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;

/** Helper class for parsing json into Target model object. */
public class TargetMapper {

  public static Target fromJson(JSONObject targetObj) {
    Target target = new Target();
    if (targetObj.has("node")) {
      target.setType(TargetType.node);
      parseMappingsObject(target, targetObj.getJSONObject("node"));
    } else if (targetObj.has("edge")) {
      target.setType(TargetType.edge);
      parseMappingsObject(target, targetObj.getJSONObject("edge"));
    } else {
      target.setType(TargetType.valueOf(targetObj.getString("type")));
      parseMappingsArray(target, targetObj);
    }
    return target;
  }

  private static void parseMappingsObject(Target target, JSONObject targetObj) {
    parseHeader(target, targetObj);
    List<Mapping> mappings =
        TransposedMappingMapper.parseMappings(target, targetObj.getJSONObject("mappings"));
    for (Mapping mapping : mappings) {
      addMapping(target, mapping);
    }
  }

  public static void parseMappingsArray(Target target, JSONObject targetObj) {

    parseHeader(target, targetObj);
    JSONArray mappingsArray = targetObj.getJSONArray("mappings");
    for (int i = 0; i < mappingsArray.length(); i++) {
      JSONObject mappingObj = mappingsArray.getJSONObject(i);
      addMapping(target, VerboseMappingMapper.fromJsonObject(mappingObj));
    }
  }

  private static void addMapping(Target target, Mapping mapping) {
    target.getMappings().add(mapping);
    if (mapping.getField() != null) {
      target.getMappingByFieldMap().put(mapping.getField(), mapping);
      target.getFieldNames().add(mapping.getField());
    }
  }

  private static void parseHeader(Target target, JSONObject targetObj) {
    target.setName(targetObj.getString("name"));
    target.setActive(!targetObj.has("active") || targetObj.getBoolean("active"));
    target.setSaveMode(SaveMode.valueOf(targetObj.getString("mode")));
    target.setSource(targetObj.has("source") ? targetObj.getString("source") : "");
    target.setAutoMap(!targetObj.has("automap") || targetObj.getBoolean("automap"));
    if (targetObj.has("execute_after")) {
      target.setExecuteAfter(ActionExecuteAfter.valueOf(targetObj.getString("execute_after")));
    } else {
      if (target.getType() == TargetType.node) {
        // this will not wait for anything...
        target.setExecuteAfter(ActionExecuteAfter.sources);
      } else if (target.getType() == TargetType.edge) {
        target.setExecuteAfter(ActionExecuteAfter.nodes);
      }
    }
    target.setExecuteAfterName(
        targetObj.has("execute_after_name") ? targetObj.getString("execute_after_name") : "");

    if (targetObj.has("transform")) {
      JSONObject queryObj = targetObj.getJSONObject("transform");
      if (queryObj.has("aggregations")) {
        List<Aggregation> aggregations = new ArrayList<>();
        JSONArray aggregationsArray = queryObj.getJSONArray("aggregations");
        for (int i = 0; i < aggregationsArray.length(); i++) {
          JSONObject aggregationObj = aggregationsArray.getJSONObject(i);
          Aggregation agg = new Aggregation();
          agg.setExpression(aggregationObj.getString("expr"));
          agg.setField(aggregationObj.getString("field"));
          aggregations.add(agg);
        }
        target.getTransform().setAggregations(aggregations);
      }
      target.getTransform().setGroup(queryObj.has("group") && queryObj.getBoolean("group"));
      target
          .getTransform()
          .setOrderBy(queryObj.has("order_by") ? queryObj.getString("order_by") : "");
      target.getTransform().setLimit(queryObj.has("limit") ? queryObj.getInt("limit") : -1);
      target.getTransform().setWhere(queryObj.has("where") ? queryObj.getString("where") : "");
    }
  }
}
