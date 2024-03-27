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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.importer.v1.actions.Action;
import org.neo4j.importer.v1.actions.BigQueryAction;
import org.neo4j.importer.v1.actions.CypherAction;
import org.neo4j.importer.v1.actions.CypherExecutionMode;
import org.neo4j.importer.v1.actions.HttpAction;
import org.neo4j.importer.v1.actions.HttpMethod;

/**
 * Helper class for parsing legacy json into {@link Action}.
 *
 * @deprecated use the current JSON format instead
 */
@Deprecated
public class ActionMapper {

  public static List<Action> fromJson(JSONArray json) {
    List<Action> actions = new ArrayList<>(json.length());
    for (int i = 0; i < json.length(); i++) {
      actions.add(fromJson(json.getJSONObject(i)));
    }
    return actions;
  }

  private static Action fromJson(JSONObject json) {
    String type = json.getString("type").toLowerCase(Locale.ROOT);
    boolean active = JsonObjects.getBooleanOrDefault(json, "active", true);
    String name = json.getString("name");
    var options = json.getJSONArray("options");
    if (options.length() != 1) {
      throw new IllegalArgumentException(
          String.format("Expected a single option for Cypher query, got %d", options.length()));
    }
    var option = options.getJSONObject(0);
    switch (type) {
      case "cypher":
        return new CypherAction(
            active,
            name,
            null, // TODO: stage
            option.getString("cypher"),
            CypherExecutionMode.AUTOCOMMIT);
      case "bigquery":
        return new BigQueryAction(
            active,
            name,
            null, // TODO: stage
            option.getString("sql"));
      case "http_get":
        return new HttpAction(
            active,
            name,
            null, // TODO: stage
            option.getString("url"),
            HttpMethod.GET,
            ensureStringValues(json.getJSONObject("headers").toMap()));
      case "http_post":
        return new HttpAction(
            active,
            name,
            null, // TODO: stage
            option.getString("url"),
            HttpMethod.POST,
            ensureStringValues(json.getJSONObject("headers").toMap()));
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported action type %s, expected one of: cypher, bigquery, http_get, http_post",
                type));
    }
  }

  private static Map<String, String> ensureStringValues(Map<String, Object> map) {
    Map<String, String> result = new HashMap<>(map.size());
    for (String key : map.keySet()) {
      result.put(key, (String) map.get("key"));
    }
    return result;
  }
}
