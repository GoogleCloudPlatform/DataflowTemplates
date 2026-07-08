/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.actions;

import static com.google.cloud.teleport.v2.neo4j.actions.ActionProviderExtensions.getActionStage;
import static com.google.cloud.teleport.v2.neo4j.actions.ActionProviderExtensions.getActive;
import static com.google.cloud.teleport.v2.neo4j.actions.ActionProviderExtensions.getName;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import java.util.Locale;
import java.util.Map;
import org.neo4j.importer.v1.actions.ActionProvider;

public class HttpActionProvider implements ActionProvider<HttpAction> {

  private final YAMLMapper objectMapper;

  public HttpActionProvider() {
    this.objectMapper = new YAMLMapper();
  }

  @Override
  public String supportedType() {
    return "http";
  }

  @Override
  public HttpAction apply(ObjectNode objectNode) {
    return new HttpAction(
        getActive(objectNode),
        getName(objectNode),
        getActionStage(objectNode),
        objectNode.get("url").asText(""),
        parseMethodLeniently(objectNode),
        parseHeaders(objectNode));
  }

  private static HttpMethod parseMethodLeniently(ObjectNode objectNode) {
    if (!objectNode.has("method")) {
      return null;
    }
    JsonNode rawMethod = objectNode.get("method");
    if (!rawMethod.isTextual()) {
      return null;
    }
    try {
      return HttpMethod.valueOf(rawMethod.asText().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private Map<String, String> parseHeaders(ObjectNode objectNode) {
    return !objectNode.has("headers")
        ? Map.of()
        : objectMapper.convertValue(objectNode.get("headers"), new TypeReference<>() {});
  }
}
