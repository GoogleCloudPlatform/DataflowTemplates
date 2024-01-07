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
package com.google.cloud.teleport.v2.neo4j.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.networknt.schema.CustomErrorMessageType;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion.VersionFlag;
import com.networknt.schema.ValidationMessage;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Json {

  public static final JsonSchemaFactory SCHEMA_FACTORY =
      JsonSchemaFactory.getInstance(VersionFlag.V202012);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static ParsingResult parseAndValidate(String json, JsonSchema schema) {
    ParsingResult result = Json.parseNode(json);
    return result.flatMap(node -> ParsingResult.of(node, schema.validate(node)));
  }

  public static <T> T map(ParsingResult result, Class<T> type) {
    return MAPPER.convertValue(result.node, type);
  }

  private static ParsingResult parseNode(String json) {
    try {
      return ParsingResult.success(MAPPER.readTree(json));
    } catch (JsonProcessingException e) {
      return ParsingResult.failure(
          List.of(
              ValidationMessage.of(
                  "invalidJson",
                  CustomErrorMessageType.of("dataflow.invalidJSON"),
                  new MessageFormat("The provided string is not valid JSON: {1}"),
                  "$",
                  "$",
                  json)));
    }
  }

  public static class ParsingResult {
    private final JsonNode node;
    private final Collection<ValidationMessage> errors;

    private ParsingResult(JsonNode node, Collection<ValidationMessage> errors) {
      this.node = node;
      this.errors = errors;
    }

    public static ParsingResult of(JsonNode node, Collection<ValidationMessage> messages) {
      if (!messages.isEmpty()) {
        return failure(messages);
      }
      return success(node);
    }

    public static ParsingResult success(JsonNode node) {
      return new ParsingResult(node, List.of());
    }

    public static ParsingResult failure(Collection<ValidationMessage> errors) {
      return new ParsingResult(null, errors);
    }

    public ParsingResult flatMap(Function<JsonNode, ParsingResult> fn) {
      if (!errors.isEmpty()) {
        return this;
      }
      return fn.apply(node);
    }

    public boolean isSuccessful() {
      return errors.isEmpty();
    }

    public List<String> formatErrors(String prefix) {
      return errors.stream()
          .map(msg -> formatValidationError(prefix, msg))
          .collect(Collectors.toList());
    }

    @VisibleForTesting
    Collection<ValidationMessage> getErrors() {
      return errors;
    }

    @VisibleForTesting
    JsonNode getJsonNode() {
      return node;
    }

    private static String formatValidationError(String prefix, ValidationMessage error) {
      return String.format("%s: %s", prefix, error.getMessage());
    }
  }
}
