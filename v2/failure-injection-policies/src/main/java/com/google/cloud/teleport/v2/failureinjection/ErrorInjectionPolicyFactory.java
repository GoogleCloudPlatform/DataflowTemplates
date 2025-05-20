/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.failureinjection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class for creating instances of {@link ErrorInjectionPolicy}. It parses a JSON
 * configuration string to determine which policy to instantiate and passes the relevant
 * configuration parameters to the policy's constructor.
 */
public class ErrorInjectionPolicyFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ErrorInjectionPolicyFactory.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String POLICY_TYPE_FIELD = "policyType";
  private static final String POLICY_INPUT_FIELD = "policyInput";

  private static final String TYPE_ALWAYS_FAIL = "AlwaysFailPolicy";
  private static final String TYPE_INITIAL_LIMITED_DURATION =
      "InitialLimitedDurationErrorInjectionPolicy";
  private static final String TYPE_NO_OP = "NoOp";

  /**
   * Creates an {@link ErrorInjectionPolicy} based on a JSON configuration string.
   *
   * <p>The JSON string must contain:
   *
   * <ul>
   *   <li>{@code policyType}: A string identifying the policy to create (e.g., "AlwaysFail",
   *       "InitialLimitedDuration", "NoOp").
   *   <li>{@code policyInput}: A string containing the input parameter required by the specified
   *       policy's constructor (this can be an empty string or omitted if the policy doesn't
   *       require input, like NoOpPolicy).
   * </ul>
   *
   * Example JSON:
   *
   * <pre>{@code
   * {
   * "policyType": "InitialLimitedDuration",
   * "policyInput": "PT5M"
   * }
   * }</pre>
   *
   * @param jsonParameter A JSON string containing the policy configuration.
   * @return An instance of the configured {@link ErrorInjectionPolicy}.
   * @throws IllegalArgumentException if jsonParameter is invalid JSON, specifies an unknown
   *     policyType, or if the policyInput is invalid for the chosen policy.
   */
  public static ErrorInjectionPolicy getErrorInjectionPolicy(String jsonParameter) {
    if (jsonParameter == null || jsonParameter.trim().isEmpty()) {
      LOG.error("JSON parameter for ErrorInjectionPolicyFactory cannot be null or empty.");
      return new NoOpPolicy();
    }

    try {
      JsonNode rootNode = OBJECT_MAPPER.readTree(jsonParameter);

      // Check for presence and validity of policyType field
      if (!rootNode.hasNonNull(POLICY_TYPE_FIELD)) {
        throw new IllegalArgumentException(
            "Missing required field in JSON configuration: " + POLICY_TYPE_FIELD);
      }
      String policyType = rootNode.get(POLICY_TYPE_FIELD).asText();
      if (policyType.trim().isEmpty()) {
        throw new IllegalArgumentException(
            "Field '" + POLICY_TYPE_FIELD + "' cannot be empty in JSON configuration.");
      }

      // Extract policyInput as a JsonNode. Use path() which returns MissingNode if absent.
      // Policies are responsible for handling MissingNode, NullNode, or specific types as needed.
      JsonNode policyInputNode = rootNode.path(POLICY_INPUT_FIELD);

      LOG.info(
          "Attempting to create ErrorInjectionPolicy of type '{}' with input node type: {}",
          policyType,
          policyInputNode.getNodeType());
      LOG.debug("Policy input node content: {}", policyInputNode);

      switch (policyType) {
        case TYPE_ALWAYS_FAIL:
          return new AlwaysFailPolicy(policyInputNode);

        case TYPE_INITIAL_LIMITED_DURATION:
          return new InitialLimitedDurationErrorInjectionPolicy(policyInputNode);

        case TYPE_NO_OP:
        default:
          return new NoOpPolicy();
      }

    } catch (JsonProcessingException e) {
      LOG.error("Failed to parse JSON configuration parameter: {}", jsonParameter, e);
      throw new IllegalArgumentException("Invalid JSON format for configuration parameter.", e);
    } catch (Exception e) {
      LOG.error("Failed to create ErrorInjectionPolicy for config: {}", jsonParameter, e);
      throw new IllegalArgumentException(
          "Failed to create error injection policy: " + e.getMessage(), e);
    }
  }
}
