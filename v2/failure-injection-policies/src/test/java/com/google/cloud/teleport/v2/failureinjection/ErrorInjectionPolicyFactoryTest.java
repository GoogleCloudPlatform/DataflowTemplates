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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

public class ErrorInjectionPolicyFactoryTest {

  private String createJson(String policyType, JsonNode policyInput) {
    ObjectNode root = JsonNodeFactory.instance.objectNode();
    root.put("policyType", policyType);
    if (policyInput != null) {
      root.set("policyInput", policyInput);
    }
    return root.toString();
  }

  private String createJsonWithTextInput(String policyType, String policyInput) {
    return createJson(policyType, JsonNodeFactory.instance.textNode(policyInput));
  }

  @Test
  public void shouldReturnAlwaysFailPolicyForValidConfig() {
    String json = createJsonWithTextInput("AlwaysFailPolicy", "test message");
    ErrorInjectionPolicy policy = ErrorInjectionPolicyFactory.getErrorInjectionPolicy(json);

    assertNotNull("Policy should not be null", policy);
    assertTrue(
        "Policy should be an instance of AlwaysFailPolicy", policy instanceof AlwaysFailPolicy);
  }

  @Test
  public void shouldReturnAlwaysFailPolicyForValidConfigWithObjectInput() {
    ObjectNode input = JsonNodeFactory.instance.objectNode();
    input.put("message", "test message");
    input.put("code", 500);
    String json = createJson("AlwaysFailPolicy", input);

    ErrorInjectionPolicy policy = ErrorInjectionPolicyFactory.getErrorInjectionPolicy(json);

    assertNotNull("Policy should not be null", policy);
    assertTrue(
        "Policy should be an instance of AlwaysFailPolicy", policy instanceof AlwaysFailPolicy);
  }

  @Test
  public void shouldReturnInitialLimitedDurationPolicyForValidConfig() {
    String json = createJsonWithTextInput("InitialLimitedDurationErrorInjectionPolicy", "PT10M");
    ErrorInjectionPolicy policy = ErrorInjectionPolicyFactory.getErrorInjectionPolicy(json);

    assertNotNull("Policy should not be null", policy);
    assertTrue(
        "Policy should be an instance of InitialLimitedDurationErrorInjectionPolicy",
        policy instanceof InitialLimitedDurationErrorInjectionPolicy);
  }

  @Test
  public void shouldReturnInitialLimitedDurationPolicyForValidConfigWithObjectInput() {
    ObjectNode input = JsonNodeFactory.instance.objectNode();
    input.put("duration", "PT5S");
    String json = createJson("InitialLimitedDurationErrorInjectionPolicy", input);
    ErrorInjectionPolicy policy = ErrorInjectionPolicyFactory.getErrorInjectionPolicy(json);

    assertNotNull("Policy should not be null", policy);
    assertTrue(
        "Policy should be an instance of InitialLimitedDurationErrorInjectionPolicy",
        policy instanceof InitialLimitedDurationErrorInjectionPolicy);
  }

  @Test
  public void shouldReturnNoOpPolicyForNoOpType() {
    String json = createJsonWithTextInput("NoOp", "");
    ErrorInjectionPolicy policy = ErrorInjectionPolicyFactory.getErrorInjectionPolicy(json);

    assertNotNull("Policy should not be null", policy);
    assertTrue("Policy should be an instance of NoOpPolicy", policy instanceof NoOpPolicy);
  }

  @Test
  public void shouldReturnNoOpPolicyForUnknownPolicyType() {
    String json = createJsonWithTextInput("SomeUnknownPolicyType", "data");
    ErrorInjectionPolicy policy = ErrorInjectionPolicyFactory.getErrorInjectionPolicy(json);

    assertNotNull("Policy should not be null", policy);
    assertTrue(
        "Policy should be an instance of NoOpPolicy for unknown type",
        policy instanceof NoOpPolicy);
  }

  @Test
  public void shouldReturnNoOpPolicyForNullInputString() {
    ErrorInjectionPolicy policy = ErrorInjectionPolicyFactory.getErrorInjectionPolicy(null);
    assertNotNull("Policy should not be null", policy);
    assertTrue(
        "Policy should be an instance of NoOpPolicy for null input", policy instanceof NoOpPolicy);
  }

  @Test
  public void shouldReturnNoOpPolicyForEmptyInputString() {
    ErrorInjectionPolicy policy = ErrorInjectionPolicyFactory.getErrorInjectionPolicy("");
    assertNotNull("Policy should not be null", policy);
    assertTrue(
        "Policy should be an instance of NoOpPolicy for empty input", policy instanceof NoOpPolicy);
  }

  @Test
  public void shouldReturnNoOpPolicyForBlankInputString() {
    ErrorInjectionPolicy policy = ErrorInjectionPolicyFactory.getErrorInjectionPolicy("   ");
    assertNotNull("Policy should not be null", policy);
    assertTrue(
        "Policy should be an instance of NoOpPolicy for blank input", policy instanceof NoOpPolicy);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIllegalArgumentExceptionForInvalidJsonFormat() {
    String invalidJson = "{ \"policyType\": \"AlwaysFailPolicy\", ";
    ErrorInjectionPolicyFactory.getErrorInjectionPolicy(invalidJson);
  }

  @Test
  public void shouldThrowIllegalArgumentExceptionWithCorrectCauseForInvalidJsonFormat() {
    String invalidJson = "{ \"policyType\": \"AlwaysFailPolicy\", ";
    try {
      ErrorInjectionPolicyFactory.getErrorInjectionPolicy(invalidJson);
      fail("Expected IllegalArgumentException was not thrown");
    } catch (IllegalArgumentException e) {
      assertTrue(
          "Exception message should indicate invalid JSON",
          e.getMessage().contains("Invalid JSON format"));
      assertNotNull("Cause should not be null", e.getCause());
      assertTrue(
          "Cause should be JsonProcessingException",
          e.getCause() instanceof JsonProcessingException);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIllegalArgumentExceptionIfPolicyTypeFieldIsMissing() {
    ObjectNode root = JsonNodeFactory.instance.objectNode();
    root.put("policyInput", "someValue");
    String json = root.toString();
    ErrorInjectionPolicyFactory.getErrorInjectionPolicy(json);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIllegalArgumentExceptionIfPolicyTypeFieldIsNull() {
    ObjectNode root = JsonNodeFactory.instance.objectNode();
    root.set("policyType", JsonNodeFactory.instance.nullNode());
    root.put("policyInput", "someValue");
    String json = root.toString();
    ErrorInjectionPolicyFactory.getErrorInjectionPolicy(json);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIllegalArgumentExceptionIfPolicyTypeValueIsEmpty() {
    String json = createJsonWithTextInput("", "someValue");
    ErrorInjectionPolicyFactory.getErrorInjectionPolicy(json);
  }

  @Test
  public void shouldWrapAndThrowIllegalArgumentExceptionFromPolicyConstructorWithObjectInput() {
    ObjectNode input = JsonNodeFactory.instance.objectNode();
    input.put("duration", "INVALID_DURATION");
    String json = createJson("InitialLimitedDurationErrorInjectionPolicy", input);

    try {
      ErrorInjectionPolicyFactory.getErrorInjectionPolicy(json);
      fail("Expected IllegalArgumentException was not thrown");
    } catch (IllegalArgumentException e) {
      assertTrue(
          "Exception message should indicate creation failure",
          e.getMessage().contains("Failed to create error injection policy"));
      assertNotNull("Cause should not be null", e.getCause());
      assertTrue(
          "Cause should be IllegalArgumentException from constructor",
          e.getCause() instanceof IllegalArgumentException);
    }
  }
}
