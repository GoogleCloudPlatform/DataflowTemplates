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
package com.google.cloud.teleport.plugin.terraform;

import static com.google.cloud.teleport.plugin.terraform.TerraformVariable.Type.BOOL;
import static com.google.cloud.teleport.plugin.terraform.TerraformVariable.Type.NUMBER;
import static com.google.cloud.teleport.plugin.terraform.TerraformVariable.Type.STRING;
import static com.google.common.truth.Truth.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.teleport.plugin.model.ImageSpecParameter;
import com.google.cloud.teleport.plugin.model.ImageSpecParameterType;
import org.junit.Test;

public class TemplateTerraformGeneratorTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String PARAMETER_NAME = "a_parameter";
  private static final String PARAMETER_HELP = "some description";

  @Test
  public void givenTextType_thenStringVariable() {
    JsonNode variable = TemplateTerraformGenerator.variable(imageSpecParameter());
    assertThat(variable).isEqualTo(variableNode());
  }

  @Test
  public void givenNumberType_thenNumberVariable() {
    JsonNode variable =
        TemplateTerraformGenerator.variable(imageSpecParameter(ImageSpecParameterType.NUMBER));
    assertThat(variable).isEqualTo(variableNode(NUMBER));
  }

  @Test
  public void givenBooleanType_thenBoolVariable() {
    JsonNode variable =
        TemplateTerraformGenerator.variable(imageSpecParameter(ImageSpecParameterType.BOOLEAN));
    assertThat(variable).isEqualTo(variableNode(BOOL));
  }

  @Test
  public void givenBigQueryTableType_thenStringVariable() {
    JsonNode variable =
        TemplateTerraformGenerator.variable(
            imageSpecParameter(ImageSpecParameterType.BIGQUERY_TABLE));
    assertThat(variable).isEqualTo(variableNode(STRING));
  }

  @Test
  public void givenOptional_thenNullableVariable() {
    JsonNode variable =
        TemplateTerraformGenerator.variable(imageSpecParameter(ImageSpecParameterType.TEXT, true));
    assertThat(variable).isEqualTo(variableNode(STRING, true));
  }

  private static ImageSpecParameter imageSpecParameter() {
    return imageSpecParameter(ImageSpecParameterType.TEXT);
  }

  private static JsonNode variableNode() {
    return variableNode(STRING);
  }

  private static ImageSpecParameter imageSpecParameter(ImageSpecParameterType type) {
    return imageSpecParameter(type, false);
  }

  private static JsonNode variableNode(TerraformVariable.Type type) {
    return variableNode(type, false);
  }

  private static ImageSpecParameter imageSpecParameter(
      ImageSpecParameterType type, boolean optional) {
    ImageSpecParameter parameter = new ImageSpecParameter();
    parameter.setName(PARAMETER_NAME);
    parameter.setHelpText(PARAMETER_HELP);
    parameter.setParamType(type);
    parameter.setOptional(optional);

    return parameter;
  }

  private static JsonNode variableNode(TerraformVariable.Type type, boolean nullable) {
    ObjectNode node = OBJECT_MAPPER.createObjectNode();
    node.put("name", PARAMETER_NAME);
    node.put("description", PARAMETER_HELP);
    node.put("type", type.name());
    node.put("nullable", nullable);
    return node;
  }
}
