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

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.teleport.v2.neo4j.model.Json.ParsingResult;
import com.google.cloud.teleport.v2.neo4j.model.enums.EdgeNodesSaveMode;
import com.google.cloud.teleport.v2.neo4j.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.model.enums.PropertyType;
import com.google.cloud.teleport.v2.neo4j.model.enums.RoleType;
import com.google.cloud.teleport.v2.neo4j.model.enums.SaveMode;
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.job.JobSpec;
import com.google.cloud.teleport.v2.neo4j.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.model.job.Source;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import com.networknt.schema.ValidationMessage;
import com.google.cloud.teleport.v2.neo4j.options.Neo4jFlexTemplateOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

public class InputValidatorTest {
  private Target nodeTarget;
  private Target edgeTarget;
  private JobSpec jobSpec;

  @Before
  public void prepare() {
    Source source = new Source();
    source.setName("placeholder_source");
    nodeTarget = new Target();
    nodeTarget.setName("placeholder_node_target");
    nodeTarget.setType(TargetType.node);
    nodeTarget.setSource(source.getName());
    nodeTarget.setMappings(nodeMappings());
    edgeTarget = new Target();
    edgeTarget.setName("placeholder_edge_target");
    edgeTarget.setType(TargetType.edge);
    edgeTarget.setSource(source.getName());
    edgeTarget.setMappings(edgeMappings());
    jobSpec = new JobSpec();
    jobSpec.getSources().put("placeholder_source", source);
  }

  @Test
  public void validatesOptionsWithGcsConnectionMetadata() {
    Neo4jFlexTemplateOptions options = mock(Neo4jFlexTemplateOptions.class);
    when(options.getJobSpecUri()).thenReturn("gs://example.com/spec.json");
    when(options.getNeo4jConnectionUri()).thenReturn("gs://example.com/neo4j.json");

    assertThat(InputValidator.validateNeo4jPipelineOptions(options)).isEmpty();
  }

  @Test
  public void validatesOptionsWithConnectionMetadataSecret() {
    Neo4jFlexTemplateOptions options = mock(Neo4jFlexTemplateOptions.class);
    when(options.getJobSpecUri()).thenReturn("gs://example.com/spec.json");
    when(options.getNeo4jConnectionSecretId())
        .thenReturn("projects/my-project/secrets/a-secret/versions/1");

    assertThat(InputValidator.validateNeo4jPipelineOptions(options)).isEmpty();
  }

  @Test
  public void rejectsCustomQueryTargetWithoutQuery() {
    String sourceName = "some source";
    Target target = new Target();
    target.setName("custom query target");
    target.setType(TargetType.custom_query);
    target.setSource(sourceName);
    JobSpec jobSpec = new JobSpec();
    jobSpec.getSources().put(sourceName, source(sourceName));
    jobSpec.getTargets().add(target);

    List<String> errorMessages = InputValidator.validateJobSpec(jobSpec);

    assertThat(errorMessages).contains("Custom target must define a query");
  }

  @Test
  public void rejectsCustomQueryTargetWithMappings() {
    String sourceName = "some source";
    Target target = new Target();
    target.setName("custom query target");
    target.setType(TargetType.custom_query);
    target.setCustomQuery("RETURN 42");
    target.setMappings(List.of(new Mapping()));
    target.setSource(sourceName);
    JobSpec jobSpec = new JobSpec();
    jobSpec.getSources().put(sourceName, source(sourceName));
    jobSpec.getTargets().add(target);

    List<String> errorMessages = InputValidator.validateJobSpec(jobSpec);

    assertThat(errorMessages).contains("Custom target must not define any mapping");
  }

  @Test
  public void rejectsCustomQueryTargetWithNonDefaultTransform() {
    String sourceName = "some source";
    Target target = new Target();
    target.setName("custom query target");
    target.setType(TargetType.custom_query);
    target.setCustomQuery("RETURN 42");
    target.getTransform().setGroup(true);
    target.setSource(sourceName);
    JobSpec jobSpec = new JobSpec();
    jobSpec.getSources().put(sourceName, source(sourceName));
    jobSpec.getTargets().add(target);

    List<String> errorMessages = InputValidator.validateJobSpec(jobSpec);

    assertThat(errorMessages).contains("Custom target must not define any transform");
  }

  @Test
  public void invalidatesSpecWhenSameNodePropertyIsMappedToDifferentFields() {
    Mapping mapping1 = new Mapping();
    mapping1.setFragmentType(FragmentType.node);
    mapping1.setRole(RoleType.key);
    mapping1.setName("duplicateTargetProperty");
    mapping1.setField("source_field_1");
    nodeTarget.getMappings().add(mapping1);
    Mapping mapping2 = new Mapping();
    mapping2.setFragmentType(FragmentType.node);
    mapping2.setRole(RoleType.property);
    mapping2.setName("duplicateTargetProperty");
    mapping2.setField("source_field_2");
    nodeTarget.getMappings().add(mapping2);
    jobSpec.setTargets(List.of(nodeTarget));

    List<String> errorMessages = InputValidator.validateJobSpec(jobSpec);

    assertThat(errorMessages)
        .isEqualTo(
            List.of(
                "Property duplicateTargetProperty of target placeholder_node_target is mapped to too many source fields: source_field_1, source_field_2"));
  }

  @Test
  public void invalidatesSpecWithConflictingMappingsOfActiveNodeTargets() {
    Mapping mapping1 = new Mapping();
    mapping1.setFragmentType(FragmentType.node);
    mapping1.setRole(RoleType.key);
    mapping1.setName("duplicateTargetProperty");
    mapping1.setField("source_field_1");
    nodeTarget.getMappings().add(mapping1);
    Mapping mapping2 = new Mapping();
    mapping2.setFragmentType(FragmentType.node);
    mapping2.setRole(RoleType.property);
    mapping2.setName("duplicateTargetProperty");
    mapping2.setField("source_field_2");
    nodeTarget.getMappings().add(mapping2);
    jobSpec.setTargets(List.of(nodeTarget));

    List<String> errorMessages = InputValidator.validateJobSpec(jobSpec);

    assertThat(errorMessages)
        .isEqualTo(
            List.of(
                "Property duplicateTargetProperty of target placeholder_node_target is mapped to too many source fields: source_field_1, source_field_2"));
  }

  @Test
  public void invalidatesSpecWhenSameRelPropertyIsMappedToDifferentFields() {
    Mapping mapping1 = new Mapping();
    mapping1.setFragmentType(FragmentType.rel);
    mapping1.setRole(RoleType.key);
    mapping1.setName("targetProperty");
    mapping1.setField("source_field_1");
    edgeTarget.getMappings().add(mapping1);
    Mapping mapping2 = new Mapping();
    mapping2.setFragmentType(FragmentType.rel);
    mapping2.setRole(RoleType.property);
    mapping2.setName("targetProperty");
    mapping2.setField("source_field_2");
    edgeTarget.getMappings().add(mapping2);
    jobSpec.setTargets(List.of(edgeTarget));

    List<String> errorMessages = InputValidator.validateJobSpec(jobSpec);

    assertThat(errorMessages)
        .isEqualTo(
            List.of(
                "Property targetProperty of target placeholder_edge_target is mapped to too many source fields: source_field_1, source_field_2"));
  }

  @Test
  public void invalidatesSpecWhenSameNodePropertyMappedToDifferentTypes() {
    Mapping mapping1 = new Mapping();
    mapping1.setFragmentType(FragmentType.node);
    mapping1.setRole(RoleType.property);
    mapping1.setName("targetProperty2");
    mapping1.setField("source_field");
    mapping1.setType(PropertyType.Boolean);
    nodeTarget.getMappings().add(mapping1);
    Mapping mapping2 = new Mapping();
    mapping2.setFragmentType(FragmentType.node);
    mapping2.setRole(RoleType.property);
    mapping2.setName("targetProperty2");
    mapping2.setField("source_field");
    mapping2.setType(PropertyType.Float);
    nodeTarget.getMappings().add(mapping2);
    jobSpec.setTargets(List.of(nodeTarget));

    List<String> errorMessages = InputValidator.validateJobSpec(jobSpec);

    assertThat(errorMessages)
        .isEqualTo(
            List.of(
                "Property targetProperty2 of target placeholder_node_target is mapped to too many types: Boolean, Float"));
  }

  @Test
  public void invalidatesSpecThatMergesRelationshipButCreatesItsNodes() {
    edgeTarget.setSaveMode(SaveMode.merge);
    edgeTarget.setEdgeNodesMatchMode(EdgeNodesSaveMode.create);
    jobSpec.setTargets(List.of(edgeTarget));

    List<String> errorMessages = InputValidator.validateJobSpec(jobSpec);

    assertThat(errorMessages)
        .isEqualTo(
            List.of(
                "Edge target placeholder_edge_target uses incompatible save modes: either change the target's save mode to create or the edge node mode to match or merge"));
  }

  @Test
  public void acceptsBasicAuthConnectionValidJson() {
    String json =
        "{\"server_url\": \"bolt://example.com\", \"username\": \"user\",\"pwd\": \"password\"}";

    ParsingResult result = InputValidator.validateNeo4jConnection(json);

    assertThat(result.isSuccessful()).isTrue();
    JsonNode node = result.getJsonNode();
    assertThat(node.get("server_url").textValue()).isEqualTo("bolt://example.com");
    assertThat(node.get("username").textValue()).isEqualTo("user");
    assertThat(node.get("pwd").textValue()).isEqualTo("password");
  }

  @Test
  public void acceptsNoAuthConnectionValidJson() {
    String json = "{\"auth_type\": \"none\", \"server_url\": \"bolt://example.com\"}";

    ParsingResult result = InputValidator.validateNeo4jConnection(json);

    assertThat(result.isSuccessful()).isTrue();
    JsonNode node = result.getJsonNode();
    assertThat(node.get("server_url").textValue()).isEqualTo("bolt://example.com");
  }

  @Test
  public void acceptsKerberosAuthConnectionValidJson() {
    String json =
        "{\"auth_type\": \"kerberos\", \"server_url\": \"bolt://example.com\", \"ticket\": \"dGhpcyBpcyBhIHRpY2tldA==\"}";

    ParsingResult result = InputValidator.validateNeo4jConnection(json);

    assertThat(result.isSuccessful()).isTrue();
    JsonNode node = result.getJsonNode();
    assertThat(node.get("server_url").textValue()).isEqualTo("bolt://example.com");
    assertThat(node.get("ticket").textValue()).isEqualTo("dGhpcyBpcyBhIHRpY2tldA==");
  }

  @Test
  public void acceptsBearerAuthConnectionValidJson() {
    String json =
        "{\"auth_type\": \"bearer\", \"server_url\": \"bolt://example.com\", \"token\": \"a-token\"}";

    ParsingResult result = InputValidator.validateNeo4jConnection(json);

    assertThat(result.isSuccessful()).isTrue();
    JsonNode node = result.getJsonNode();
    assertThat(node.get("server_url").textValue()).isEqualTo("bolt://example.com");
    assertThat(node.get("token").textValue()).isEqualTo("a-token");
  }

  @Test
  public void acceptsCustomAuthConnectionValidJson() {
    String json =
        "{\"auth_type\": \"custom\", \"server_url\": \"bolt://example.com\", \"principal\": \"a-principal\", \"credentials\": \"some-credentials\", \"scheme\": \"a-scheme\"}";

    ParsingResult result = InputValidator.validateNeo4jConnection(json);

    assertThat(result.isSuccessful()).isTrue();
    JsonNode node = result.getJsonNode();
    assertThat(node.get("server_url").textValue()).isEqualTo("bolt://example.com");
    assertThat(node.get("principal").textValue()).isEqualTo("a-principal");
    assertThat(node.get("credentials").textValue()).isEqualTo("some-credentials");
    assertThat(node.get("scheme").textValue()).isEqualTo("a-scheme");
  }

  @Test
  public void rejectsConnectionInvalidJson() {
    String json = "invalidjson";

    ParsingResult result = InputValidator.validateNeo4jConnection(json);

    assertThat(result.isSuccessful()).isFalse();
    List<String> errors =
        result.getErrors().stream().map(ValidationMessage::toString).collect(Collectors.toList());
    assertThat(errors).hasSize(1);
    assertThat(errors.get(0)).startsWith("The provided string is not valid JSON: invalidjson");
  }

  @Test
  public void rejectsConnectionNonObjectJson() {
    String json = "[]";

    ParsingResult result = InputValidator.validateNeo4jConnection(json);

    assertThat(result.isSuccessful()).isFalse();
    List<String> errors =
        result.getErrors().stream().map(ValidationMessage::toString).collect(Collectors.toList());
    assertThat(errors).containsExactly("$: array found, object expected");
  }

  @Test
  public void rejectsBasicAuthConnectionJsonWithoutServerUrl() {
    String json = "{\"username\": \"user\", \"pwd\":  \"password\"}";

    ParsingResult result = InputValidator.validateNeo4jConnection(json);

    assertThat(result.isSuccessful()).isFalse();
    List<String> errors =
        result.getErrors().stream().map(ValidationMessage::toString).collect(Collectors.toList());
    assertThat(errors).containsExactly("$.server_url: is missing but it is required");
  }

  @Test
  public void rejectsConnectionJsonWithInvalidServerUrl() {
    String json = "{\"server_url\": \"not-a-uri\", \"username\": \"user\", \"pwd\":  \"password\"}";

    ParsingResult result = InputValidator.validateNeo4jConnection(json);

    assertThat(result.isSuccessful()).isFalse();
    List<String> errors =
        result.getErrors().stream().map(ValidationMessage::toString).collect(Collectors.toList());
    assertThat(errors)
        .contains("$.server_url: does not match the uri pattern must be a valid RFC 3986 URI");
  }

  @Test
  public void rejectsConnectionJsonWithUnsupportedServerUrl() {
    String json =
        "{\"server_url\": \"http://unsupported.example.com\", \"username\": \"user\", \"pwd\":  \"password\"}";

    ParsingResult result = InputValidator.validateNeo4jConnection(json);

    assertThat(result.isSuccessful()).isFalse();
    List<String> errors =
        result.getErrors().stream().map(ValidationMessage::toString).collect(Collectors.toList());
    assertThat(errors)
        .containsExactly(
            "$.server_url: does not match the regex pattern ^(neo4j|bolt)(\\+s(sc)?)?://");
  }

  @Test
  public void rejectsConnectionJsonWithEmptyDatabase() {
    String json =
        "{\"server_url\": \"bolt://example.com\", \"username\": \"user\",\"pwd\":  \"password\", \"database\":  \"\"}";

    ParsingResult result = InputValidator.validateNeo4jConnection(json);

    assertThat(result.isSuccessful()).isFalse();
    List<String> errors =
        result.getErrors().stream().map(ValidationMessage::toString).collect(Collectors.toList());
    assertThat(errors).containsExactly("$.database: must be at least 1 characters long");
  }

  @Test
  public void rejectsConnectionJsonWithInvalidAuthType() {
    String json =
        "{\"server_url\": \"bolt://example.com\", \"username\": \"user\",\"pwd\":  \"password\", \"auth_type\":  \"invalid-auth-type\"}";

    ParsingResult result = InputValidator.validateNeo4jConnection(json);

    assertThat(result.isSuccessful()).isFalse();
    List<String> errors =
        result.getErrors().stream().map(ValidationMessage::toString).collect(Collectors.toList());
    assertThat(errors)
        .containsExactly(
            "$.auth_type: does not have a value in the enumeration [basic, none, kerberos, bearer, custom]");
  }

  @Test
  public void rejectsBasicAuthConnectionJsonWithoutUsername() {
    String json = "{\"server_url\": \"bolt://example.com\", \"pwd\":  \"password\"}";

    ParsingResult result = InputValidator.validateNeo4jConnection(json);

    assertThat(result.isSuccessful()).isFalse();
    List<String> errors =
        result.getErrors().stream().map(ValidationMessage::toString).collect(Collectors.toList());
    assertThat(errors).containsExactly("$.username: is missing but it is required");
  }

  @Test
  public void rejectsBasicAuthConnectionJsonWithEmptyUsername() {
    String json =
        "{\"server_url\": \"bolt://example.com\", \"username\": \"\",\"pwd\":  \"password\"}";

    ParsingResult result = InputValidator.validateNeo4jConnection(json);

    assertThat(result.isSuccessful()).isFalse();
    List<String> errors =
        result.getErrors().stream().map(ValidationMessage::toString).collect(Collectors.toList());
    assertThat(errors).containsExactly("$.username: must be at least 1 characters long");
  }

  @Test
  public void rejectsBasicAuthConnectionJsonWithoutPassword() {
    String json = "{\"server_url\": \"bolt://example.com\", \"username\": \"user\"}";

    ParsingResult result = InputValidator.validateNeo4jConnection(json);

    assertThat(result.isSuccessful()).isFalse();
    List<String> errors =
        result.getErrors().stream().map(ValidationMessage::toString).collect(Collectors.toList());
    assertThat(errors).containsExactly("$.pwd: is missing but it is required");
  }

  @Test
  public void rejectsBasicAuthConnectionJsonWithEmptyPassword() {
    String json = "{\"server_url\": \"bolt://example.com\", \"username\": \"user\",\"pwd\":  \"\"}";

    ParsingResult result = InputValidator.validateNeo4jConnection(json);

    assertThat(result.isSuccessful()).isFalse();
    List<String> errors =
        result.getErrors().stream().map(ValidationMessage::toString).collect(Collectors.toList());
    assertThat(errors).containsExactly("$.pwd: must be at least 1 characters long");
  }

  @Test
  public void rejectsKerberosConnectionJsonWithoutTicket() {
    String json = "{\"auth_type\": \"kerberos\",\"server_url\": \"bolt://example.com\"}";

    ParsingResult result = InputValidator.validateNeo4jConnection(json);

    assertThat(result.isSuccessful()).isFalse();
    List<String> errors =
        result.getErrors().stream().map(ValidationMessage::toString).collect(Collectors.toList());
    assertThat(errors).containsExactly("$.ticket: is missing but it is required");
  }

  @Test
  public void rejectsKerberosConnectionJsonWithEmptyTicket() {
    String json =
        "{\"auth_type\": \"kerberos\",\"server_url\": \"bolt://example.com\", \"ticket\": \"\"}";

    ParsingResult result = InputValidator.validateNeo4jConnection(json);

    assertThat(result.isSuccessful()).isFalse();
    List<String> errors =
        result.getErrors().stream().map(ValidationMessage::toString).collect(Collectors.toList());
    assertThat(errors).containsExactly("$.ticket: must be at least 1 characters long");
  }

  @Test
  public void rejectsBearerConnectionJsonWithEmptyToken() {
    String json =
        "{\"auth_type\": \"bearer\",\"server_url\": \"bolt://example.com\", \"token\": \"\"}";

    ParsingResult result = InputValidator.validateNeo4jConnection(json);

    assertThat(result.isSuccessful()).isFalse();
    List<String> errors =
        result.getErrors().stream().map(ValidationMessage::toString).collect(Collectors.toList());
    assertThat(errors).containsExactly("$.token: must be at least 1 characters long");
  }

  @Test
  public void rejectsCustomConnectionJsonWithNoPrincipal() {
    String json =
        "{\"auth_type\": \"custom\",\"server_url\": \"bolt://example.com\", \"credentials\": \"some-credentials\", \"scheme\": \"a-scheme\"}";

    ParsingResult result = InputValidator.validateNeo4jConnection(json);

    assertThat(result.isSuccessful()).isFalse();
    List<String> errors =
        result.getErrors().stream().map(ValidationMessage::toString).collect(Collectors.toList());
    assertThat(errors).containsExactly("$.principal: is missing but it is required");
  }

  @Test
  public void rejectsCustomConnectionJsonWithEmptyPrincipal() {
    String json =
        "{\"auth_type\": \"custom\",\"server_url\": \"bolt://example.com\", \"principal\": \"\", \"credentials\": \"some-credentials\", \"scheme\": \"a-scheme\"}";

    ParsingResult result = InputValidator.validateNeo4jConnection(json);

    assertThat(result.isSuccessful()).isFalse();
    List<String> errors =
        result.getErrors().stream().map(ValidationMessage::toString).collect(Collectors.toList());
    assertThat(errors).containsExactly("$.principal: must be at least 1 characters long");
  }

  @Test
  public void rejectsCustomConnectionJsonWithNoCredentials() {
    String json =
        "{\"auth_type\": \"custom\",\"server_url\": \"bolt://example.com\", \"principal\": \"a-principal\", \"scheme\": \"a-scheme\"}";

    ParsingResult result = InputValidator.validateNeo4jConnection(json);

    assertThat(result.isSuccessful()).isFalse();
    List<String> errors =
        result.getErrors().stream().map(ValidationMessage::toString).collect(Collectors.toList());
    assertThat(errors).containsExactly("$.credentials: is missing but it is required");
  }

  @Test
  public void rejectsCustomConnectionJsonWithEmptyCredentials() {
    String json =
        "{\"auth_type\": \"custom\",\"server_url\": \"bolt://example.com\", \"principal\": \"a-principal\", \"credentials\": \"\", \"scheme\": \"a-scheme\"}";

    ParsingResult result = InputValidator.validateNeo4jConnection(json);

    assertThat(result.isSuccessful()).isFalse();
    List<String> errors =
        result.getErrors().stream().map(ValidationMessage::toString).collect(Collectors.toList());
    assertThat(errors).containsExactly("$.credentials: must be at least 1 characters long");
  }

  @Test
  public void rejectsCustomConnectionJsonWithNoScheme() {
    String json =
        "{\"auth_type\": \"custom\",\"server_url\": \"bolt://example.com\", \"principal\": \"a-principal\", \"credentials\": \"some-credentials\"}";

    ParsingResult result = InputValidator.validateNeo4jConnection(json);

    assertThat(result.isSuccessful()).isFalse();
    List<String> errors =
        result.getErrors().stream().map(ValidationMessage::toString).collect(Collectors.toList());
    assertThat(errors).containsExactly("$.scheme: is missing but it is required");
  }

  @Test
  public void rejectsCustomConnectionJsonWithEmptyScheme() {
    String json =
        "{\"auth_type\": \"custom\",\"server_url\": \"bolt://example.com\", \"principal\": \"a-principal\", \"credentials\": \"some-credentials\", \"scheme\": \"\"}";

    ParsingResult result = InputValidator.validateNeo4jConnection(json);

    assertThat(result.isSuccessful()).isFalse();
    List<String> errors =
        result.getErrors().stream().map(ValidationMessage::toString).collect(Collectors.toList());
    assertThat(errors).containsExactly("$.scheme: must be at least 1 characters long");
  }

  @Test
  public void rejectsCustomConnectionJsonWithEmptyRealm() {
    String json =
        "{\"auth_type\": \"custom\",\"server_url\": \"bolt://example.com\", \"principal\": \"a-principal\", \"credentials\": \"some-credentials\", \"scheme\": \"a-scheme\", \"realm\": \"\"}";

    ParsingResult result = InputValidator.validateNeo4jConnection(json);

    assertThat(result.isSuccessful()).isFalse();
    List<String> errors =
        result.getErrors().stream().map(ValidationMessage::toString).collect(Collectors.toList());
    assertThat(errors).containsExactly("$.realm: must be at least 1 characters long");
  }

    @Test
    public void invalidatesOptionsIfBothSecretAndGcsUriAreMissing() {
        Neo4jFlexTemplateOptions options = mock(Neo4jFlexTemplateOptions.class);
        when(options.getJobSpecUri()).thenReturn("gs://example.com/spec.json");

        List<String> errors = InputValidator.validateNeo4jPipelineOptions(options);

        assertThat(errors)
                .isEqualTo(
                        List.of("Neither Neo4j connection URI nor Neo4j connection secret were provided."));
    }

    @Test
    public void invalidatesOptionsIfBothSecretAndGcsUriAreSpecified() {
        Neo4jFlexTemplateOptions options = mock(Neo4jFlexTemplateOptions.class);
        when(options.getJobSpecUri()).thenReturn("gs://example.com/spec.json");
        when(options.getNeo4jConnectionUri()).thenReturn("gs://example.com/neo4j.json");
        when(options.getNeo4jConnectionSecretId())
                .thenReturn("projects/my-project/secrets/a-secret/versions/1");

        List<String> errors = InputValidator.validateNeo4jPipelineOptions(options);

        assertThat(errors)
                .isEqualTo(
                        List.of(
                                "Both Neo4j connection URI and Neo4j connection secret were provided: only one must be set."));
    }

    @Test
    public void invalidatesOptionsIfSecretFormatIsIncorrect() {
        Neo4jFlexTemplateOptions options = mock(Neo4jFlexTemplateOptions.class);
        when(options.getJobSpecUri()).thenReturn("gs://example.com/spec.json");
        when(options.getNeo4jConnectionSecretId()).thenReturn("wrongly-formatted-secret");

        List<String> errors = InputValidator.validateNeo4jPipelineOptions(options);

        assertThat(errors)
                .isEqualTo(
                        List.of(
                                "Neo4j connection secret must be in the form projects/{project}/secrets/{secret}/versions/{secret_version}"));
    }

  private static List<Mapping> nodeMappings() {
    Mapping key = new Mapping();
    key.setFragmentType(FragmentType.node);
    key.setRole(RoleType.key);
    key.setField("source_column");
    key.setName("targetProperty");
    Mapping label = new Mapping();
    label.setFragmentType(FragmentType.node);
    label.setRole(RoleType.label);
    label.setConstant("\"PlaceholderLabel\"");
    return new ArrayList<>(List.of(key, label));
  }

  private static List<Mapping> edgeMappings() {
    Mapping type = new Mapping();
    type.setFragmentType(FragmentType.rel);
    type.setRole(RoleType.type);
    type.setConstant("\"PLACEHOLDER_TYPE\"");
    Mapping source = new Mapping();
    source.setName("sourcePlaceholderProperty");
    source.setFragmentType(FragmentType.source);
    source.setRole(RoleType.key);
    source.setField("placeholder_source_field");
    Mapping target = new Mapping();
    source.setName("targetPlaceholderProperty");
    target.setFragmentType(FragmentType.target);
    target.setRole(RoleType.key);
    target.setField("placeholder_target_field");
    return new ArrayList<>(List.of(type, source, target));
  }

  private static Source source(String name) {
    Source source = new Source();
    source.setName(name);
    return source;
  }
}
