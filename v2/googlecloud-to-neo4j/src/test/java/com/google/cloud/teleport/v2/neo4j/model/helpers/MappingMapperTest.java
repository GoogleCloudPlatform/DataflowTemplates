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
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.neo4j.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.model.enums.RoleType;
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.job.FieldNameTuple;
import com.google.cloud.teleport.v2.neo4j.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

public class MappingMapperTest {
  @Test
  public void parsesMultipleKeyMappingsFromObjectForEdgeSourceNode() {
    Target target = target("my-target", TargetType.edge);
    JSONObject mappings = new JSONObject();
    mappings.put("type", "PLACEHOLDER");
    JSONObject sourceKeys = new JSONObject();
    sourceKeys.put("key1", "value1");
    sourceKeys.put("key2", "value2");
    JSONObject sourceMapping = new JSONObject();
    sourceMapping.put("label", "Placeholder");
    sourceMapping.put("key", sourceKeys);
    mappings.put("source", sourceMapping);

    List<Mapping> result =
        MappingMapper.parseMappings(target, mappings).stream()
            .filter(mapping -> mapping.getRole() == RoleType.key)
            .collect(toList());

    assertThat(result)
        .isEqualTo(
            List.of(
                new Mapping(FragmentType.source, RoleType.key, fieldTuple("key1", "value1")),
                new Mapping(FragmentType.source, RoleType.key, fieldTuple("key2", "value2"))));
  }

  @Test
  public void parsesMultipleKeyMappingsFromObjectArrayForEdgeSourceNode() {
    Target target = target("my-target", TargetType.edge);
    JSONObject mappings = new JSONObject();
    mappings.put("type", "PLACEHOLDER");
    JSONArray sourceKeys = new JSONArray();
    sourceKeys.put(Map.of("key1", "value1", "key2", "value2"));
    sourceKeys.put(Map.of("key3", "value3", "key4", "value4"));
    JSONObject sourceMapping = new JSONObject();
    sourceMapping.put("label", "Placeholder");
    sourceMapping.put("key", sourceKeys);
    mappings.put("source", sourceMapping);

    List<Mapping> result =
        MappingMapper.parseMappings(target, mappings).stream()
            .filter(mapping -> mapping.getRole() == RoleType.key)
            .collect(toList());

    assertThat(result)
        .isEqualTo(
            List.of(
                new Mapping(FragmentType.source, RoleType.key, fieldTuple("key1", "value1")),
                new Mapping(FragmentType.source, RoleType.key, fieldTuple("key2", "value2")),
                new Mapping(FragmentType.source, RoleType.key, fieldTuple("key3", "value3")),
                new Mapping(FragmentType.source, RoleType.key, fieldTuple("key4", "value4"))));
  }

  @Test
  public void parsesMultipleKeyMappingsFromStringArrayForEdgeSourceNode() {
    Target target = target("my-target", TargetType.edge);
    JSONObject mappings = new JSONObject();
    mappings.put("type", "PLACEHOLDER");
    JSONArray sourceKeys = new JSONArray();
    sourceKeys.put("value1");
    sourceKeys.put("value2");
    JSONObject sourceMapping = new JSONObject();
    sourceMapping.put("label", "Placeholder");
    sourceMapping.put("key", sourceKeys);
    mappings.put("source", sourceMapping);

    List<Mapping> result =
        MappingMapper.parseMappings(target, mappings).stream()
            .filter(mapping -> mapping.getRole() == RoleType.key)
            .collect(toList());

    assertThat(result)
        .isEqualTo(
            List.of(
                new Mapping(FragmentType.source, RoleType.key, fieldTuple("value1", "value1")),
                new Mapping(FragmentType.source, RoleType.key, fieldTuple("value2", "value2"))));
  }

  @Test
  public void parsesMultipleKeyMappingsFromMixedArrayForEdgeSourceNode() {
    Target target = target("my-target", TargetType.edge);
    JSONObject mappings = new JSONObject();
    mappings.put("type", "PLACEHOLDER");
    JSONArray sourceKeys = new JSONArray();
    sourceKeys.put("value1");
    sourceKeys.put(Map.of("key2", "value2"));
    JSONObject sourceMapping = new JSONObject();
    sourceMapping.put("label", "Placeholder");
    sourceMapping.put("key", sourceKeys);
    mappings.put("source", sourceMapping);

    List<Mapping> result =
        MappingMapper.parseMappings(target, mappings).stream()
            .filter(mapping -> mapping.getRole() == RoleType.key)
            .collect(toList());

    assertThat(result)
        .isEqualTo(
            List.of(
                new Mapping(FragmentType.source, RoleType.key, fieldTuple("value1", "value1")),
                new Mapping(FragmentType.source, RoleType.key, fieldTuple("key2", "value2"))));
  }

  @Test
  public void parsesMultipleKeyMappingsFromObjectForEdgeTargetNode() {
    Target target = target("my-target", TargetType.edge);
    JSONObject mappings = new JSONObject();
    mappings.put("type", "PLACEHOLDER");
    JSONObject targetKeys = new JSONObject();
    targetKeys.put("key1", "value1");
    targetKeys.put("key2", "value2");
    JSONObject targetMapping = new JSONObject();
    targetMapping.put("label", "Placeholder");
    targetMapping.put("key", targetKeys);
    mappings.put("target", targetMapping);

    List<Mapping> result =
        MappingMapper.parseMappings(target, mappings).stream()
            .filter(mapping -> mapping.getRole() == RoleType.key)
            .collect(toList());

    assertThat(result)
        .isEqualTo(
            List.of(
                new Mapping(FragmentType.target, RoleType.key, fieldTuple("key1", "value1")),
                new Mapping(FragmentType.target, RoleType.key, fieldTuple("key2", "value2"))));
  }

  @Test
  public void parsesMultipleKeyMappingsFromObjectArrayForEdgeTargetNode() {
    Target target = target("my-target", TargetType.edge);
    JSONObject mappings = new JSONObject();
    mappings.put("type", "PLACEHOLDER");
    JSONArray targetKeys = new JSONArray();
    targetKeys.put(Map.of("key1", "value1", "key2", "value2"));
    targetKeys.put(Map.of("key3", "value3", "key4", "value4"));
    JSONObject sourceMapping = new JSONObject();
    sourceMapping.put("label", "Placeholder");
    sourceMapping.put("key", targetKeys);
    mappings.put("target", sourceMapping);

    List<Mapping> result =
        MappingMapper.parseMappings(target, mappings).stream()
            .filter(mapping -> mapping.getRole() == RoleType.key)
            .collect(toList());

    assertThat(result)
        .isEqualTo(
            List.of(
                new Mapping(FragmentType.target, RoleType.key, fieldTuple("key1", "value1")),
                new Mapping(FragmentType.target, RoleType.key, fieldTuple("key2", "value2")),
                new Mapping(FragmentType.target, RoleType.key, fieldTuple("key3", "value3")),
                new Mapping(FragmentType.target, RoleType.key, fieldTuple("key4", "value4"))));
  }

  @Test
  public void parsesMultipleKeyMappingsFromStringArrayForEdgeTargetNode() {
    Target target = target("my-target", TargetType.edge);
    JSONObject mappings = new JSONObject();
    mappings.put("type", "PLACEHOLDER");
    JSONArray targetKeys = new JSONArray();
    targetKeys.put("value1");
    targetKeys.put("value2");
    JSONObject sourceMapping = new JSONObject();
    sourceMapping.put("label", "Placeholder");
    sourceMapping.put("key", targetKeys);
    mappings.put("target", sourceMapping);

    List<Mapping> result =
        MappingMapper.parseMappings(target, mappings).stream()
            .filter(mapping -> mapping.getRole() == RoleType.key)
            .collect(toList());

    assertThat(result)
        .isEqualTo(
            List.of(
                new Mapping(FragmentType.target, RoleType.key, fieldTuple("value1", "value1")),
                new Mapping(FragmentType.target, RoleType.key, fieldTuple("value2", "value2"))));
  }

  @Test
  public void parsesMultipleKeyMappingsFromMixedArrayForEdgeTargetNode() {
    Target target = target("my-target", TargetType.edge);
    JSONObject mappings = new JSONObject();
    mappings.put("type", "PLACEHOLDER");
    JSONArray targetKeys = new JSONArray();
    targetKeys.put(Map.of("key1", "value1"));
    targetKeys.put("value2");
    JSONObject sourceMapping = new JSONObject();
    sourceMapping.put("label", "Placeholder");
    sourceMapping.put("key", targetKeys);
    mappings.put("target", sourceMapping);

    List<Mapping> result =
        MappingMapper.parseMappings(target, mappings).stream()
            .filter(mapping -> mapping.getRole() == RoleType.key)
            .collect(toList());

    assertThat(result)
        .isEqualTo(
            List.of(
                new Mapping(FragmentType.target, RoleType.key, fieldTuple("key1", "value1")),
                new Mapping(FragmentType.target, RoleType.key, fieldTuple("value2", "value2"))));
  }

  @Test
  public void rejectsNodeMappingsForAlreadyMappedFields() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                MappingMapper.parseMappings(
                    target("my-target", TargetType.node),
                    jsonKeys(
                        jsonFieldTuple("source_field", "graphProperty1"),
                        jsonFieldTuple("source_field", "graphProperty2") // duplicate
                        )));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Duplicate mapping: field source_field has already been mapped for target my-target");
  }

  @Test
  public void rejectsEdgeMappingsForAlreadyMappedFields() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                MappingMapper.parseMappings(
                    target("my-target", TargetType.edge),
                    jsonKeys(
                        jsonFieldTuple("source_field", "graphProperty1"),
                        jsonFieldTuple("source_field", "graphProperty2") // duplicate
                        )));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            "Duplicate mapping: field source_field has already been mapped for target my-target");
  }

  @Test
  public void allowsMultipleMappingsWithoutFields() {
    List<Mapping> mappings =
        MappingMapper.parseMappings(
            target("my-target", TargetType.node),
            jsonLabels("\"Customer\"", "\"Buyer\"")); // label mappings don't map to source fields

    assertThat(mappings)
        .isEqualTo(
            List.of(
                indexedMapping(FragmentType.node, RoleType.label, constantFieldTuple("Customer")),
                indexedMapping(FragmentType.node, RoleType.label, constantFieldTuple("Buyer"))));
  }

  private static Target target(String name, TargetType type) {
    Target target = new Target();
    target.setName(name);
    target.setType(type);
    return target;
  }

  private static JSONObject jsonKeys(JSONObject... fieldTuples) {
    JSONObject mappings = new JSONObject();
    JSONArray array = new JSONArray();
    for (JSONObject fieldMapping : fieldTuples) {
      array.put(fieldMapping);
    }
    mappings.put("keys", array);
    return mappings;
  }

  private static JSONObject jsonLabels(String... labels) {
    JSONObject mappings = new JSONObject();
    JSONArray array = new JSONArray();
    for (String label : labels) {
      array.put(label);
    }
    mappings.put("labels", array);
    return mappings;
  }

  private static JSONObject jsonFieldTuple(String field, String property) {
    JSONObject nodeKeyMapping1 = new JSONObject();
    nodeKeyMapping1.put(field, property);
    return nodeKeyMapping1;
  }

  private static Mapping indexedMapping(
      FragmentType fragment, RoleType role, FieldNameTuple tuple) {
    Mapping mapping = new Mapping(fragment, role, tuple);
    mapping.setIndexed(true);
    return mapping;
  }

  private FieldNameTuple constantFieldTuple(String constant) {
    FieldNameTuple tuple = new FieldNameTuple();
    tuple.setName(constant);
    tuple.setConstant(constant);
    return tuple;
  }

  @NotNull
  private static FieldNameTuple fieldTuple(String field, String name) {
    FieldNameTuple fieldNameTuple = new FieldNameTuple();
    fieldNameTuple.setField(field);
    fieldNameTuple.setName(name);
    return fieldNameTuple;
  }
}
