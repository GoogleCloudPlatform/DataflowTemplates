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

import com.google.cloud.teleport.v2.neo4j.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.model.enums.PropertyType;
import com.google.cloud.teleport.v2.neo4j.model.enums.RoleType;
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.job.FieldNameTuple;
import com.google.cloud.teleport.v2.neo4j.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import java.util.Comparator;
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
            .filter(mapping -> roleIs(mapping, RoleType.key))
            .sorted(Comparator.comparing(Mapping::getField))
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
            .filter(mapping -> roleIs(mapping, RoleType.key))
            .sorted(Comparator.comparing(Mapping::getField))
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
            .filter(mapping -> roleIs(mapping, RoleType.key))
            .sorted(Comparator.comparing(Mapping::getField))
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
            .filter(mapping -> roleIs(mapping, RoleType.key))
            .sorted(Comparator.comparing(Mapping::getField))
            .collect(toList());

    assertThat(result)
        .isEqualTo(
            List.of(
                new Mapping(FragmentType.source, RoleType.key, fieldTuple("key2", "value2")),
                new Mapping(FragmentType.source, RoleType.key, fieldTuple("value1", "value1"))));
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
            .filter(mapping -> roleIs(mapping, RoleType.key))
            .sorted(Comparator.comparing(Mapping::getField))
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
            .filter(mapping -> roleIs(mapping, RoleType.key))
            .sorted(Comparator.comparing(Mapping::getField))
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
            .filter(mapping -> roleIs(mapping, RoleType.key))
            .sorted(Comparator.comparing(Mapping::getField))
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
            .filter(mapping -> roleIs(mapping, RoleType.key))
            .sorted(Comparator.comparing(Mapping::getField))
            .collect(toList());

    assertThat(result)
        .isEqualTo(
            List.of(
                new Mapping(FragmentType.target, RoleType.key, fieldTuple("key1", "value1")),
                new Mapping(FragmentType.target, RoleType.key, fieldTuple("value2", "value2"))));
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
                mapping(FragmentType.node, RoleType.label, constantFieldTuple("Customer")),
                mapping(FragmentType.node, RoleType.label, constantFieldTuple("Buyer"))));
  }

  @Test
  public void supportsNodeBooleanPropertiesDefinedAsObjectArray() {
    JSONObject jsonMappings = new JSONObject();
    jsonMappings.put("label", "Placeholder");
    jsonMappings.put("keys", "placeholder");
    JSONArray booleanProps = new JSONArray();
    booleanProps.put(jsonFieldTuple("boolean_source_field1", "booleanNodeProperty1"));
    booleanProps.put(jsonFieldTuple("boolean_source_field2", "booleanNodeProperty2"));
    JSONObject properties = new JSONObject();
    properties.put("booleans", booleanProps);
    jsonMappings.put("properties", properties);

    List<Mapping> mappings =
        MappingMapper.parseMappings(target("my-target", TargetType.node), jsonMappings);
    List<Mapping> propertyMappings =
        mappings.stream().filter(mapping -> roleIs(mapping, RoleType.property)).collect(toList());

    assertThat(propertyMappings)
        .isEqualTo(
            List.of(
                typedMapping(
                    PropertyType.Boolean,
                    FragmentType.node,
                    RoleType.property,
                    fieldTuple("boolean_source_field1", "booleanNodeProperty1")),
                typedMapping(
                    PropertyType.Boolean,
                    FragmentType.node,
                    RoleType.property,
                    fieldTuple("boolean_source_field2", "booleanNodeProperty2"))));
  }

  @Test
  public void supportsEdgeBooleanPropertiesDefinedAsObjectArray() {
    JSONObject jsonMappings = new JSONObject();
    jsonMappings.put("type", "Placeholder");
    jsonMappings.put("keys", "placeholder");
    JSONArray booleanProps = new JSONArray();
    booleanProps.put(jsonFieldTuple("boolean_source_field1", "booleanNodeProperty1"));
    booleanProps.put(jsonFieldTuple("boolean_source_field2", "booleanNodeProperty2"));
    JSONObject properties = new JSONObject();
    properties.put("booleans", booleanProps);
    jsonMappings.put("properties", properties);

    List<Mapping> mappings =
        MappingMapper.parseMappings(target("my-target", TargetType.edge), jsonMappings);
    List<Mapping> propertyMappings =
        mappings.stream().filter(mapping -> roleIs(mapping, RoleType.property)).collect(toList());

    assertThat(propertyMappings)
        .isEqualTo(
            List.of(
                typedMapping(
                    PropertyType.Boolean,
                    FragmentType.rel,
                    RoleType.property,
                    fieldTuple("boolean_source_field1", "booleanNodeProperty1")),
                typedMapping(
                    PropertyType.Boolean,
                    FragmentType.rel,
                    RoleType.property,
                    fieldTuple("boolean_source_field2", "booleanNodeProperty2"))));
  }

  private static Target target(String name, TargetType type) {
    Target target = new Target();
    target.setName(name);
    target.setType(type);
    return target;
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
    JSONObject tuple = new JSONObject();
    tuple.put(field, property);
    return tuple;
  }

  private static Mapping typedMapping(
      PropertyType propertyType, FragmentType fragment, RoleType role, FieldNameTuple tuple) {
    Mapping mapping = new Mapping(fragment, role, tuple);
    mapping.setType(propertyType);
    mapping.setIndexed(false);
    return mapping;
  }

  private static Mapping indexedMapping(
      FragmentType fragment, RoleType role, FieldNameTuple tuple) {
    Mapping mapping = new Mapping(fragment, role, tuple);
    mapping.setIndexed(true);
    return mapping;
  }

  private static Mapping mapping(FragmentType fragment, RoleType role, FieldNameTuple tuple) {
    return new Mapping(fragment, role, tuple);
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

  private static boolean roleIs(Mapping mapping, RoleType roleType) {
    return mapping.getRole() == roleType;
  }
}
