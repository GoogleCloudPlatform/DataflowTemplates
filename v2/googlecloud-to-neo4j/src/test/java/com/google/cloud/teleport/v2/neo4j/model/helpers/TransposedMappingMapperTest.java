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

public class TransposedMappingMapperTest {

  @Test
  public void parsesMultipleKeyMappingsFromObjectForEdgeSourceNode() {
    Target target = edgeTarget();
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
        TransposedMappingMapper.parseMappings(target, mappings).stream()
            .filter(mapping -> mapping.getRole() == RoleType.key)
            .sorted(Comparator.comparing(Mapping::getField))
            .collect(toList());

    assertThat(result)
        .isEqualTo(
            List.of(
                new Mapping(FragmentType.source, RoleType.key, field("key1", "value1")),
                new Mapping(FragmentType.source, RoleType.key, field("key2", "value2"))));
  }

  @Test
  public void parsesMultipleKeyMappingsFromObjectArrayForEdgeSourceNode() {
    Target target = edgeTarget();
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
        TransposedMappingMapper.parseMappings(target, mappings).stream()
            .filter(mapping -> mapping.getRole() == RoleType.key)
            .collect(toList());

    assertThat(result)
        .isEqualTo(
            List.of(
                new Mapping(FragmentType.source, RoleType.key, field("key1", "value1")),
                new Mapping(FragmentType.source, RoleType.key, field("key2", "value2")),
                new Mapping(FragmentType.source, RoleType.key, field("key3", "value3")),
                new Mapping(FragmentType.source, RoleType.key, field("key4", "value4"))));
  }

  @Test
  public void parsesMultipleKeyMappingsFromStringArrayForEdgeSourceNode() {
    Target target = edgeTarget();
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
        TransposedMappingMapper.parseMappings(target, mappings).stream()
            .filter(mapping -> mapping.getRole() == RoleType.key)
            .collect(toList());

    assertThat(result)
        .isEqualTo(
            List.of(
                new Mapping(FragmentType.source, RoleType.key, field("value1", "value1")),
                new Mapping(FragmentType.source, RoleType.key, field("value2", "value2"))));
  }

  @Test
  public void parsesMultipleKeyMappingsFromMixedArrayForEdgeSourceNode() {
    Target target = edgeTarget();
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
        TransposedMappingMapper.parseMappings(target, mappings).stream()
            .filter(mapping -> mapping.getRole() == RoleType.key)
            .collect(toList());

    assertThat(result)
        .isEqualTo(
            List.of(
                new Mapping(FragmentType.source, RoleType.key, field("value1", "value1")),
                new Mapping(FragmentType.source, RoleType.key, field("key2", "value2"))));
  }

  @Test
  public void parsesMultipleKeyMappingsFromObjectForEdgeTargetNode() {
    Target target = edgeTarget();
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
        TransposedMappingMapper.parseMappings(target, mappings).stream()
            .filter(mapping -> mapping.getRole() == RoleType.key)
            .collect(toList());

    assertThat(result)
        .isEqualTo(
            List.of(
                new Mapping(FragmentType.target, RoleType.key, field("key1", "value1")),
                new Mapping(FragmentType.target, RoleType.key, field("key2", "value2"))));
  }

  @Test
  public void parsesMultipleKeyMappingsFromObjectArrayForEdgeTargetNode() {
    Target target = edgeTarget();
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
        TransposedMappingMapper.parseMappings(target, mappings).stream()
            .filter(mapping -> mapping.getRole() == RoleType.key)
            .collect(toList());

    assertThat(result)
        .isEqualTo(
            List.of(
                new Mapping(FragmentType.target, RoleType.key, field("key1", "value1")),
                new Mapping(FragmentType.target, RoleType.key, field("key2", "value2")),
                new Mapping(FragmentType.target, RoleType.key, field("key3", "value3")),
                new Mapping(FragmentType.target, RoleType.key, field("key4", "value4"))));
  }

  @Test
  public void parsesMultipleKeyMappingsFromStringArrayForEdgeTargetNode() {
    Target target = edgeTarget();
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
        TransposedMappingMapper.parseMappings(target, mappings).stream()
            .filter(mapping -> mapping.getRole() == RoleType.key)
            .collect(toList());

    assertThat(result)
        .isEqualTo(
            List.of(
                new Mapping(FragmentType.target, RoleType.key, field("value1", "value1")),
                new Mapping(FragmentType.target, RoleType.key, field("value2", "value2"))));
  }

  @Test
  public void parsesMultipleKeyMappingsFromMixedArrayForEdgeTargetNode() {
    Target target = edgeTarget();
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
        TransposedMappingMapper.parseMappings(target, mappings).stream()
            .filter(mapping -> mapping.getRole() == RoleType.key)
            .collect(toList());

    assertThat(result)
        .isEqualTo(
            List.of(
                new Mapping(FragmentType.target, RoleType.key, field("key1", "value1")),
                new Mapping(FragmentType.target, RoleType.key, field("value2", "value2"))));
  }

  @NotNull
  private static Target edgeTarget() {
    Target target = new Target();
    target.setType(TargetType.edge);
    return target;
  }

  @NotNull
  private static FieldNameTuple field(String field, String name) {
    FieldNameTuple fieldNameTuple = new FieldNameTuple();
    fieldNameTuple.setField(field);
    fieldNameTuple.setName(name);
    return fieldNameTuple;
  }
}
