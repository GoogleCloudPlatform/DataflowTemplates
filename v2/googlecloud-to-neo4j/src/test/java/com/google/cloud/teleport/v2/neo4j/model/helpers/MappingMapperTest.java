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
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.neo4j.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.model.enums.RoleType;
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.job.FieldNameTuple;
import com.google.cloud.teleport.v2.neo4j.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

public class MappingMapperTest {

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
}
