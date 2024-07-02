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
import static org.neo4j.importer.v1.targets.PropertyType.BOOLEAN;

import java.util.List;
import java.util.Map;
import org.json.JSONObject;
import org.junit.Test;
import org.neo4j.importer.v1.targets.NodeExistenceConstraint;
import org.neo4j.importer.v1.targets.NodeKeyConstraint;
import org.neo4j.importer.v1.targets.NodeSchema;
import org.neo4j.importer.v1.targets.NodeTarget;
import org.neo4j.importer.v1.targets.PropertyMapping;
import org.neo4j.importer.v1.targets.RelationshipExistenceConstraint;
import org.neo4j.importer.v1.targets.RelationshipSchema;
import org.neo4j.importer.v1.targets.WriteMode;

public class MappingMapperTest {

  @Test
  public void parses_edge_source_node_with_key_mappings_from_object() {
    var edge =
        new JSONObject(
            Map.of(
                "name", "an-edge",
                "source", "a-source",
                "mappings",
                    Map.of(
                        "source",
                        Map.of(
                            "label",
                            "Placeholder",
                            "key",
                            Map.of("key1", "value1", "key2", "value2")))));

    var result = MappingMapper.parseEdgeNode(edge, "source", WriteMode.CREATE);

    assertThat(result)
        .isEqualTo(
            new NodeTarget(
                true,
                "an-edge-source",
                "a-source",
                null,
                WriteMode.CREATE,
                null,
                List.of("Placeholder"),
                List.of(
                    new PropertyMapping("key1", "value1", null),
                    new PropertyMapping("key2", "value2", null)),
                nodeKeys(
                    List.of(
                        new NodeKeyConstraint(
                            "an-edge-source-Placeholder-node-single-key-for-value1-value2",
                            "Placeholder",
                            List.of("value1", "value2"),
                            null)))));
  }

  @Test
  public void parses_edge_source_node_with_key_mappings_from_object_array() {
    var edge =
        new JSONObject(
            Map.of(
                "name", "an-edge",
                "source", "a-source",
                "mappings",
                    Map.of(
                        "source",
                        Map.of(
                            "label",
                            "Placeholder",
                            "keys",
                            List.of(
                                Map.of("key1", "value1", "key2", "value2"),
                                Map.of("key3", "value3", "key4", "value4"))))));

    NodeTarget result = MappingMapper.parseEdgeNode(edge, "source", WriteMode.MERGE);

    assertThat(result)
        .isEqualTo(
            new NodeTarget(
                true,
                "an-edge-source",
                "a-source",
                null,
                WriteMode.MERGE,
                null,
                List.of("Placeholder"),
                List.of(
                    new PropertyMapping("key1", "value1", null),
                    new PropertyMapping("key2", "value2", null),
                    new PropertyMapping("key3", "value3", null),
                    new PropertyMapping("key4", "value4", null)),
                nodeKeys(
                    List.of(
                        new NodeKeyConstraint(
                            "an-edge-source-Placeholder-node-key-for-value1-value2",
                            "Placeholder",
                            List.of("value1", "value2"),
                            null),
                        new NodeKeyConstraint(
                            "an-edge-source-Placeholder-node-key-for-value3-value4",
                            "Placeholder",
                            List.of("value3", "value4"),
                            null)))));
  }

  @Test
  public void parses_edge_source_node_with_key_mappings_from_string_array() {
    var edge =
        new JSONObject(
            Map.of(
                "name", "an-edge",
                "source", "a-source",
                "mappings",
                    new JSONObject(
                        Map.of(
                            "source",
                            Map.of("label", "Placeholder", "key", List.of("value1", "value2"))))));

    var result = MappingMapper.parseEdgeNode(edge, "source", WriteMode.CREATE);

    assertThat(result)
        .isEqualTo(
            new NodeTarget(
                true,
                "an-edge-source",
                "a-source",
                null,
                WriteMode.CREATE,
                null,
                List.of("Placeholder"),
                List.of(
                    new PropertyMapping("value1", "value1", null),
                    new PropertyMapping("value2", "value2", null)),
                nodeKeys(
                    List.of(
                        new NodeKeyConstraint(
                            "an-edge-source-Placeholder-node-single-key-for-value1-value2",
                            "Placeholder",
                            List.of("value1", "value2"),
                            null)))));
  }

  @Test
  public void parses_edge_source_node_with_key_mappings_from_mixed_array() {
    var edge =
        new JSONObject(
            Map.of(
                "name", "an-edge",
                "source", "a-source",
                "mappings",
                    new JSONObject(
                        Map.of(
                            "type",
                            "TYPE",
                            "source",
                            Map.of(
                                "label",
                                "Placeholder",
                                "keys",
                                List.of("value1", Map.of("key2", "value2")))))));

    var node = MappingMapper.parseEdgeNode(edge, "source", WriteMode.MERGE);

    assertThat(node)
        .isEqualTo(
            new NodeTarget(
                true,
                "an-edge-source",
                "a-source",
                null,
                WriteMode.MERGE,
                null,
                List.of("Placeholder"),
                List.of(
                    new PropertyMapping("value1", "value1", null),
                    new PropertyMapping("key2", "value2", null)),
                nodeKeys(
                    List.of(
                        new NodeKeyConstraint(
                            "an-edge-source-Placeholder-node-key-for-value1",
                            "Placeholder",
                            List.of("value1"),
                            null),
                        new NodeKeyConstraint(
                            "an-edge-source-Placeholder-node-key-for-value2",
                            "Placeholder",
                            List.of("value2"),
                            null)))));
  }

  @Test
  public void parses_labels() {
    var mappings = Map.<String, Object>of("labels", new String[] {"\"Customer\"", "\"Buyer\""});

    var labels = MappingMapper.parseLabels(new JSONObject(mappings));

    assertThat(labels).isEqualTo(List.of("Customer", "Buyer"));
  }

  @Test
  public void parses_mandatory_mapping_array_of_node_target() {
    var mappings =
        new JSONObject(
            Map.of(
                "properties",
                Map.of("mandatory", List.of(Map.of("source_field", "targetProperty")))));

    var nodeSchema =
        MappingMapper.parseNodeSchema(
            "placeholder-target", List.of("Placeholder"), mappings, List.of());

    assertThat(nodeSchema)
        .isEqualTo(
            new NodeSchema(
                null,
                null,
                null,
                List.of(
                    new NodeExistenceConstraint(
                        "placeholder-target-Placeholder-node-not-null-for-targetProperty",
                        "Placeholder",
                        "targetProperty")),
                null,
                null,
                null,
                null,
                null));
  }

  @Test
  public void parses_mandatory_mapping_object_of_edge_target() {

    var schema =
        MappingMapper.parseEdgeSchema(
            "placeholder-target",
            "PLACEHOLDER",
            new JSONObject(
                Map.of(
                    "properties", Map.of("mandatory", Map.of("source_field", "targetProperty")))),
            List.of());

    assertThat(schema)
        .isEqualTo(
            new RelationshipSchema(
                null,
                null,
                null,
                List.of(
                    new RelationshipExistenceConstraint(
                        "placeholder-target-PLACEHOLDER-relationship-not-null-for-targetProperty",
                        "targetProperty")),
                null,
                null,
                null,
                null,
                null));
  }

  @Test
  public void supports_boolean_properties_defined_as_object_array() {
    var mappings =
        new JSONObject(
            Map.of(
                "properties",
                Map.of(
                    "keys",
                    "placeholder",
                    "booleans",
                    List.of(
                        Map.of("boolean_source_field1", "booleanNodeProperty1"),
                        Map.of("boolean_source_field2", "booleanNodeProperty2")))));

    var propertyMappings = MappingMapper.parseMappings(mappings);

    assertThat(propertyMappings)
        .isEqualTo(
            List.of(
                new PropertyMapping("placeholder", "placeholder", null),
                new PropertyMapping("boolean_source_field1", "booleanNodeProperty1", BOOLEAN),
                new PropertyMapping("boolean_source_field2", "booleanNodeProperty2", BOOLEAN)));
  }

  private static NodeSchema nodeKeys(List<NodeKeyConstraint> keys) {
    return new NodeSchema(null, keys, null, null, null, null, null, null, null);
  }
}
