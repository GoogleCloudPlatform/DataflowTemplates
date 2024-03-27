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
import org.neo4j.importer.v1.targets.NodeSchema;
import org.neo4j.importer.v1.targets.PropertyMapping;
import org.neo4j.importer.v1.targets.RelationshipExistenceConstraint;
import org.neo4j.importer.v1.targets.RelationshipNode;
import org.neo4j.importer.v1.targets.RelationshipSchema;

public class MappingMapperTest {

  @Test
  public void parsesMultipleKeyMappingsFromObjectForEdgeSourceNode() {
    var mappings =
        new JSONObject(
            Map.of(
                "source",
                Map.of("label", "Placeholder", "key", Map.of("key1", "value1", "key2", "value2"))));

    RelationshipNode result = MappingMapper.parseEdgeNode(mappings, "source");

    assertThat(result)
        .isEqualTo(
            new RelationshipNode(
                "Placeholder",
                List.of(
                    new PropertyMapping("key1", "value1", null),
                    new PropertyMapping("key2", "value2", null))));
  }

  @Test
  public void parsesMultipleKeyMappingsFromObjectArrayForEdgeSourceNode() {
    var mappings =
        new JSONObject(
            Map.of(
                "source",
                Map.of(
                    "label",
                    "Placeholder",
                    "key",
                    List.of(
                        Map.of("key1", "value1", "key2", "value2"),
                        Map.of("key3", "value3", "key4", "value4")))));

    RelationshipNode result = MappingMapper.parseEdgeNode(mappings, "source");

    assertThat(result)
        .isEqualTo(
            new RelationshipNode(
                "Placeholder",
                List.of(
                    new PropertyMapping("key1", "value1", null),
                    new PropertyMapping("key2", "value2", null),
                    new PropertyMapping("key3", "value3", null),
                    new PropertyMapping("key4", "value4", null))));
  }

  @Test
  public void parsesMultipleKeyMappingsFromStringArrayForEdgeSourceNode() {
    var mappings =
        new JSONObject(
            Map.of("source", Map.of("label", "Placeholder", "key", List.of("value1", "value2"))));

    RelationshipNode result = MappingMapper.parseEdgeNode(mappings, "source");

    assertThat(result)
        .isEqualTo(
            new RelationshipNode(
                "Placeholder",
                List.of(
                    new PropertyMapping("value1", "value1", null),
                    new PropertyMapping("value2", "value2", null))));
  }

  @Test
  public void parsesMultipleKeyMappingsFromMixedArrayForEdgeSourceNode() {
    var mappings =
        new JSONObject(
            Map.of(
                "source",
                Map.of(
                    "label", "Placeholder", "key", List.of("value1", Map.of("key2", "value2")))));

    RelationshipNode result = MappingMapper.parseEdgeNode(mappings, "source");

    assertThat(result)
        .isEqualTo(
            new RelationshipNode(
                "Placeholder",
                List.of(
                    new PropertyMapping("value1", "value1", null),
                    new PropertyMapping("key2", "value2", null))));
  }

  @Test
  public void parsesMultipleKeyMappingsFromObjectForEdgeTargetNode() {
    var mappings =
        new JSONObject(
            Map.of(
                "target",
                Map.of("label", "Placeholder", "key", Map.of("key1", "value1", "key2", "value2"))));

    RelationshipNode result = MappingMapper.parseEdgeNode(mappings, "target");

    assertThat(result)
        .isEqualTo(
            new RelationshipNode(
                "Placeholder",
                List.of(
                    new PropertyMapping("key1", "value1", null),
                    new PropertyMapping("key2", "value2", null))));
  }

  @Test
  public void parsesMultipleKeyMappingsFromObjectArrayForEdgeTargetNode() {
    var mappings =
        new JSONObject(
            Map.of(
                "target",
                Map.of(
                    "label",
                    "Placeholder",
                    "key",
                    List.of(
                        Map.of("key1", "value1", "key2", "value2"),
                        Map.of("key3", "value3", "key4", "value4")))));

    RelationshipNode result = MappingMapper.parseEdgeNode(mappings, "target");

    assertThat(result)
        .isEqualTo(
            new RelationshipNode(
                "Placeholder",
                List.of(
                    new PropertyMapping("key1", "value1", null),
                    new PropertyMapping("key2", "value2", null),
                    new PropertyMapping("key3", "value3", null),
                    new PropertyMapping("key4", "value4", null))));
  }

  @Test
  public void parsesMultipleKeyMappingsFromStringArrayForEdgeTargetNode() {
    var mappings =
        new JSONObject(
            Map.of("target", Map.of("label", "Placeholder", "key", List.of("value1", "value2"))));

    RelationshipNode result = MappingMapper.parseEdgeNode(mappings, "target");

    assertThat(result)
        .isEqualTo(
            new RelationshipNode(
                "Placeholder",
                List.of(
                    new PropertyMapping("value1", "value1", null),
                    new PropertyMapping("value2", "value2", null))));
  }

  @Test
  public void parsesMultipleKeyMappingsFromMixedArrayForEdgeTargetNode() {
    var mappings =
        new JSONObject(
            Map.of(
                "target",
                Map.of(
                    "label", "Placeholder", "key", List.of("value1", Map.of("key2", "value2")))));

    RelationshipNode result = MappingMapper.parseEdgeNode(mappings, "target");

    assertThat(result)
        .isEqualTo(
            new RelationshipNode(
                "Placeholder",
                List.of(
                    new PropertyMapping("value1", "value1", null),
                    new PropertyMapping("key2", "value2", null))));
  }

  @Test
  public void parsesLabels() {
    var mappings = Map.<String, Object>of("labels", new String[] {"\"Customer\"", "\"Buyer\""});

    List<String> labels = MappingMapper.parseLabels(new JSONObject(mappings));

    assertThat(labels).isEqualTo(List.of("Customer", "Buyer"));
  }

  @Test
  public void parsesMandatoryMappingArrayOfNodeTarget() {
    var mappings =
        new JSONObject(
            Map.of(
                "properties",
                Map.of("mandatory", List.of(Map.of("source_field", "targetProperty")))));

    NodeSchema nodeSchema =
        MappingMapper.parseNodeSchema("placeholder-target", List.of("Placeholder"), mappings);

    assertThat(nodeSchema)
        .isEqualTo(
            new NodeSchema(
                false,
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
  public void parsesMandatoryMappingObjectOfEdgeTarget() {
    var mappings =
        new JSONObject(
            Map.of("properties", Map.of("mandatory", Map.of("source_field", "targetProperty"))));

    RelationshipSchema schema =
        MappingMapper.parseEdgeSchema("placeholder-target", "PLACEHOLDER", mappings);

    assertThat(schema)
        .isEqualTo(
            new RelationshipSchema(
                false,
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
  public void supportsBooleanPropertiesDefinedAsObjectArray() {
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

    List<PropertyMapping> propertyMappings = MappingMapper.parseMappings(mappings);

    assertThat(propertyMappings)
        .isEqualTo(
            List.of(
                new PropertyMapping("placeholder", "placeholder", null),
                new PropertyMapping("boolean_source_field1", "booleanNodeProperty1", BOOLEAN),
                new PropertyMapping("boolean_source_field2", "booleanNodeProperty2", BOOLEAN)));
  }
}
