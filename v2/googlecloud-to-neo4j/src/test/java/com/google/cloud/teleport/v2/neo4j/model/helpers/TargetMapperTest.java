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
import static java.util.Map.entry;

import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.neo4j.importer.v1.targets.Aggregation;
import org.neo4j.importer.v1.targets.NodeExistenceConstraint;
import org.neo4j.importer.v1.targets.NodeKeyConstraint;
import org.neo4j.importer.v1.targets.NodeMatchMode;
import org.neo4j.importer.v1.targets.NodeRangeIndex;
import org.neo4j.importer.v1.targets.NodeSchema;
import org.neo4j.importer.v1.targets.NodeTarget;
import org.neo4j.importer.v1.targets.NodeUniqueConstraint;
import org.neo4j.importer.v1.targets.Order;
import org.neo4j.importer.v1.targets.OrderBy;
import org.neo4j.importer.v1.targets.PropertyMapping;
import org.neo4j.importer.v1.targets.PropertyType;
import org.neo4j.importer.v1.targets.RelationshipExistenceConstraint;
import org.neo4j.importer.v1.targets.RelationshipKeyConstraint;
import org.neo4j.importer.v1.targets.RelationshipRangeIndex;
import org.neo4j.importer.v1.targets.RelationshipSchema;
import org.neo4j.importer.v1.targets.RelationshipTarget;
import org.neo4j.importer.v1.targets.RelationshipUniqueConstraint;
import org.neo4j.importer.v1.targets.SourceTransformations;
import org.neo4j.importer.v1.targets.WriteMode;

@SuppressWarnings("deprecation")
public class TargetMapperTest {

  @Test
  public void parses_minimal_node_target() {
    var nodeTarget =
        new JSONObject(
            Map.of(
                "node",
                Map.of(
                    "name", "",
                    "mode", "append",
                    "mappings", Map.of())));

    var targets =
        TargetMapper.parse(
            new JSONArray(List.of(nodeTarget)), new OptionsParams(), new JobSpecIndex(), false);

    assertThat(targets.getNodes())
        .isEqualTo(
            List.of(
                new NodeTarget(
                    true, "node/0", "", null, WriteMode.CREATE, null, List.of(), List.of(), null)));
    assertThat(targets.getRelationships()).isEmpty();
    assertThat(targets.getCustomQueries()).isEmpty();
  }

  @Test
  public void parses_node_target() {
    var nodeTarget =
        new JSONObject(
            Map.of(
                "node",
                Map.of(
                    "active",
                    false,
                    "name",
                    "a-target",
                    "source",
                    "a-source",
                    "mode",
                    "merge",
                    "transform",
                    Map.of(
                        "group",
                        true,
                        "aggregations",
                        List.of(
                            Map.of("field", "field01", "expr", "42"),
                            Map.of("field", "field02", "expr", "24")),
                        "where",
                        "1+2=3",
                        "order_by",
                        "field01 ASC, field02 DESC",
                        "limit",
                        100),
                    "mappings",
                    Map.of(
                        "labels", List.of("\"Placeholder1\"", "\"Placeholder2\""),
                        "properties",
                            Map.<String, Object>ofEntries(
                                entry("key", List.of(Map.of("field01", "prop01"))),
                                entry("keys", List.of(Map.of("field02", "prop02"))),
                                entry("unique", List.of(Map.of("field03", "prop03"))),
                                entry("mandatory", List.of(Map.of("field04", "prop04"))),
                                entry("indexed", List.of(Map.of("field05", "prop05"))),
                                entry("dates", List.of(Map.of("field06", "prop06"))),
                                entry("doubles", List.of(Map.of("field07", "prop07"))),
                                entry("floats", List.of(Map.of("field08", "prop08"))),
                                entry("longs", List.of(Map.of("field09", "prop09"))),
                                entry("integers", List.of(Map.of("field10", "prop10"))),
                                entry("strings", List.of(Map.of("field11", "prop11"))),
                                entry("points", List.of(Map.of("field12", "prop12"))),
                                entry("booleans", List.of(Map.of("field13", "prop13"))),
                                entry(
                                    "bytearrays",
                                    List.of(Map.of("field14", "prop14"), "prop15")))))));

    var targets =
        TargetMapper.parse(
            new JSONArray(List.of(nodeTarget)), new OptionsParams(), new JobSpecIndex(), false);

    var expectedTarget =
        new NodeTarget(
            false,
            "a-target",
            "a-source",
            null,
            WriteMode.MERGE,
            new SourceTransformations(
                true,
                List.of(new Aggregation("42", "field01"), new Aggregation("24", "field02")),
                "1+2=3",
                List.of(new OrderBy("field01", Order.ASC), new OrderBy("field02", Order.DESC)),
                100),
            List.of("Placeholder1", "Placeholder2"),
            List.of(
                new PropertyMapping("field01", "prop01", null),
                new PropertyMapping("field02", "prop02", null),
                new PropertyMapping("field03", "prop03", null),
                new PropertyMapping("field04", "prop04", null),
                new PropertyMapping("field05", "prop05", null),
                new PropertyMapping("field06", "prop06", PropertyType.ZONED_DATETIME),
                new PropertyMapping("field07", "prop07", PropertyType.FLOAT),
                new PropertyMapping("field08", "prop08", PropertyType.FLOAT),
                new PropertyMapping("field09", "prop09", PropertyType.INTEGER),
                new PropertyMapping("field10", "prop10", PropertyType.INTEGER),
                new PropertyMapping("field11", "prop11", PropertyType.STRING),
                new PropertyMapping("field12", "prop12", PropertyType.POINT),
                new PropertyMapping("field13", "prop13", PropertyType.BOOLEAN),
                new PropertyMapping("field14", "prop14", PropertyType.BYTE_ARRAY),
                new PropertyMapping("prop15", "prop15", PropertyType.BYTE_ARRAY)),
            new NodeSchema(
                null,
                List.of(
                    new NodeKeyConstraint(
                        "a-target-Placeholder1-node-single-key-for-prop01",
                        "Placeholder1",
                        List.of("prop01"),
                        null),
                    new NodeKeyConstraint(
                        "a-target-Placeholder2-node-single-key-for-prop01",
                        "Placeholder2",
                        List.of("prop01"),
                        null),
                    new NodeKeyConstraint(
                        "a-target-Placeholder1-node-key-for-prop02",
                        "Placeholder1",
                        List.of("prop02"),
                        null),
                    new NodeKeyConstraint(
                        "a-target-Placeholder2-node-key-for-prop02",
                        "Placeholder2",
                        List.of("prop02"),
                        null)),
                List.of(
                    new NodeUniqueConstraint(
                        "a-target-Placeholder1-node-unique-for-prop03",
                        "Placeholder1",
                        List.of("prop03"),
                        null),
                    new NodeUniqueConstraint(
                        "a-target-Placeholder2-node-unique-for-prop03",
                        "Placeholder2",
                        List.of("prop03"),
                        null)),
                List.of(
                    new NodeExistenceConstraint(
                        "a-target-Placeholder1-node-not-null-for-prop04", "Placeholder1", "prop04"),
                    new NodeExistenceConstraint(
                        "a-target-Placeholder2-node-not-null-for-prop04",
                        "Placeholder2",
                        "prop04")),
                List.of(
                    new NodeRangeIndex(
                        "a-target-Placeholder1-node-range-index-for-prop05",
                        "Placeholder1",
                        List.of("prop05")),
                    new NodeRangeIndex(
                        "a-target-Placeholder2-node-range-index-for-prop05",
                        "Placeholder2",
                        List.of("prop05"))),
                null,
                null,
                null,
                null));
    assertThat(targets.getNodes()).isEqualTo(List.of(expectedTarget));
    assertThat(targets.getRelationships()).isEmpty();
    assertThat(targets.getCustomQueries()).isEmpty();
  }

  @Test
  public void parses_node_target_indexing_all_properties() {
    var nodeTarget =
        new JSONObject(
            Map.of(
                "node",
                Map.of(
                    "name", "a-target",
                    "source", "a-source",
                    "mode", "merge",
                    "mappings",
                        Map.of(
                            "labels", List.of("\"Placeholder\""),
                            "properties",
                                Map.<String, Object>ofEntries(
                                    entry("key", List.of(Map.of("field01", "prop01"))),
                                    entry("keys", List.of(Map.of("field02", "prop02"))),
                                    entry("unique", List.of(Map.of("field03", "prop03"))),
                                    entry("mandatory", List.of(Map.of("field04", "prop04"))),
                                    entry("indexed", List.of(Map.of("field05", "prop05"))),
                                    entry("dates", List.of(Map.of("field06", "prop06"))),
                                    entry("doubles", List.of(Map.of("field07", "prop07"))),
                                    entry("floats", List.of(Map.of("field08", "prop08"))),
                                    entry("longs", List.of(Map.of("field09", "prop09"))),
                                    entry("integers", List.of(Map.of("field10", "prop10"))),
                                    entry("strings", List.of(Map.of("field11", "prop11"))),
                                    entry("points", List.of(Map.of("field12", "prop12"))),
                                    entry("booleans", List.of(Map.of("field13", "prop13"))),
                                    entry(
                                        "bytearrays",
                                        List.of(Map.of("field14", "prop14"), "prop15")))))));

    var targets =
        TargetMapper.parse(
            new JSONArray(List.of(nodeTarget)), new OptionsParams(), new JobSpecIndex(), true);

    var expectedSchema =
        new NodeSchema(
            null,
            List.of(
                new NodeKeyConstraint(
                    "a-target-Placeholder-node-single-key-for-prop01",
                    "Placeholder",
                    List.of("prop01"),
                    null),
                new NodeKeyConstraint(
                    "a-target-Placeholder-node-key-for-prop02",
                    "Placeholder",
                    List.of("prop02"),
                    null)),
            List.of(
                new NodeUniqueConstraint(
                    "a-target-Placeholder-node-unique-for-prop03",
                    "Placeholder",
                    List.of("prop03"),
                    null)),
            List.of(
                new NodeExistenceConstraint(
                    "a-target-Placeholder-node-not-null-for-prop04", "Placeholder", "prop04")),
            List.of(
                new NodeRangeIndex(
                    "a-target-Placeholder-node-range-index-for-prop05",
                    "Placeholder",
                    List.of("prop05")),
                new NodeRangeIndex(
                    "node/default-index-for-Placeholder-prop04", "Placeholder", List.of("prop04")),
                new NodeRangeIndex(
                    "node/default-index-for-Placeholder-prop06", "Placeholder", List.of("prop06")),
                new NodeRangeIndex(
                    "node/default-index-for-Placeholder-prop07", "Placeholder", List.of("prop07")),
                new NodeRangeIndex(
                    "node/default-index-for-Placeholder-prop08", "Placeholder", List.of("prop08")),
                new NodeRangeIndex(
                    "node/default-index-for-Placeholder-prop09", "Placeholder", List.of("prop09")),
                new NodeRangeIndex(
                    "node/default-index-for-Placeholder-prop10", "Placeholder", List.of("prop10")),
                new NodeRangeIndex(
                    "node/default-index-for-Placeholder-prop11", "Placeholder", List.of("prop11")),
                new NodeRangeIndex(
                    "node/default-index-for-Placeholder-prop12", "Placeholder", List.of("prop12")),
                new NodeRangeIndex(
                    "node/default-index-for-Placeholder-prop13", "Placeholder", List.of("prop13")),
                new NodeRangeIndex(
                    "node/default-index-for-Placeholder-prop14", "Placeholder", List.of("prop14")),
                new NodeRangeIndex(
                    "node/default-index-for-Placeholder-prop15", "Placeholder", List.of("prop15"))),
            null,
            null,
            null,
            null);
    var target = targets.getNodes().iterator().next();
    assertThat(target.getSchema()).isEqualTo(expectedSchema);
  }

  @Test
  public void parses_minimal_edge_target() {
    var edgeTarget =
        new JSONObject(
            Map.of(
                "edge",
                Map.of(
                    "name", "",
                    "mode", "append",
                    "mappings", Map.of())));

    var targets =
        TargetMapper.parse(
            new JSONArray(List.of(edgeTarget)), new OptionsParams(), new JobSpecIndex(), false);

    assertThat(targets.getNodes()).isEmpty();
    assertThat(targets.getRelationships())
        .isEqualTo(
            List.of(
                new RelationshipTarget(
                    true,
                    "edge/0",
                    "",
                    null,
                    null,
                    WriteMode.CREATE,
                    NodeMatchMode.MATCH,
                    null,
                    null,
                    null,
                    List.of(),
                    null)));
    assertThat(targets.getCustomQueries()).isEmpty();
  }

  @Test
  public void parses_edge_target_with_matching_node_targets() {
    var sourceNodeTarget =
        new JSONObject(
            Map.of(
                "node",
                Map.of(
                    "name", "a-source-node-target",
                    "source", "a-source",
                    "mode", "append",
                    "mappings",
                        Map.of("label", "Placeholder1", "properties", Map.of("key", "prop1")))));
    var targetNodeTarget =
        new JSONObject(
            Map.of(
                "node",
                Map.of(
                    "name", "a-target-node-target",
                    "source", "a-source",
                    "mode", "append",
                    "mappings",
                        Map.of("label", "Placeholder2", "properties", Map.of("key", "prop2")))));
    var edgeTarget =
        new JSONObject(
            Map.of(
                "edge",
                Map.of(
                    "active",
                    false,
                    "name",
                    "a-target",
                    "source",
                    "a-source",
                    "mode",
                    "MERGE",
                    "node_match_mode",
                    "match",
                    "transform",
                    Map.of(
                        "group",
                        true,
                        "aggregations",
                        List.of(
                            Map.of("field", "field01", "expr", "42"),
                            Map.of("field", "field02", "expr", "24")),
                        "where",
                        "1+2=3",
                        "order_by",
                        "field01 ASC, field02 DESC",
                        "limit",
                        100),
                    "mappings",
                    Map.of(
                        "type",
                        "\"PLACEHOLDER\"",
                        "source",
                        Map.of(
                            "label", "Placeholder1",
                            "key", "prop1"),
                        "target",
                        Map.of(
                            "label", "Placeholder2",
                            "key", "prop2"),
                        "properties",
                        Map.<String, Object>ofEntries(
                            entry("key", List.of(Map.of("field01", "prop01"))),
                            entry("keys", List.of(Map.of("field02", "prop02"))),
                            entry("unique", List.of(Map.of("field03", "prop03"))),
                            entry("mandatory", List.of(Map.of("field04", "prop04"))),
                            entry("indexed", List.of(Map.of("field05", "prop05"))),
                            entry("dates", List.of(Map.of("field06", "prop06"))),
                            entry("doubles", List.of(Map.of("field07", "prop07"))),
                            entry("floats", List.of(Map.of("field08", "prop08"))),
                            entry("longs", List.of(Map.of("field09", "prop09"))),
                            entry("integers", List.of(Map.of("field10", "prop10"))),
                            entry("strings", List.of(Map.of("field11", "prop11"))),
                            entry("points", List.of(Map.of("field12", "prop12"))),
                            entry("booleans", List.of(Map.of("field13", "prop13"))),
                            entry(
                                "bytearrays", List.of(Map.of("field14", "prop14"), "prop15")))))));

    var targets =
        TargetMapper.parse(
            new JSONArray(List.of(sourceNodeTarget, targetNodeTarget, edgeTarget)),
            new OptionsParams(),
            new JobSpecIndex(),
            false);

    assertThat(targets.getNodes())
        .isEqualTo(
            List.of(
                new NodeTarget(
                    true,
                    "a-source-node-target",
                    "a-source",
                    null,
                    WriteMode.CREATE,
                    null,
                    List.of("Placeholder1"),
                    List.of(new PropertyMapping("prop1", "prop1", null)),
                    new NodeSchema(
                        null,
                        List.of(
                            new NodeKeyConstraint(
                                "a-source-node-target-Placeholder1-node-single-key-for-prop1",
                                "Placeholder1",
                                List.of("prop1"),
                                null)),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null)),
                new NodeTarget(
                    true,
                    "a-target-node-target",
                    "a-source",
                    null,
                    WriteMode.CREATE,
                    null,
                    List.of("Placeholder2"),
                    List.of(new PropertyMapping("prop2", "prop2", null)),
                    new NodeSchema(
                        null,
                        List.of(
                            new NodeKeyConstraint(
                                "a-target-node-target-Placeholder2-node-single-key-for-prop2",
                                "Placeholder2",
                                List.of("prop2"),
                                null)),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null))));
    assertThat(targets.getRelationships())
        .isEqualTo(
            List.of(
                new RelationshipTarget(
                    false,
                    "a-target",
                    "a-source",
                    null,
                    "PLACEHOLDER",
                    WriteMode.MERGE,
                    NodeMatchMode.MATCH,
                    new SourceTransformations(
                        true,
                        List.of(new Aggregation("42", "field01"), new Aggregation("24", "field02")),
                        "1+2=3",
                        List.of(
                            new OrderBy("field01", Order.ASC), new OrderBy("field02", Order.DESC)),
                        100),
                    "a-source-node-target",
                    "a-target-node-target",
                    List.of(
                        new PropertyMapping("field01", "prop01", null),
                        new PropertyMapping("field02", "prop02", null),
                        new PropertyMapping("field03", "prop03", null),
                        new PropertyMapping("field04", "prop04", null),
                        new PropertyMapping("field05", "prop05", null),
                        new PropertyMapping("field06", "prop06", PropertyType.ZONED_DATETIME),
                        new PropertyMapping("field07", "prop07", PropertyType.FLOAT),
                        new PropertyMapping("field08", "prop08", PropertyType.FLOAT),
                        new PropertyMapping("field09", "prop09", PropertyType.INTEGER),
                        new PropertyMapping("field10", "prop10", PropertyType.INTEGER),
                        new PropertyMapping("field11", "prop11", PropertyType.STRING),
                        new PropertyMapping("field12", "prop12", PropertyType.POINT),
                        new PropertyMapping("field13", "prop13", PropertyType.BOOLEAN),
                        new PropertyMapping("field14", "prop14", PropertyType.BYTE_ARRAY),
                        new PropertyMapping("prop15", "prop15", PropertyType.BYTE_ARRAY)),
                    new RelationshipSchema(
                        null,
                        List.of(
                            new RelationshipKeyConstraint(
                                "a-target-PLACEHOLDER-relationship-single-key-for-prop01",
                                List.of("prop01"),
                                null),
                            new RelationshipKeyConstraint(
                                "a-target-PLACEHOLDER-relationship-key-for-prop02",
                                List.of("prop02"),
                                null)),
                        List.of(
                            new RelationshipUniqueConstraint(
                                "a-target-PLACEHOLDER-relationship-unique-for-prop03",
                                List.of("prop03"),
                                null)),
                        List.of(
                            new RelationshipExistenceConstraint(
                                "a-target-PLACEHOLDER-relationship-not-null-for-prop04", "prop04")),
                        List.of(
                            new RelationshipRangeIndex(
                                "a-target-PLACEHOLDER-relationship-range-index-for-prop05",
                                List.of("prop05"))),
                        null,
                        null,
                        null,
                        null))));
    assertThat(targets.getCustomQueries()).isEmpty();
  }

  @Test
  public void parses_edge_target_without_matching_node_targets() {
    var edgeTarget =
        new JSONObject(
            Map.of(
                "edge",
                Map.of(
                    "active", false,
                    "name", "a-target",
                    "source", "a-source",
                    "mode", "MERGE",
                    "node_match_mode", "match",
                    "mappings",
                        Map.of(
                            "type",
                            "\"PLACEHOLDER\"",
                            "source",
                            Map.of(
                                "label", "Placeholder1",
                                "key", "prop1"),
                            "target",
                            Map.of(
                                "label", "Placeholder2",
                                "key", "prop2"),
                            "properties",
                            Map.<String, Object>ofEntries(
                                entry("key", List.of(Map.of("field01", "prop01"))),
                                entry("keys", List.of(Map.of("field02", "prop02"))),
                                entry("unique", List.of(Map.of("field03", "prop03"))),
                                entry("mandatory", List.of(Map.of("field04", "prop04"))),
                                entry("indexed", List.of(Map.of("field05", "prop05"))),
                                entry("dates", List.of(Map.of("field06", "prop06"))),
                                entry("doubles", List.of(Map.of("field07", "prop07"))),
                                entry("floats", List.of(Map.of("field08", "prop08"))),
                                entry("longs", List.of(Map.of("field09", "prop09"))),
                                entry("integers", List.of(Map.of("field10", "prop10"))),
                                entry("strings", List.of(Map.of("field11", "prop11"))),
                                entry("points", List.of(Map.of("field12", "prop12"))),
                                entry("booleans", List.of(Map.of("field13", "prop13"))),
                                entry(
                                    "bytearrays",
                                    List.of(Map.of("field14", "prop14"), "prop15")))))));

    var targets =
        TargetMapper.parse(
            new JSONArray(List.of(edgeTarget)), new OptionsParams(), new JobSpecIndex(), false);

    assertThat(targets.getNodes())
        .isEqualTo(
            List.of(
                new NodeTarget(
                    false,
                    "a-target-source",
                    "a-source",
                    null,
                    WriteMode.MERGE,
                    null,
                    List.of("Placeholder1"),
                    List.of(new PropertyMapping("prop1", "prop1", null)),
                    new NodeSchema(
                        null,
                        List.of(
                            new NodeKeyConstraint(
                                "a-target-source-Placeholder1-node-single-key-for-prop1",
                                "Placeholder1",
                                List.of("prop1"),
                                null)),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null)),
                new NodeTarget(
                    false,
                    "a-target-target",
                    "a-source",
                    null,
                    WriteMode.MERGE,
                    null,
                    List.of("Placeholder2"),
                    List.of(new PropertyMapping("prop2", "prop2", null)),
                    new NodeSchema(
                        null,
                        List.of(
                            new NodeKeyConstraint(
                                "a-target-target-Placeholder2-node-single-key-for-prop2",
                                "Placeholder2",
                                List.of("prop2"),
                                null)),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null))));
    assertThat(targets.getRelationships())
        .isEqualTo(
            List.of(
                new RelationshipTarget(
                    false,
                    "a-target",
                    "a-source",
                    null,
                    "PLACEHOLDER",
                    WriteMode.MERGE,
                    NodeMatchMode.MATCH,
                    null,
                    "a-target-source",
                    "a-target-target",
                    List.of(
                        new PropertyMapping("field01", "prop01", null),
                        new PropertyMapping("field02", "prop02", null),
                        new PropertyMapping("field03", "prop03", null),
                        new PropertyMapping("field04", "prop04", null),
                        new PropertyMapping("field05", "prop05", null),
                        new PropertyMapping("field06", "prop06", PropertyType.ZONED_DATETIME),
                        new PropertyMapping("field07", "prop07", PropertyType.FLOAT),
                        new PropertyMapping("field08", "prop08", PropertyType.FLOAT),
                        new PropertyMapping("field09", "prop09", PropertyType.INTEGER),
                        new PropertyMapping("field10", "prop10", PropertyType.INTEGER),
                        new PropertyMapping("field11", "prop11", PropertyType.STRING),
                        new PropertyMapping("field12", "prop12", PropertyType.POINT),
                        new PropertyMapping("field13", "prop13", PropertyType.BOOLEAN),
                        new PropertyMapping("field14", "prop14", PropertyType.BYTE_ARRAY),
                        new PropertyMapping("prop15", "prop15", PropertyType.BYTE_ARRAY)),
                    new RelationshipSchema(
                        null,
                        List.of(
                            new RelationshipKeyConstraint(
                                "a-target-PLACEHOLDER-relationship-single-key-for-prop01",
                                List.of("prop01"),
                                null),
                            new RelationshipKeyConstraint(
                                "a-target-PLACEHOLDER-relationship-key-for-prop02",
                                List.of("prop02"),
                                null)),
                        List.of(
                            new RelationshipUniqueConstraint(
                                "a-target-PLACEHOLDER-relationship-unique-for-prop03",
                                List.of("prop03"),
                                null)),
                        List.of(
                            new RelationshipExistenceConstraint(
                                "a-target-PLACEHOLDER-relationship-not-null-for-prop04", "prop04")),
                        List.of(
                            new RelationshipRangeIndex(
                                "a-target-PLACEHOLDER-relationship-range-index-for-prop05",
                                List.of("prop05"))),
                        null,
                        null,
                        null,
                        null))));
    assertThat(targets.getCustomQueries()).isEmpty();
  }

  @Test
  public void parses_edge_target_indexing_all_properties() {
    var edgeTarget =
        new JSONObject(
            Map.of(
                "edge",
                Map.of(
                    "active", false,
                    "name", "a-target",
                    "source", "a-source",
                    "mode", "MERGE",
                    "node_match_mode", "match",
                    "mappings",
                        Map.of(
                            "type",
                            "\"PLACEHOLDER\"",
                            "source",
                            Map.of(
                                "label", "Placeholder1",
                                "key", "prop1"),
                            "target",
                            Map.of(
                                "label", "Placeholder2",
                                "key", "prop2"),
                            "properties",
                            Map.<String, Object>ofEntries(
                                entry("key", List.of(Map.of("field01", "prop01"))),
                                entry("keys", List.of(Map.of("field02", "prop02"))),
                                entry("unique", List.of(Map.of("field03", "prop03"))),
                                entry("mandatory", List.of(Map.of("field04", "prop04"))),
                                entry("indexed", List.of(Map.of("field05", "prop05"))),
                                entry("dates", List.of(Map.of("field06", "prop06"))),
                                entry("doubles", List.of(Map.of("field07", "prop07"))),
                                entry("floats", List.of(Map.of("field08", "prop08"))),
                                entry("longs", List.of(Map.of("field09", "prop09"))),
                                entry("integers", List.of(Map.of("field10", "prop10"))),
                                entry("strings", List.of(Map.of("field11", "prop11"))),
                                entry("points", List.of(Map.of("field12", "prop12"))),
                                entry("booleans", List.of(Map.of("field13", "prop13"))),
                                entry(
                                    "bytearrays",
                                    List.of(Map.of("field14", "prop14"), "prop15")))))));

    var targets =
        TargetMapper.parse(
            new JSONArray(List.of(edgeTarget)), new OptionsParams(), new JobSpecIndex(), true);

    var expectedSchema =
        new RelationshipSchema(
            null,
            List.of(
                new RelationshipKeyConstraint(
                    "a-target-PLACEHOLDER-relationship-single-key-for-prop01",
                    List.of("prop01"),
                    null),
                new RelationshipKeyConstraint(
                    "a-target-PLACEHOLDER-relationship-key-for-prop02", List.of("prop02"), null)),
            List.of(
                new RelationshipUniqueConstraint(
                    "a-target-PLACEHOLDER-relationship-unique-for-prop03",
                    List.of("prop03"),
                    null)),
            List.of(
                new RelationshipExistenceConstraint(
                    "a-target-PLACEHOLDER-relationship-not-null-for-prop04", "prop04")),
            List.of(
                new RelationshipRangeIndex(
                    "a-target-PLACEHOLDER-relationship-range-index-for-prop05", List.of("prop05")),
                new RelationshipRangeIndex(
                    "edge/default-index-for-PLACEHOLDER-prop04", List.of("prop04")),
                new RelationshipRangeIndex(
                    "edge/default-index-for-PLACEHOLDER-prop06", List.of("prop06")),
                new RelationshipRangeIndex(
                    "edge/default-index-for-PLACEHOLDER-prop07", List.of("prop07")),
                new RelationshipRangeIndex(
                    "edge/default-index-for-PLACEHOLDER-prop08", List.of("prop08")),
                new RelationshipRangeIndex(
                    "edge/default-index-for-PLACEHOLDER-prop09", List.of("prop09")),
                new RelationshipRangeIndex(
                    "edge/default-index-for-PLACEHOLDER-prop10", List.of("prop10")),
                new RelationshipRangeIndex(
                    "edge/default-index-for-PLACEHOLDER-prop11", List.of("prop11")),
                new RelationshipRangeIndex(
                    "edge/default-index-for-PLACEHOLDER-prop12", List.of("prop12")),
                new RelationshipRangeIndex(
                    "edge/default-index-for-PLACEHOLDER-prop13", List.of("prop13")),
                new RelationshipRangeIndex(
                    "edge/default-index-for-PLACEHOLDER-prop14", List.of("prop14")),
                new RelationshipRangeIndex(
                    "edge/default-index-for-PLACEHOLDER-prop15", List.of("prop15"))),
            null,
            null,
            null,
            null);

    var relationshipTarget = targets.getRelationships().iterator().next();
    assertThat(relationshipTarget.getSchema()).isEqualTo(expectedSchema);
  }

  @Test
  public void parses_custom_query_target() {
    JSONObject jsonTarget = jsonTargetOfType("custom_query");
    JSONObject customObject = jsonTarget.getJSONObject("custom_query");
    customObject.put("query", "UNWIND $rows AS row CREATE (:Node {prop: row.prop})");

    var targets =
        TargetMapper.parse(
            new JSONArray(List.of(jsonTarget)), new OptionsParams(), new JobSpecIndex(), false);

    assertThat(targets.getNodes()).isEmpty();
    assertThat(targets.getRelationships()).isEmpty();
    assertThat(targets.getCustomQueries()).hasSize(1);
    var target = targets.getCustomQueries().get(0);
    assertThat(target.getQuery()).isEqualTo("UNWIND $rows AS row CREATE (:Node {prop: row.prop})");
  }

  @Test
  public void combines_property_mapping_information() {
    var nodeTarget =
        new JSONObject(
            Map.of(
                "node",
                Map.of(
                    "name", "a-target",
                    "mode", "append",
                    "mappings",
                        Map.of(
                            "label",
                            "\"Placeholder\"",
                            "properties",
                            Map.of(
                                "booleans", List.of(Map.of("field1", "prop1")),
                                "integers", List.of("prop2"),
                                "key", Map.of("field1", "prop1"),
                                "unique", "prop2")))));

    var targets =
        TargetMapper.parse(
            new JSONArray(List.of(nodeTarget)), new OptionsParams(), new JobSpecIndex(), false);

    var target = targets.getNodes().iterator().next();
    assertThat(target.getProperties())
        .isEqualTo(
            List.of(
                new PropertyMapping("field1", "prop1", PropertyType.BOOLEAN),
                new PropertyMapping("prop2", "prop2", PropertyType.INTEGER)));
    assertThat(target.getSchema())
        .isEqualTo(
            new NodeSchema(
                null,
                List.of(
                    new NodeKeyConstraint(
                        "a-target-Placeholder-node-single-key-for-prop1",
                        "Placeholder",
                        List.of("prop1"),
                        null)),
                List.of(
                    new NodeUniqueConstraint(
                        "a-target-Placeholder-node-unique-for-prop2",
                        "Placeholder",
                        List.of("prop2"),
                        null)),
                null,
                null,
                null,
                null,
                null,
                null));
  }

  @Test
  public void sets_specified_match_mode_for_edge_targets() {
    JSONObject target = jsonTargetOfType("edge");
    JSONObject edge = target.getJSONObject("edge");
    edge.put("mode", "merge");
    edge.put("edge_nodes_match_mode", "merge");
    edge.put(
        "mappings",
        Map.of(
            "type", "TYPE",
            "source", Map.of("label", "Placeholder1", "key", "key1"),
            "target", Map.of("label", "Placeholder2", "key", "key2")));

    var targets =
        TargetMapper.parse(
            new JSONArray(List.of(target)), new OptionsParams(), new JobSpecIndex(), false);

    assertThat(targets.getRelationships()).hasSize(1);
    assertThat(targets.getRelationships().get(0).getNodeMatchMode()).isEqualTo(NodeMatchMode.MERGE);
  }

  @Test
  public void sets_specified_match_mode_for_edge_targets_in_append_mode() {
    JSONObject target = jsonTargetOfType("edge");
    JSONObject edge = target.getJSONObject("edge");
    edge.put("mode", "append");
    edge.put("edge_nodes_match_mode", "merge");
    edge.put(
        "mappings",
        Map.of(
            "type", "TYPE",
            "source", Map.of("label", "Placeholder1", "key", "key1"),
            "target", Map.of("label", "Placeholder2", "key", "key2")));

    var targets =
        TargetMapper.parse(
            new JSONArray(List.of(target)), new OptionsParams(), new JobSpecIndex(), false);

    assertThat(targets.getRelationships()).hasSize(1);
    assertThat(targets.getRelationships().get(0).getNodeMatchMode()).isEqualTo(NodeMatchMode.MERGE);
  }

  @Test
  public void sets_default_node_match_mode_for_edge_targets_to_merge() {
    JSONObject target = jsonTargetOfType("edge");
    JSONObject edge = target.getJSONObject("edge");
    edge.put("mode", "merge");
    edge.put(
        "mappings",
        Map.of(
            "type", "TYPE",
            "source", Map.of("label", "Placeholder1", "key", "key1"),
            "target", Map.of("label", "Placeholder2", "key", "key2")));

    var targets =
        TargetMapper.parse(
            new JSONArray(List.of(target)), new OptionsParams(), new JobSpecIndex(), false);

    assertThat(targets.getRelationships()).hasSize(1);
    assertThat(targets.getRelationships().get(0).getNodeMatchMode()).isEqualTo(NodeMatchMode.MATCH);
  }

  @Test
  public void sets_default_node_match_mode_for_edge_targets_to_create() {
    JSONObject target = jsonTargetOfType("edge");
    JSONObject edge = target.getJSONObject("edge");
    edge.put("mode", "append");
    edge.put(
        "mappings",
        Map.of(
            "type", "TYPE",
            "source", Map.of("label", "Placeholder1", "key", "key1"),
            "target", Map.of("label", "Placeholder2", "key", "key2")));

    var targets =
        TargetMapper.parse(
            new JSONArray(List.of(target)), new OptionsParams(), new JobSpecIndex(), false);

    assertThat(targets.getRelationships()).hasSize(1);
    assertThat(targets.getRelationships().get(0).getNodeMatchMode()).isEqualTo(NodeMatchMode.MATCH);
  }

  @Test
  public void parses_simple_order_by() {
    JSONObject orderBy = new JSONObject(Map.of("order_by", "col"));

    List<OrderBy> clauses = TargetMapper.parseOrderBy(orderBy);

    assertThat(clauses).isEqualTo(List.of(new OrderBy("col", null)));
  }

  @Test
  public void parses_simple_list_of_order_by() {
    JSONObject orderBy = new JSONObject(Map.of("order_by", "col, col2"));

    List<OrderBy> clauses = TargetMapper.parseOrderBy(orderBy);

    assertThat(clauses).isEqualTo(List.of(new OrderBy("col", null), new OrderBy("col2", null)));
  }

  @Test
  public void parses_order_by_with_direction() {
    JSONObject orderBy = new JSONObject(Map.of("order_by", "col DESC"));

    List<OrderBy> clauses = TargetMapper.parseOrderBy(orderBy);

    assertThat(clauses).isEqualTo(List.of(new OrderBy("col", Order.DESC)));
  }

  @Test
  public void parses_order_by_list_with_direction() {
    JSONObject orderBy = new JSONObject(Map.of("order_by", "col DESC, col2 ASC"));

    List<OrderBy> clauses = TargetMapper.parseOrderBy(orderBy);

    assertThat(clauses)
        .isEqualTo(List.of(new OrderBy("col", Order.DESC), new OrderBy("col2", Order.ASC)));
  }

  @Test
  public void parses_single_dependency_of_node_target() {
    var nodeTarget =
        new JSONObject(
            Map.of(
                "node",
                Map.of(
                    "name",
                    "target2",
                    "source",
                    "a-source",
                    "mode",
                    "append",
                    "execute_after",
                    "node",
                    "execute_after_name",
                    "target1",
                    "mappings",
                    Map.of("label", "\"Placeholder2\"", "properties", Map.of("key", "a-key")))));
    var jsonTargets = new JSONArray(List.of(nodeTarget));
    var index = new JobSpecIndex();
    TargetMapper.index(jsonTargets, index);

    var targets = TargetMapper.parse(jsonTargets, new OptionsParams(), index, false);

    assertThat(targets.getNodes()).hasSize(1);
    var target = targets.getNodes().iterator().next();
    assertThat(target.getDependencies()).isEqualTo(List.of("target1"));
  }

  @Test
  public void resolves_dependencies_of_node_target() {
    var edgeTarget1 =
        new JSONObject(
            Map.of(
                "edge",
                Map.of(
                    "name", "edge-target-1",
                    "mode", "append",
                    "mappings", Map.of())));
    var edgeTarget2 =
        new JSONObject(
            Map.of(
                "edge",
                Map.of(
                    "name", "edge-target-2",
                    "mode", "append",
                    "mappings", Map.of())));
    var nodeTarget =
        new JSONObject(
            Map.of(
                "node",
                Map.of(
                    "name",
                    "node-target",
                    "source",
                    "a-source",
                    "mode",
                    "append",
                    "execute_after",
                    "edges",
                    "mappings",
                    Map.of("label", "\"Placeholder2\"", "properties", Map.of("key", "a-key")))));
    var jsonTargets = new JSONArray(List.of(edgeTarget1, nodeTarget, edgeTarget2));
    var index = new JobSpecIndex();
    TargetMapper.index(jsonTargets, index);

    var targets = TargetMapper.parse(jsonTargets, new OptionsParams(), index, false);

    assertThat(targets.getNodes()).hasSize(1);
    var target = targets.getNodes().iterator().next();
    assertThat(target.getDependencies())
        .containsExactlyElementsIn(List.of("edge-target-1", "edge-target-2"));
  }

  @Test
  public void resolves_dependencies_of_edge_target() {
    var edgeTarget =
        new JSONObject(
            Map.of(
                "edge",
                Map.of(
                    "name", "edge-target",
                    "mode", "append",
                    "execute_after", "nodes",
                    "mappings", Map.of())));
    var nodeTarget1 =
        new JSONObject(
            Map.of(
                "node",
                Map.of(
                    "name",
                    "node-target-1",
                    "source",
                    "a-source",
                    "mode",
                    "append",
                    "mappings",
                    Map.of("label", "\"Placeholder1\"", "properties", Map.of("key", "a-key")))));
    var nodeTarget2 =
        new JSONObject(
            Map.of(
                "node",
                Map.of(
                    "name",
                    "node-target-2",
                    "source",
                    "a-source",
                    "mode",
                    "append",
                    "mappings",
                    Map.of("label", "\"Placeholder2\"", "properties", Map.of("key", "a-key")))));
    var jsonTargets = new JSONArray(List.of(edgeTarget, nodeTarget1, nodeTarget2));
    var index = new JobSpecIndex();
    TargetMapper.index(jsonTargets, index);

    var targets = TargetMapper.parse(jsonTargets, new OptionsParams(), index, false);

    assertThat(targets.getRelationships()).hasSize(1);
    var target = targets.getRelationships().iterator().next();
    assertThat(target.getDependencies())
        .containsExactlyElementsIn(List.of("node-target-1", "node-target-2"));
  }

  @Test
  public void resolves_dependencies_of_custom_query() {
    var queryTarget =
        new JSONObject(
            Map.of(
                "custom_query",
                Map.of(
                    "name", "query-target",
                    "execute_after", "nodes",
                    "query", "RETURN 42")));
    var nodeTarget1 =
        new JSONObject(
            Map.of(
                "node",
                Map.of(
                    "name",
                    "node-target-1",
                    "source",
                    "a-source",
                    "mode",
                    "append",
                    "mappings",
                    Map.of("label", "\"Placeholder1\"", "properties", Map.of("key", "a-key")))));
    var nodeTarget2 =
        new JSONObject(
            Map.of(
                "node",
                Map.of(
                    "name",
                    "node-target-2",
                    "source",
                    "a-source",
                    "mode",
                    "append",
                    "mappings",
                    Map.of("label", "\"Placeholder2\"", "properties", Map.of("key", "a-key")))));
    var jsonTargets = new JSONArray(List.of(queryTarget, nodeTarget1, nodeTarget2));
    var index = new JobSpecIndex();
    TargetMapper.index(jsonTargets, index);

    var targets = TargetMapper.parse(jsonTargets, new OptionsParams(), index, false);

    assertThat(targets.getCustomQueries()).hasSize(1);
    var target = targets.getCustomQueries().iterator().next();
    assertThat(target.getDependencies())
        .containsExactlyElementsIn(List.of("node-target-1", "node-target-2"));
  }

  private static JSONObject jsonTargetOfType(String type) {
    var target = new JSONObject();
    target.put("name", UUID.randomUUID().toString());
    target.put("mappings", new JSONObject());
    var result = new JSONObject();
    result.put(type, target);
    return result;
  }
}
