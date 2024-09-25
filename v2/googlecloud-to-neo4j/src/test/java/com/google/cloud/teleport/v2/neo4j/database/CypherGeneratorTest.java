/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.database;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.neo4j.model.helpers.JobSpecMapper;
import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;
import org.neo4j.importer.v1.ImportSpecification;
import org.neo4j.importer.v1.targets.EntityTarget;
import org.neo4j.importer.v1.targets.NodeExistenceConstraint;
import org.neo4j.importer.v1.targets.NodeFullTextIndex;
import org.neo4j.importer.v1.targets.NodeKeyConstraint;
import org.neo4j.importer.v1.targets.NodeMatchMode;
import org.neo4j.importer.v1.targets.NodePointIndex;
import org.neo4j.importer.v1.targets.NodeRangeIndex;
import org.neo4j.importer.v1.targets.NodeSchema;
import org.neo4j.importer.v1.targets.NodeTarget;
import org.neo4j.importer.v1.targets.NodeTextIndex;
import org.neo4j.importer.v1.targets.NodeTypeConstraint;
import org.neo4j.importer.v1.targets.NodeUniqueConstraint;
import org.neo4j.importer.v1.targets.NodeVectorIndex;
import org.neo4j.importer.v1.targets.PropertyMapping;
import org.neo4j.importer.v1.targets.PropertyType;
import org.neo4j.importer.v1.targets.RelationshipExistenceConstraint;
import org.neo4j.importer.v1.targets.RelationshipFullTextIndex;
import org.neo4j.importer.v1.targets.RelationshipKeyConstraint;
import org.neo4j.importer.v1.targets.RelationshipPointIndex;
import org.neo4j.importer.v1.targets.RelationshipRangeIndex;
import org.neo4j.importer.v1.targets.RelationshipSchema;
import org.neo4j.importer.v1.targets.RelationshipTarget;
import org.neo4j.importer.v1.targets.RelationshipTextIndex;
import org.neo4j.importer.v1.targets.RelationshipTypeConstraint;
import org.neo4j.importer.v1.targets.RelationshipUniqueConstraint;
import org.neo4j.importer.v1.targets.RelationshipVectorIndex;
import org.neo4j.importer.v1.targets.TargetType;
import org.neo4j.importer.v1.targets.Targets;
import org.neo4j.importer.v1.targets.WriteMode;

public class CypherGeneratorTest {

  private static final String SPEC_PATH = "src/test/resources/testing-specs/cypher-generator-test";

  @Test
  public void specifies_keys_in_relationship_merge_pattern() {
    var importSpecification =
        JobSpecMapper.parse(
            SPEC_PATH + "/single-target-relation-import-with-keys-spec.json", new OptionsParams());
    RelationshipTarget relationshipTarget =
        importSpecification.getTargets().getRelationships().iterator().next();
    String statement = CypherGenerator.getImportStatement(importSpecification, relationshipTarget);

    assertThat(statement)
        .isEqualTo(
            "UNWIND $rows AS row "
                + "MATCH (start:`Source` {`id`: row.`source`}) "
                + "MATCH (end:`Target` {`id`: row.`target`}) "
                + "MERGE (start)-[r:`LINKS` {`id1`: row.`rel_id_1`, `id2`: row.`rel_id_2`}]->(end) "
                + "SET r.`ts` = row.`timestamp`");
  }

  @Test
  public void specifies_only_type_in_keyless_relationship_merge_pattern() {
    var importSpecification =
        JobSpecMapper.parse(
            SPEC_PATH + "/single-target-relation-import-without-keys-spec.json",
            new OptionsParams());
    RelationshipTarget relationshipTarget =
        importSpecification.getTargets().getRelationships().iterator().next();
    String statement = CypherGenerator.getImportStatement(importSpecification, relationshipTarget);

    assertThat(statement)
        .isEqualTo(
            "UNWIND $rows AS row "
                + "MATCH (start:`Source` {`id`: row.`source`}) "
                + "MATCH (end:`Target` {`id`: row.`target`}) "
                + "MERGE (start)-[r:`LINKS`]->(end) "
                + "SET r.`ts` = row.`timestamp`");
  }

  @Test
  public void merges_edges_as_well_as_their_start_and_end_nodes() {
    var importSpecification =
        JobSpecMapper.parse(
            SPEC_PATH + "/single-target-relation-import-merge-all.json", new OptionsParams());
    RelationshipTarget relationshipTarget =
        importSpecification.getTargets().getRelationships().iterator().next();

    String statement = CypherGenerator.getImportStatement(importSpecification, relationshipTarget);

    assertThat(statement)
        .isEqualTo(
            "UNWIND $rows AS row "
                + "MERGE (start:`Source` {`src_id`: row.`source`}) "
                + "MERGE (end:`Target` {`tgt_id`: row.`target`}) "
                + "MERGE (start)-[r:`LINKS`]->(end) SET r.`ts` = row.`timestamp`");
  }

  @Test
  public void creates_edges_and_merges_their_start_and_end_nodes() {
    var importSpecification =
        JobSpecMapper.parse(
            SPEC_PATH + "/single-target-relation-import-create-rels-merge-nodes.json",
            new OptionsParams());
    RelationshipTarget relationshipTarget =
        importSpecification.getTargets().getRelationships().iterator().next();

    String statement = CypherGenerator.getImportStatement(importSpecification, relationshipTarget);

    assertThat(statement)
        .isEqualTo(
            "UNWIND $rows AS row "
                + "MERGE (start:`Source` {`src_id`: row.`source`}) "
                + "MERGE (end:`Target` {`tgt_id`: row.`target`}) "
                + "CREATE (start)-[r:`LINKS`]->(end) SET r.`ts` = row.`timestamp`");
  }

  @Test
  public void does_not_generate_constraints_for_edge_without_schema() {
    var importSpecification =
        JobSpecMapper.parse(
            SPEC_PATH + "/single-target-relation-import-merge-all.json", new OptionsParams());
    RelationshipTarget relationshipTarget =
        importSpecification.getTargets().getRelationships().iterator().next();

    Set<String> statements =
        CypherGenerator.getSchemaStatements(
            relationshipTarget, capabilitiesFor("5.20", "enterprise"));

    assertThat(statements).isEmpty();
  }

  @Test
  public void generates_relationship_key_statement() {
    var relationship =
        new RelationshipTarget(
            true,
            "self-linking-nodes",
            "a-source",
            null,
            "SELF_LINKS_TO",
            WriteMode.MERGE,
            NodeMatchMode.MERGE,
            null,
            "node-target",
            "node-target",
            List.of(new PropertyMapping("source_field", "targetRelProperty", null)),
            new RelationshipSchema(
                null,
                List.of(
                    new RelationshipKeyConstraint("rel-key", List.of("targetRelProperty"), null)),
                null,
                null,
                null,
                null,
                null,
                null,
                null));

    Set<String> schemaStatements =
        CypherGenerator.getSchemaStatements(relationship, capabilitiesFor("5.20", "enterprise"));

    assertThat(schemaStatements)
        .isEqualTo(
            Set.of(
                "CREATE CONSTRAINT `rel-key` IF NOT EXISTS FOR ()-[r:`SELF_LINKS_TO`]-() REQUIRE (r.`targetRelProperty`) IS RELATIONSHIP KEY"));
  }

  @Test
  public void matches_nodes_of_relationship_to_merge() {
    var startNode =
        new NodeTarget(
            true,
            "start-node-target",
            "a-source",
            null,
            WriteMode.MERGE,
            null,
            List.of("StartNode"),
            List.of(new PropertyMapping("source_node_field", "targetNodeProperty", null)),
            new NodeSchema(
                null,
                List.of(
                    new NodeKeyConstraint(
                        "node-key", "StartNode", List.of("targetNodeProperty"), null)),
                null,
                null,
                null,
                null,
                null,
                null,
                null));
    var endNode =
        new NodeTarget(
            true,
            "end-node-target",
            "a-source",
            null,
            WriteMode.MERGE,
            null,
            List.of("EndNode"),
            List.of(new PropertyMapping("source_node_field", "targetNodeProperty", null)),
            new NodeSchema(
                null,
                List.of(
                    new NodeKeyConstraint(
                        "node-key", "EndNode", List.of("targetNodeProperty"), null)),
                null,
                null,
                null,
                null,
                null,
                null,
                null));
    var relationship =
        new RelationshipTarget(
            true,
            "matches-rel-nodes",
            "a-source",
            null,
            "LINKS_TO",
            WriteMode.MERGE,
            NodeMatchMode.MATCH,
            null,
            "start-node-target",
            "end-node-target",
            List.of(new PropertyMapping("source_field", "targetRelProperty", null)),
            new RelationshipSchema(
                null,
                List.of(
                    new RelationshipKeyConstraint("rel-key", List.of("targetRelProperty"), null)),
                null,
                null,
                null,
                null,
                null,
                null,
                null));
    var importSpecification =
        new ImportSpecification(
            "1.0",
            null,
            null,
            new Targets(List.of(startNode, endNode), List.of(relationship), null),
            null);

    var importStatement = CypherGenerator.getImportStatement(importSpecification, relationship);

    assertThat(importStatement)
        .isEqualTo(
            "UNWIND $rows AS row "
                + "MATCH (start:`StartNode` {`targetNodeProperty`: row.`source_node_field`}) "
                + "MATCH (end:`EndNode` {`targetNodeProperty`: row.`source_node_field`}) "
                + "MERGE (start)-[r:`LINKS_TO` {`targetRelProperty`: row.`source_field`}]->(end)");
  }

  @Test
  public void generates_schema_statements_for_node_target() {
    var target =
        new NodeTarget(
            true,
            "a-target",
            "a-source",
            null,
            WriteMode.MERGE,
            null,
            List.of("Placeholder"),
            List.of(
                new PropertyMapping("prop1", "prop1", PropertyType.BOOLEAN),
                new PropertyMapping("prop2", "prop2", null),
                new PropertyMapping("prop3", "prop3", null),
                new PropertyMapping("prop4", "prop4", null),
                new PropertyMapping("prop5", "prop5", null),
                new PropertyMapping("prop6", "prop6", null),
                new PropertyMapping("prop7", "prop7", null),
                new PropertyMapping("prop8", "prop8", null),
                new PropertyMapping("prop9", "prop9", null)),
            new NodeSchema(
                List.of(new NodeTypeConstraint("type-constraint-1", "Placeholder", "prop1")),
                List.of(
                    new NodeKeyConstraint(
                        "key-constraint-1",
                        "Placeholder",
                        List.of("prop2"),
                        Map.of("indexProvider", "range-1.0"))),
                List.of(
                    new NodeUniqueConstraint(
                        "unique-constraint-1", "Placeholder", List.of("prop3"), null)),
                List.of(
                    new NodeExistenceConstraint("existence-constraint-1", "Placeholder", "prop4")),
                List.of(new NodeRangeIndex("range-index-1", "Placeholder", List.of("prop5"))),
                List.of(
                    new NodeTextIndex(
                        "text-index-1",
                        "Placeholder",
                        "prop6",
                        Map.of("indexProvider", "text-2.0"))),
                List.of(
                    new NodePointIndex(
                        "point-index-1",
                        "Placeholder",
                        "prop7",
                        Map.of(
                            "indexConfig",
                            Map.of("spatial.cartesian.min", List.of(-100.0, 100.0))))),
                List.of(
                    new NodeFullTextIndex(
                        "full-text-index-1",
                        List.of("Placeholder"),
                        List.of("prop8"),
                        Map.of("indexConfig", Map.of("fulltext.analyzer", "english")))),
                List.of(
                    new NodeVectorIndex(
                        "vector-index-1",
                        "Placeholder",
                        "prop9",
                        Map.of("indexConfig", Map.of("vector.dimensions", 1536))))));

    var statements =
        CypherGenerator.getSchemaStatements(target, capabilitiesFor("5.20", "enterprise"));

    assertThat(statements)
        .isEqualTo(
            Set.of(
                "CREATE CONSTRAINT `type-constraint-1` IF NOT EXISTS FOR (n:`Placeholder`) REQUIRE n.`prop1` IS :: BOOLEAN",
                "CREATE CONSTRAINT `key-constraint-1` IF NOT EXISTS FOR (n:`Placeholder`) REQUIRE (n.`prop2`) IS NODE KEY OPTIONS {`indexProvider`: 'range-1.0'}",
                "CREATE CONSTRAINT `unique-constraint-1` IF NOT EXISTS FOR (n:`Placeholder`) REQUIRE (n.`prop3`) IS UNIQUE",
                "CREATE CONSTRAINT `existence-constraint-1` IF NOT EXISTS FOR (n:`Placeholder`) REQUIRE n.`prop4` IS NOT NULL",
                "CREATE INDEX `range-index-1` IF NOT EXISTS FOR (n:`Placeholder`) ON (n.`prop5`)",
                "CREATE TEXT INDEX `text-index-1` IF NOT EXISTS FOR (n:`Placeholder`) ON (n.`prop6`) OPTIONS {`indexProvider`: 'text-2.0'}",
                "CREATE POINT INDEX `point-index-1` IF NOT EXISTS FOR (n:`Placeholder`) ON (n.`prop7`) OPTIONS {`indexConfig`: {`spatial.cartesian.min`: [-100.0,100.0]}}",
                "CREATE FULLTEXT INDEX `full-text-index-1` IF NOT EXISTS FOR (n:`Placeholder`) ON EACH [n.`prop8`] OPTIONS {`indexConfig`: {`fulltext.analyzer`: 'english'}}",
                "CREATE VECTOR INDEX `vector-index-1` IF NOT EXISTS FOR (n:`Placeholder`) ON (n.`prop9`) OPTIONS {`indexConfig`: {`vector.dimensions`: 1536}}"));
  }

  @Test
  public void generates_schema_statements_for_node_target_against_Neo4j_44_CE() {
    var target =
        new NodeTarget(
            true,
            "a-target",
            "a-source",
            null,
            WriteMode.MERGE,
            null,
            List.of("Placeholder"),
            List.of(
                new PropertyMapping("prop1", "prop1", PropertyType.BOOLEAN),
                new PropertyMapping("prop2", "prop2", null),
                new PropertyMapping("prop3", "prop3", null),
                new PropertyMapping("prop4", "prop4", null),
                new PropertyMapping("prop5", "prop5", null),
                new PropertyMapping("prop6", "prop6", null),
                new PropertyMapping("prop7", "prop7", null),
                new PropertyMapping("prop8", "prop8", null),
                new PropertyMapping("prop9", "prop9", null)),
            new NodeSchema(
                List.of(new NodeTypeConstraint("type-constraint-1", "Placeholder", "prop1")),
                List.of(
                    new NodeKeyConstraint(
                        "key-constraint-1",
                        "Placeholder",
                        List.of("prop2"),
                        Map.of("indexProvider", "range-1.0"))),
                List.of(
                    new NodeUniqueConstraint(
                        "unique-constraint-1", "Placeholder", List.of("prop3"), null)),
                List.of(
                    new NodeExistenceConstraint("existence-constraint-1", "Placeholder", "prop4")),
                List.of(new NodeRangeIndex("range-index-1", "Placeholder", List.of("prop5"))),
                List.of(
                    new NodeTextIndex(
                        "text-index-1",
                        "Placeholder",
                        "prop6",
                        Map.of("indexProvider", "text-2.0"))),
                List.of(
                    new NodePointIndex(
                        "point-index-1",
                        "Placeholder",
                        "prop7",
                        Map.of(
                            "indexConfig",
                            Map.of("spatial.cartesian.min", List.of(-100.0, 100.0))))),
                List.of(
                    new NodeFullTextIndex(
                        "full-text-index-1",
                        List.of("Placeholder"),
                        List.of("prop8"),
                        Map.of("indexConfig", Map.of("fulltext.analyzer", "english")))),
                List.of(
                    new NodeVectorIndex(
                        "vector-index-1",
                        "Placeholder",
                        "prop9",
                        Map.of("indexConfig", Map.of("vector.dimensions", 1536))))));

    var statements =
        CypherGenerator.getSchemaStatements(target, capabilitiesFor("4.4.0", "community"));

    assertThat(statements)
        .isEqualTo(
            Set.of(
                "CREATE INDEX `range-index-1` IF NOT EXISTS FOR (n:`Placeholder`) ON (n.`prop5`)",
                "CREATE TEXT INDEX `text-index-1` IF NOT EXISTS FOR (n:`Placeholder`) ON (n.`prop6`) OPTIONS {`indexProvider`: 'text-2.0'}",
                "CREATE POINT INDEX `point-index-1` IF NOT EXISTS FOR (n:`Placeholder`) ON (n.`prop7`) OPTIONS {`indexConfig`: {`spatial.cartesian.min`: [-100.0,100.0]}}",
                "CREATE FULLTEXT INDEX `full-text-index-1` IF NOT EXISTS FOR (n:`Placeholder`) ON EACH [n.`prop8`] OPTIONS {`indexConfig`: {`fulltext.analyzer`: 'english'}}"));
  }

  @Test
  public void generates_schema_statements_for_relationship_target() {
    var target =
        new RelationshipTarget(
            true,
            "a-target",
            "a-source",
            null,
            "PLACEHOLDER",
            WriteMode.CREATE,
            NodeMatchMode.MERGE,
            null,
            "a-start-node-target",
            "an-end-node-target",
            List.of(
                new PropertyMapping("prop1", "prop1", PropertyType.LOCAL_DATETIME_ARRAY),
                new PropertyMapping("prop2", "prop2", null),
                new PropertyMapping("prop3", "prop3", null),
                new PropertyMapping("prop4", "prop4", null),
                new PropertyMapping("prop5", "prop5", null),
                new PropertyMapping("prop6", "prop6", null),
                new PropertyMapping("prop7", "prop7", null),
                new PropertyMapping("prop8", "prop8", null),
                new PropertyMapping("prop9", "prop9", null)),
            new RelationshipSchema(
                List.of(new RelationshipTypeConstraint("type-constraint-1", "prop1")),
                List.of(
                    new RelationshipKeyConstraint(
                        "key-constraint-1",
                        List.of("prop2"),
                        Map.of("indexProvider", "range-1.0"))),
                List.of(
                    new RelationshipUniqueConstraint(
                        "unique-constraint-1", List.of("prop3"), null)),
                List.of(new RelationshipExistenceConstraint("existence-constraint-1", "prop4")),
                List.of(new RelationshipRangeIndex("range-index-1", List.of("prop5"))),
                List.of(
                    new RelationshipTextIndex(
                        "text-index-1", "prop6", Map.of("indexProvider", "text-2.0"))),
                List.of(
                    new RelationshipPointIndex(
                        "point-index-1",
                        "prop7",
                        Map.of(
                            "indexConfig",
                            Map.of("spatial.cartesian.min", List.of(-100.0, 100.0))))),
                List.of(
                    new RelationshipFullTextIndex(
                        "full-text-index-1",
                        List.of("prop8"),
                        Map.of("indexConfig", Map.of("fulltext.analyzer", "english")))),
                List.of(
                    new RelationshipVectorIndex(
                        "vector-index-1",
                        "prop9",
                        Map.of("indexConfig", Map.of("vector.dimensions", 1536))))));

    var statements =
        CypherGenerator.getSchemaStatements(target, capabilitiesFor("5.20", "enterprise"));

    assertThat(statements)
        .isEqualTo(
            Set.of(
                "CREATE CONSTRAINT `type-constraint-1` IF NOT EXISTS FOR ()-[r:`PLACEHOLDER`]-() REQUIRE r.`prop1` IS :: LIST<LOCAL DATETIME NOT NULL>",
                "CREATE CONSTRAINT `key-constraint-1` IF NOT EXISTS FOR ()-[r:`PLACEHOLDER`]-() REQUIRE (r.`prop2`) IS RELATIONSHIP KEY OPTIONS {`indexProvider`: 'range-1.0'}",
                "CREATE CONSTRAINT `unique-constraint-1` IF NOT EXISTS FOR ()-[r:`PLACEHOLDER`]-() REQUIRE (r.`prop3`) IS UNIQUE",
                "CREATE CONSTRAINT `existence-constraint-1` IF NOT EXISTS FOR ()-[r:`PLACEHOLDER`]-() REQUIRE r.`prop4` IS NOT NULL",
                "CREATE INDEX `range-index-1` IF NOT EXISTS FOR ()-[r:`PLACEHOLDER`]-() ON (r.`prop5`)",
                "CREATE TEXT INDEX `text-index-1` IF NOT EXISTS FOR ()-[r:`PLACEHOLDER`]-() ON (r.`prop6`) OPTIONS {`indexProvider`: 'text-2.0'}",
                "CREATE POINT INDEX `point-index-1` IF NOT EXISTS FOR ()-[r:`PLACEHOLDER`]-() ON (r.`prop7`) OPTIONS {`indexConfig`: {`spatial.cartesian.min`: [-100.0,100.0]}}",
                "CREATE FULLTEXT INDEX `full-text-index-1` IF NOT EXISTS FOR ()-[r:`PLACEHOLDER`]-() ON EACH [r.`prop8`] OPTIONS {`indexConfig`: {`fulltext.analyzer`: 'english'}}",
                "CREATE VECTOR INDEX `vector-index-1` IF NOT EXISTS FOR ()-[r:`PLACEHOLDER`]-() ON (r.`prop9`) OPTIONS {`indexConfig`: {`vector.dimensions`: 1536}}"));
  }

  @Test
  public void generates_schema_statements_for_relationship_target_against_Neo4j_44_CE() {
    var target =
        new RelationshipTarget(
            true,
            "a-target",
            "a-source",
            null,
            "PLACEHOLDER",
            WriteMode.CREATE,
            NodeMatchMode.MERGE,
            null,
            "a-start-node-target",
            "an-end-node-target",
            List.of(
                new PropertyMapping("prop1", "prop1", PropertyType.LOCAL_DATETIME_ARRAY),
                new PropertyMapping("prop2", "prop2", null),
                new PropertyMapping("prop3", "prop3", null),
                new PropertyMapping("prop4", "prop4", null),
                new PropertyMapping("prop5", "prop5", null),
                new PropertyMapping("prop6", "prop6", null),
                new PropertyMapping("prop7", "prop7", null),
                new PropertyMapping("prop8", "prop8", null),
                new PropertyMapping("prop9", "prop9", null)),
            new RelationshipSchema(
                List.of(new RelationshipTypeConstraint("type-constraint-1", "prop1")),
                List.of(
                    new RelationshipKeyConstraint(
                        "key-constraint-1",
                        List.of("prop2"),
                        Map.of("indexProvider", "range-1.0"))),
                List.of(
                    new RelationshipUniqueConstraint(
                        "unique-constraint-1", List.of("prop3"), null)),
                List.of(new RelationshipExistenceConstraint("existence-constraint-1", "prop4")),
                List.of(new RelationshipRangeIndex("range-index-1", List.of("prop5"))),
                List.of(
                    new RelationshipTextIndex(
                        "text-index-1", "prop6", Map.of("indexProvider", "text-2.0"))),
                List.of(
                    new RelationshipPointIndex(
                        "point-index-1",
                        "prop7",
                        Map.of(
                            "indexConfig",
                            Map.of("spatial.cartesian.min", List.of(-100.0, 100.0))))),
                List.of(
                    new RelationshipFullTextIndex(
                        "full-text-index-1",
                        List.of("prop8"),
                        Map.of("indexConfig", Map.of("fulltext.analyzer", "english")))),
                List.of(
                    new RelationshipVectorIndex(
                        "vector-index-1",
                        "prop9",
                        Map.of("indexConfig", Map.of("vector.dimensions", 1536))))));

    var statements =
        CypherGenerator.getSchemaStatements(target, capabilitiesFor("4.4.0", "community"));

    assertThat(statements)
        .isEqualTo(
            Set.of(
                "CREATE INDEX `range-index-1` IF NOT EXISTS FOR ()-[r:`PLACEHOLDER`]-() ON (r.`prop5`)",
                "CREATE TEXT INDEX `text-index-1` IF NOT EXISTS FOR ()-[r:`PLACEHOLDER`]-() ON (r.`prop6`) OPTIONS {`indexProvider`: 'text-2.0'}",
                "CREATE POINT INDEX `point-index-1` IF NOT EXISTS FOR ()-[r:`PLACEHOLDER`]-() ON (r.`prop7`) OPTIONS {`indexConfig`: {`spatial.cartesian.min`: [-100.0,100.0]}}",
                "CREATE FULLTEXT INDEX `full-text-index-1` IF NOT EXISTS FOR ()-[r:`PLACEHOLDER`]-() ON EACH [r.`prop8`] OPTIONS {`indexConfig`: {`fulltext.analyzer`: 'english'}}"));
  }

  @Test
  public void does_not_generate_schema_statements_for_relationship_target_without_schema() {
    var target =
        new RelationshipTarget(
            true,
            "a-target",
            "a-source",
            null,
            "TYPE",
            WriteMode.CREATE,
            NodeMatchMode.MERGE,
            null,
            "a-start-node-target",
            "an-end-node-target",
            null,
            null);

    var statements =
        CypherGenerator.getSchemaStatements(target, capabilitiesFor("5.20", "enterprise"));

    assertThat(statements).isEmpty();
  }

  @Test
  public void
      generates_correct_schema_statement_for_multi_label_node_key_constraints_when_merging_edge_nodes_on_neo4j_5_enterprise() {
    assertSchemaStatements(
        "/multi-label-single-pass-import.json",
        capabilitiesFor("5.1.0", "enterprise"),
        Set.of(
            "CREATE CONSTRAINT `Edge import-source-Source1-node-single-key-for-src_id` IF NOT EXISTS FOR (n:`Source1`) REQUIRE (n.`src_id`) IS NODE KEY",
            "CREATE CONSTRAINT `Edge import-source-Source2-node-single-key-for-src_id` IF NOT EXISTS FOR (n:`Source2`) REQUIRE (n.`src_id`) IS NODE KEY",
            "CREATE CONSTRAINT `Edge import-target-Target1-node-single-key-for-tgt_id` IF NOT EXISTS FOR (n:`Target1`) REQUIRE (n.`tgt_id`) IS NODE KEY",
            "CREATE CONSTRAINT `Edge import-target-Target2-node-single-key-for-tgt_id` IF NOT EXISTS FOR (n:`Target2`) REQUIRE (n.`tgt_id`) IS NODE KEY"));
  }

  @Test
  public void
      generates_correct_schema_statement_for_multi_label_node_key_constraints_when_merging_edge_nodes_on_neo4j_5_aura() {
    assertSchemaStatements(
        "/multi-label-single-pass-import.json",
        capabilitiesFor("5.1-aura", "enterprise"),
        Set.of(
            "CREATE CONSTRAINT `Edge import-source-Source1-node-single-key-for-src_id` IF NOT EXISTS FOR (n:`Source1`) REQUIRE (n.`src_id`) IS NODE KEY",
            "CREATE CONSTRAINT `Edge import-source-Source2-node-single-key-for-src_id` IF NOT EXISTS FOR (n:`Source2`) REQUIRE (n.`src_id`) IS NODE KEY",
            "CREATE CONSTRAINT `Edge import-target-Target1-node-single-key-for-tgt_id` IF NOT EXISTS FOR (n:`Target1`) REQUIRE (n.`tgt_id`) IS NODE KEY",
            "CREATE CONSTRAINT `Edge import-target-Target2-node-single-key-for-tgt_id` IF NOT EXISTS FOR (n:`Target2`) REQUIRE (n.`tgt_id`) IS NODE KEY"));
  }

  @Test
  public void
      generates_correct_schema_statement_for_multi_label_node_key_constraints_when_merging_edge_nodes_on_neo4j_5_community() {
    assertSchemaStatements(
        "/multi-label-single-pass-import.json", capabilitiesFor("5.1.6", "community"), Set.of());
  }

  @Test
  public void
      generates_correct_schema_statement_for_multi_label_node_key_constraints_when_merging_edge_nodes_on_neo4j_44_enterprise() {
    assertSchemaStatements(
        "/multi-label-single-pass-import.json",
        capabilitiesFor("4.4.25", "enterprise"),
        Set.of(
            "CREATE CONSTRAINT `Edge import-source-Source1-node-single-key-for-src_id` IF NOT EXISTS FOR (n:`Source1`) REQUIRE (n.`src_id`) IS NODE KEY",
            "CREATE CONSTRAINT `Edge import-source-Source2-node-single-key-for-src_id` IF NOT EXISTS FOR (n:`Source2`) REQUIRE (n.`src_id`) IS NODE KEY",
            "CREATE CONSTRAINT `Edge import-target-Target1-node-single-key-for-tgt_id` IF NOT EXISTS FOR (n:`Target1`) REQUIRE (n.`tgt_id`) IS NODE KEY",
            "CREATE CONSTRAINT `Edge import-target-Target2-node-single-key-for-tgt_id` IF NOT EXISTS FOR (n:`Target2`) REQUIRE (n.`tgt_id`) IS NODE KEY"));
  }

  @Test
  public void
      generates_correct_schema_statement_for_multi_label_node_key_constraints_when_merging_edge_nodes_on_neo4j_44_aura() {
    assertSchemaStatements(
        "/multi-label-single-pass-import.json",
        capabilitiesFor("4.4-aura", "enterprise"),
        Set.of(
            "CREATE CONSTRAINT `Edge import-source-Source1-node-single-key-for-src_id` IF NOT EXISTS FOR (n:`Source1`) REQUIRE (n.`src_id`) IS NODE KEY",
            "CREATE CONSTRAINT `Edge import-source-Source2-node-single-key-for-src_id` IF NOT EXISTS FOR (n:`Source2`) REQUIRE (n.`src_id`) IS NODE KEY",
            "CREATE CONSTRAINT `Edge import-target-Target1-node-single-key-for-tgt_id` IF NOT EXISTS FOR (n:`Target1`) REQUIRE (n.`tgt_id`) IS NODE KEY",
            "CREATE CONSTRAINT `Edge import-target-Target2-node-single-key-for-tgt_id` IF NOT EXISTS FOR (n:`Target2`) REQUIRE (n.`tgt_id`) IS NODE KEY"));
  }

  @Test
  public void
      generates_correct_schema_statement_for_multi_label_node_key_constraints_when_merging_edge_nodes_on_neo4j_44_community() {
    assertSchemaStatements(
        "/multi-label-single-pass-import.json", capabilitiesFor("4.4.25", "community"), Set.of());
  }

  @Test
  public void
      generates_correct_schema_statement_for_multi_distinct_keys_node_key_constraints_when_merging_edge_nodes_on_neo4j_5_enterprise() {
    assertSchemaStatements(
        "/multi-distinct-keys-single-pass-import.json",
        capabilitiesFor("5.1.0", "enterprise"),
        Set.of(
            "CREATE CONSTRAINT `Edge import-source-Source-node-key-for-src_id1` IF NOT EXISTS FOR (n:`Source`) REQUIRE (n.`src_id1`) IS NODE KEY",
            "CREATE CONSTRAINT `Edge import-source-Source-node-key-for-src_id2` IF NOT EXISTS FOR (n:`Source`) REQUIRE (n.`src_id2`) IS NODE KEY",
            "CREATE CONSTRAINT `Edge import-target-Target-node-key-for-tgt_id1` IF NOT EXISTS FOR (n:`Target`) REQUIRE (n.`tgt_id1`) IS NODE KEY",
            "CREATE CONSTRAINT `Edge import-target-Target-node-key-for-tgt_id2` IF NOT EXISTS FOR (n:`Target`) REQUIRE (n.`tgt_id2`) IS NODE KEY"));
  }

  @Test
  public void
      generates_correct_schema_statement_for_multi_distinct_keys_node_key_constraints_when_merging_edge_nodes_on_neo4j_5_aura() {
    assertSchemaStatements(
        "/multi-distinct-keys-single-pass-import.json",
        capabilitiesFor("5.1-aura", "enterprise"),
        Set.of(
            "CREATE CONSTRAINT `Edge import-source-Source-node-key-for-src_id1` IF NOT EXISTS FOR (n:`Source`) REQUIRE (n.`src_id1`) IS NODE KEY",
            "CREATE CONSTRAINT `Edge import-source-Source-node-key-for-src_id2` IF NOT EXISTS FOR (n:`Source`) REQUIRE (n.`src_id2`) IS NODE KEY",
            "CREATE CONSTRAINT `Edge import-target-Target-node-key-for-tgt_id1` IF NOT EXISTS FOR (n:`Target`) REQUIRE (n.`tgt_id1`) IS NODE KEY",
            "CREATE CONSTRAINT `Edge import-target-Target-node-key-for-tgt_id2` IF NOT EXISTS FOR (n:`Target`) REQUIRE (n.`tgt_id2`) IS NODE KEY"));
  }

  @Test
  public void
      generates_correct_schema_statement_for_multi_distinct_keys_node_key_constraints_when_merging_edge_nodes_on_neo4j_5_community() {
    assertSchemaStatements(
        "/multi-distinct-keys-single-pass-import.json",
        capabilitiesFor("5.1.0", "community"),
        Set.of());
  }

  @Test
  public void
      generates_correct_schema_statement_for_multi_distinct_keys_node_key_constraints_when_merging_edge_nodes_on_neo4j_44_enterprise() {
    assertSchemaStatements(
        "/multi-distinct-keys-single-pass-import.json",
        capabilitiesFor("4.4.25", "enterprise"),
        Set.of(
            "CREATE CONSTRAINT `Edge import-source-Source-node-key-for-src_id1` IF NOT EXISTS FOR (n:`Source`) REQUIRE (n.`src_id1`) IS NODE KEY",
            "CREATE CONSTRAINT `Edge import-source-Source-node-key-for-src_id2` IF NOT EXISTS FOR (n:`Source`) REQUIRE (n.`src_id2`) IS NODE KEY",
            "CREATE CONSTRAINT `Edge import-target-Target-node-key-for-tgt_id1` IF NOT EXISTS FOR (n:`Target`) REQUIRE (n.`tgt_id1`) IS NODE KEY",
            "CREATE CONSTRAINT `Edge import-target-Target-node-key-for-tgt_id2` IF NOT EXISTS FOR (n:`Target`) REQUIRE (n.`tgt_id2`) IS NODE KEY"));
  }

  @Test
  public void
      generates_correct_schema_statement_for_distinct_keys_node_key_constraints_when_merging_edge_nodes_on_neo4j_44_aura() {
    assertSchemaStatements(
        "/multi-distinct-keys-single-pass-import.json",
        capabilitiesFor("4.4-aura", "enterprise"),
        Set.of(
            "CREATE CONSTRAINT `Edge import-source-Source-node-key-for-src_id1` IF NOT EXISTS FOR (n:`Source`) REQUIRE (n.`src_id1`) IS NODE KEY",
            "CREATE CONSTRAINT `Edge import-source-Source-node-key-for-src_id2` IF NOT EXISTS FOR (n:`Source`) REQUIRE (n.`src_id2`) IS NODE KEY",
            "CREATE CONSTRAINT `Edge import-target-Target-node-key-for-tgt_id1` IF NOT EXISTS FOR (n:`Target`) REQUIRE (n.`tgt_id1`) IS NODE KEY",
            "CREATE CONSTRAINT `Edge import-target-Target-node-key-for-tgt_id2` IF NOT EXISTS FOR (n:`Target`) REQUIRE (n.`tgt_id2`) IS NODE KEY"));
  }

  @Test
  public void
      generates_correct_schema_statement_for_multi_distinct_keys_node_key_constraints_when_merging_edge_nodes_on_neo4j_44_community() {
    assertSchemaStatements(
        "/multi-distinct-keys-single-pass-import.json",
        capabilitiesFor("4.4.25", "community"),
        Set.of());
  }

  private static Neo4jCapabilities capabilitiesFor(String neo4jVersion, String neo4jEdition) {
    return new Neo4jCapabilities(neo4jVersion, neo4jEdition);
  }

  private void assertSchemaStatements(
      String path, Neo4jCapabilities capabilities, Set<String> expectedStatements) {
    var jobSpec = JobSpecMapper.parse(SPEC_PATH + path, new OptionsParams());
    var statements =
        jobSpec.getTargets().getAll().stream()
            .filter(
                target ->
                    target.isActive()
                        && (target.getTargetType() == TargetType.NODE
                            || target.getTargetType() == TargetType.RELATIONSHIP))
            .map(target -> (EntityTarget) target)
            .flatMap(target -> CypherGenerator.getSchemaStatements(target, capabilities).stream())
            .collect(Collectors.toSet());

    assertThat(statements).isEqualTo(expectedStatements);
  }
}
