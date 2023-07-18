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
import com.google.cloud.teleport.v2.neo4j.model.job.JobSpec;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import com.google.cloud.teleport.v2.neo4j.utils.ModelUtils;
import java.util.List;
import org.junit.Test;

/** Unit test functions in the cypher generator. */
public class CypherGeneratorTest {

  private static final String SPEC_PATH = "src/test/resources/testing-specs/cypher-generator-test";

  @Test
  public void testFixIdentifierFirstCharAndSpaces() {
    String testExpression = "1Customer Id";
    String testExpressionValidated = ModelUtils.makeValidNeo4jIdentifier(testExpression);
    assertThat(testExpressionValidated).isEqualTo("_1Customer_Id");
  }

  @Test
  public void specifiesKeysInRelationshipMergePattern() {
    JobSpec jobSpec =
        JobSpecMapper.fromUri(SPEC_PATH + "/single-target-relation-import-with-keys-spec.json");
    Target relationshipTarget = jobSpec.getTargets().iterator().next();
    String statement = CypherGenerator.getUnwindCreateCypher(relationshipTarget);

    assertThat(statement)
        .isEqualTo(
            "UNWIND $rows AS row  "
                + "MATCH (source:Source {id: row.source}) "
                + "MATCH (target:Target {id: row.target}) "
                + "MERGE (source)-[rel:LINKS {rel_id1: row.rel_id_1,rel_id2: row.rel_id_2}]->(target) "
                + "SET rel += {ts: row.timestamp}");
  }

  @Test
  public void specifiesOnlyTypeInKeylessRelationshipMergePattern() {
    JobSpec jobSpec =
        JobSpecMapper.fromUri(SPEC_PATH + "/single-target-relation-import-without-keys-spec.json");
    Target relationshipTarget = jobSpec.getTargets().iterator().next();
    String statement = CypherGenerator.getUnwindCreateCypher(relationshipTarget);

    assertThat(statement)
        .isEqualTo(
            "UNWIND $rows AS row  "
                + "MATCH (source:Source {id: row.source}) "
                + "MATCH (target:Target {id: row.target}) "
                + "MERGE (source)-[rel:LINKS]->(target) "
                + "SET rel += {ts: row.timestamp}");
  }

  @Test
  public void mergesEdgesAsWellAsTheirStartAndEndNodes() {
    JobSpec jobSpec = JobSpecMapper.fromUri(SPEC_PATH + "/single-pass-import.json");
    Target relationshipTarget = jobSpec.getTargets().iterator().next();

    String statement = CypherGenerator.getUnwindCreateCypher(relationshipTarget);

    assertThat(statement)
        .isEqualTo(
            "UNWIND $rows AS row  "
                + "MERGE (source:Source {src_id: row.source}) "
                + "MERGE (target:Target {tgt_id: row.target}) "
                + "MERGE (source)-[rel:LINKS]->(target) SET rel += {ts: row.timestamp}");
  }

  @Test
  public void generatesNodeKeyConstraintsWhenMergingEdgeNodes() {
    JobSpec jobSpec = JobSpecMapper.fromUri(SPEC_PATH + "/single-pass-import.json");
    Target relationshipTarget = jobSpec.getTargets().iterator().next();

    List<String> statements =
        CypherGenerator.getEdgeNodeConstraintsCypherStatements(relationshipTarget);

    assertThat(statements)
        .isEqualTo(
            List.of(
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Source) REQUIRE n.src_id IS NODE KEY",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Target) REQUIRE n.tgt_id IS NODE KEY"));
  }

  @Test
  public void generatesMultiLabelNodeKeyConstraintsWhenMergingEdgeNodes() {
    JobSpec jobSpec =
        JobSpecMapper.fromUri(SPEC_PATH + "/multi-label-single-pass-import.verbose.json");
    Target relationshipTarget = jobSpec.getTargets().iterator().next();

    List<String> statements =
        CypherGenerator.getEdgeNodeConstraintsCypherStatements(relationshipTarget);

    assertThat(statements)
        .isEqualTo(
            List.of(
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Source1) REQUIRE n.src_id IS NODE KEY",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Source2) REQUIRE n.src_id IS NODE KEY",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Target1) REQUIRE n.tgt_id IS NODE KEY",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Target2) REQUIRE n.tgt_id IS NODE KEY"));
  }

  @Test
  public void generatesMultiDistinctKeysNodeKeyConstraintsWhenMergingEdgeNodes() {
    JobSpec jobSpec =
        JobSpecMapper.fromUri(SPEC_PATH + "/multi-distinct-keys-single-pass-import.verbose.json");
    Target relationshipTarget = jobSpec.getTargets().iterator().next();

    List<String> statements =
        CypherGenerator.getEdgeNodeConstraintsCypherStatements(relationshipTarget);

    assertThat(statements)
        .isEqualTo(
            List.of(
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Source) REQUIRE n.src_id1 IS NODE KEY",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Source) REQUIRE n.src_id2 IS NODE KEY",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Target) REQUIRE n.tgt_id1 IS NODE KEY",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Target) REQUIRE n.tgt_id2 IS NODE KEY"));
  }
}
