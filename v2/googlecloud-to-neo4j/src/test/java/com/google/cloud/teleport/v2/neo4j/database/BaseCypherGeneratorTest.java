/*
 * Copyright (C) 2025 Google LLC
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
import java.util.Set;
import java.util.stream.Collectors;
import org.neo4j.importer.v1.ImportSpecification;
import org.neo4j.importer.v1.targets.EntityTarget;
import org.neo4j.importer.v1.targets.TargetType;

public abstract sealed class BaseCypherGeneratorTest permits CypherGeneratorTest {
  protected static final String SPEC_PATH =
      "src/test/resources/testing-specs/cypher-generator-test/";
  protected static final String MULTI_DISTINCT_KEYS_SINGLE_PASS =
      "multi-distinct-keys-single-pass-import.json";
  protected static final String MULTI_LABEL_SINGLE_PASS = "multi-label-single-pass-import.json";
  protected static final String SINGLE_TARGET_CREATE_RELS_MERGE_NODES =
      "single-target-relation-import-create-rels-merge-nodes.json";
  protected static final String SINGLE_TARGET_MERGE_ALL =
      "single-target-relation-import-merge-all.json";
  protected static final String SINGLE_TARGET_WITH_KEYS =
      "single-target-relation-import-with-keys-spec.json";
  protected static final String SINGLE_TARGET_WITHOUT_KEYS =
      "single-target-relation-import-without-keys-spec.json";

  protected static ImportSpecification importSpecificationOf(String specFile) {
    return JobSpecMapper.parse(SPEC_PATH + specFile, new OptionsParams());
  }

  protected void assertImportStatementOf(ImportSpecification spec, String expectedCypher) {
    final Neo4jCapabilities preCypher25 = new Neo4jCapabilities("2025.05", "community");
    var relationshipTarget = spec.getTargets().getRelationships().iterator().next();
    var actualCypher = CypherGenerator.getImportStatement(spec, relationshipTarget, preCypher25);
    assertThat(actualCypher).isEqualTo(expectedCypher);
  }

  protected void assertCypherPrefixOf(ImportSpecification spec, String expectedCypherPrefix) {
    final Neo4jCapabilities postCypher25 = new Neo4jCapabilities("2025.06", "community");
    var relationshipTarget = spec.getTargets().getRelationships().iterator().next();
    var actualCypher = CypherGenerator.getImportStatement(spec, relationshipTarget, postCypher25);
    assertThat(actualCypher).startsWith(expectedCypherPrefix);
  }

  protected void assertSchemaStatements(
      ImportSpecification spec, Neo4jCapabilities capabilities, Set<String> expectedStatements) {
    var statements =
        spec.getTargets().getAll().stream()
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

  protected Neo4jCapabilities capabilitiesFor(String neo4jVersion, String neo4jEdition) {
    return new Neo4jCapabilities(neo4jVersion, neo4jEdition);
  }
}
