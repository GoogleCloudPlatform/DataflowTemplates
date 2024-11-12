/*
 * Copyright (C) 2024 Google LLC
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
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.importer.v1.targets.RelationshipTarget;

@RunWith(Parameterized.class)
public class NewSpecCypherGeneratorTest {

  private static final String SPEC_PATH =
      "src/test/resources/testing-specs/new-spec-cypher-generator-test";

  @Parameterized.Parameter public String fileFormat;

  @Parameterized.Parameters(name = "{0}")
  public static List<String> testParameters() {
    return Arrays.asList("json", "yaml");
  }

  @Test
  public void merges_nodes_creates_relationship() {
    var importSpecification =
        JobSpecMapper.parse(
            SPEC_PATH + "/create-rels-merge-nodes." + fileFormat, new OptionsParams());

    RelationshipTarget relationshipTarget =
        importSpecification.getTargets().getRelationships().iterator().next();
    String statement = CypherGenerator.getImportStatement(importSpecification, relationshipTarget);

    assertThat(statement)
        .isEqualTo(
            "UNWIND $rows AS row MERGE (start:`LabelA` {`property1`: row.`field_1`}) MERGE (end:`LabelB` {`property1`: row.`field_1`}) CREATE (start)-[r:`TYPE` {`id`: row.`field_1`}]->(end)");
  }
}
