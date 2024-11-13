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
package com.google.cloud.teleport.v2.neo4j.model.helpers;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import java.util.List;
import org.junit.Test;

public class JobSpecMapperTest {

  private static final String SPEC_PATH = "src/test/resources/testing-specs/job-spec-mapper-test";

  @Test
  public void parses_valid_json_legacy_spec() {
    var importSpecification =
        JobSpecMapper.parse(SPEC_PATH + "/valid-json-legacy-spec.json", new OptionsParams());

    var sources = importSpecification.getSources();
    assertThat(sources).hasSize(1);
    assertThat(sources.get(0).getType()).isEqualTo("text");
    assertThat(sources.get(0).getName()).isEqualTo("source_csv");

    var targets = importSpecification.getTargets();

    var nodes = targets.getNodes();
    assertThat(nodes).hasSize(2);

    var sourceNode = nodes.get(0);
    assertThat(sourceNode.getSource()).isEqualTo("source_csv");
    assertThat(sourceNode.getName()).isEqualTo("Source CSV rel file-source");
    assertThat(sourceNode.getWriteMode().name()).isEqualTo("MERGE");
    assertThat(sourceNode.getLabels()).isEqualTo(List.of("Source"));
    assertThat(sourceNode.getProperties()).hasSize(1);
    assertThat(sourceNode.getProperties().get(0).getSourceField()).isEqualTo("source");
    assertThat(sourceNode.getProperties().get(0).getTargetProperty()).isEqualTo("src_id");
    assertThat(sourceNode.getSchema().getKeyConstraints()).hasSize(1);
    assertThat(sourceNode.getSchema().getKeyConstraints().get(0).getName())
        .isEqualTo("Source CSV rel file-source-Source-node-single-key-for-src_id");
    assertThat(sourceNode.getSchema().getKeyConstraints().get(0).getLabel()).isEqualTo("Source");
    assertThat(sourceNode.getSchema().getKeyConstraints().get(0).getProperties()).hasSize(1);
    assertThat(sourceNode.getSchema().getKeyConstraints().get(0).getProperties().get(0))
        .isEqualTo("src_id");

    var targetNode = nodes.get(1);
    assertThat(targetNode.getSource()).isEqualTo("source_csv");
    assertThat(targetNode.getName()).isEqualTo("Source CSV rel file-target");
    assertThat(targetNode.getWriteMode().name()).isEqualTo("MERGE");
    assertThat(targetNode.getLabels()).isEqualTo(List.of("Target"));
    assertThat(targetNode.getProperties()).hasSize(1);
    assertThat(targetNode.getProperties().get(0).getSourceField()).isEqualTo("target");
    assertThat(targetNode.getProperties().get(0).getTargetProperty()).isEqualTo("tgt_id");
    assertThat(targetNode.getSchema().getKeyConstraints()).hasSize(1);
    assertThat(targetNode.getSchema().getKeyConstraints().get(0).getName())
        .isEqualTo("Source CSV rel file-target-Target-node-single-key-for-tgt_id");
    assertThat(targetNode.getSchema().getKeyConstraints().get(0).getLabel()).isEqualTo("Target");
    assertThat(targetNode.getSchema().getKeyConstraints().get(0).getProperties()).hasSize(1);
    assertThat(targetNode.getSchema().getKeyConstraints().get(0).getProperties().get(0))
        .isEqualTo("tgt_id");

    var relationships = targets.getRelationships();
    assertThat(relationships).hasSize(1);

    var relationship = relationships.get(0);
    assertThat(relationship.getName()).isEqualTo("Source CSV rel file");
    assertThat(relationship.getSource()).isEqualTo("source_csv");
    assertThat(relationship.getType()).isEqualTo("LINKS");
    assertThat(relationship.getWriteMode().name()).isEqualTo("CREATE");
    assertThat(relationship.getNodeMatchMode().name()).isEqualTo("MERGE");
    assertThat(relationship.getStartNodeReference()).isEqualTo("Source CSV rel file-source");
    assertThat(relationship.getEndNodeReference()).isEqualTo("Source CSV rel file-target");
    assertThat(relationship.getProperties()).hasSize(1);
    assertThat(relationship.getProperties().get(0).getSourceField()).isEqualTo("timestamp");
    assertThat(relationship.getProperties().get(0).getTargetProperty()).isEqualTo("ts");
  }

  @Test
  public void parses_valid_json_import_spec() {
    var importSpecification =
        JobSpecMapper.parse(SPEC_PATH + "/valid-json-import-spec.json", new OptionsParams());

    var sources = importSpecification.getSources();
    assertThat(sources).hasSize(2);
    assertThat(sources.get(0).getType()).isEqualTo("bigquery");
    assertThat(sources.get(0).getName()).isEqualTo("a-source");
    assertThat(sources.get(1).getType()).isEqualTo("bigquery");
    assertThat(sources.get(1).getName()).isEqualTo("b-source");

    var targets = importSpecification.getTargets();

    var nodes = targets.getNodes();
    assertThat(nodes).hasSize(2);

    var sourceNode = nodes.get(0);
    assertThat(sourceNode.getSource()).isEqualTo("a-source");
    assertThat(sourceNode.getName()).isEqualTo("a-node-target");
    assertThat(sourceNode.getWriteMode().name()).isEqualTo("MERGE");
    assertThat(sourceNode.getLabels()).isEqualTo(List.of("LabelA"));
    assertThat(sourceNode.getProperties()).hasSize(2);
    assertThat(sourceNode.getProperties().get(0).getSourceField()).isEqualTo("field_1");
    assertThat(sourceNode.getProperties().get(0).getTargetProperty()).isEqualTo("property1");
    assertThat(sourceNode.getProperties().get(1).getSourceField()).isEqualTo("field_2");
    assertThat(sourceNode.getProperties().get(1).getTargetProperty()).isEqualTo("property2");
    assertThat(sourceNode.getSchema().getKeyConstraints()).hasSize(1);
    assertThat(sourceNode.getSchema().getKeyConstraints().get(0).getName())
        .isEqualTo("LabelA key constraint");
    assertThat(sourceNode.getSchema().getKeyConstraints().get(0).getLabel()).isEqualTo("LabelA");
    assertThat(sourceNode.getSchema().getKeyConstraints().get(0).getProperties()).hasSize(1);
    assertThat(sourceNode.getSchema().getKeyConstraints().get(0).getProperties().get(0))
        .isEqualTo("property1");

    var targetNode = nodes.get(1);
    assertThat(targetNode.getSource()).isEqualTo("b-source");
    assertThat(targetNode.getName()).isEqualTo("b-node-target");
    assertThat(targetNode.getWriteMode().name()).isEqualTo("MERGE");
    assertThat(targetNode.getLabels()).isEqualTo(List.of("LabelB"));
    assertThat(targetNode.getProperties()).hasSize(2);
    assertThat(targetNode.getProperties().get(0).getSourceField()).isEqualTo("field_1");
    assertThat(targetNode.getProperties().get(0).getTargetProperty()).isEqualTo("property1");
    assertThat(targetNode.getProperties().get(1).getSourceField()).isEqualTo("field_2");
    assertThat(targetNode.getProperties().get(1).getTargetProperty()).isEqualTo("property2");
    assertThat(targetNode.getSchema().getKeyConstraints()).hasSize(1);
    assertThat(targetNode.getSchema().getKeyConstraints().get(0).getName())
        .isEqualTo("LabelB key constraint");
    assertThat(targetNode.getSchema().getKeyConstraints().get(0).getLabel()).isEqualTo("LabelB");
    assertThat(targetNode.getSchema().getKeyConstraints().get(0).getProperties()).hasSize(1);
    assertThat(targetNode.getSchema().getKeyConstraints().get(0).getProperties().get(0))
        .isEqualTo("property1");

    var relationships = targets.getRelationships();
    assertThat(relationships).hasSize(1);

    var relationship = relationships.get(0);
    assertThat(relationship.getName()).isEqualTo("a-target");
    assertThat(relationship.getSource()).isEqualTo("a-source");
    assertThat(relationship.getType()).isEqualTo("TYPE");
    assertThat(relationship.getWriteMode().name()).isEqualTo("CREATE");
    assertThat(relationship.getNodeMatchMode().name()).isEqualTo("MERGE");
    assertThat(relationship.getStartNodeReference()).isEqualTo("a-node-target");
    assertThat(relationship.getEndNodeReference()).isEqualTo("b-node-target");
    assertThat(relationship.getProperties()).hasSize(1);
    assertThat(relationship.getProperties().get(0).getSourceField()).isEqualTo("field_1");
    assertThat(relationship.getProperties().get(0).getTargetProperty()).isEqualTo("id");
    assertThat(relationship.getSchema().getKeyConstraints()).hasSize(1);
    assertThat(relationship.getSchema().getKeyConstraints().get(0).getName())
        .isEqualTo("rel key constraint");
    assertThat(relationship.getSchema().getKeyConstraints().get(0).getProperties()).hasSize(1);
    assertThat(relationship.getSchema().getKeyConstraints().get(0).getProperties().get(0))
        .isEqualTo("id");
  }

  @Test
  public void parses_valid_yaml_import_spec() {
    var importSpecification =
        JobSpecMapper.parse(SPEC_PATH + "/valid-yaml-import-spec.yaml", new OptionsParams());

    var sources = importSpecification.getSources();
    assertThat(sources).hasSize(2);
    assertThat(sources.get(0).getType()).isEqualTo("bigquery");
    assertThat(sources.get(0).getName()).isEqualTo("a-source");
    assertThat(sources.get(1).getType()).isEqualTo("bigquery");
    assertThat(sources.get(1).getName()).isEqualTo("b-source");

    var targets = importSpecification.getTargets();

    var nodes = targets.getNodes();
    assertThat(nodes).hasSize(2);

    var sourceNode = nodes.get(0);
    assertThat(sourceNode.getSource()).isEqualTo("a-source");
    assertThat(sourceNode.getName()).isEqualTo("a-node-target");
    assertThat(sourceNode.getWriteMode().name()).isEqualTo("MERGE");
    assertThat(sourceNode.getLabels()).isEqualTo(List.of("LabelA"));
    assertThat(sourceNode.getProperties()).hasSize(2);
    assertThat(sourceNode.getProperties().get(0).getSourceField()).isEqualTo("field_1");
    assertThat(sourceNode.getProperties().get(0).getTargetProperty()).isEqualTo("property1");
    assertThat(sourceNode.getProperties().get(1).getSourceField()).isEqualTo("field_2");
    assertThat(sourceNode.getProperties().get(1).getTargetProperty()).isEqualTo("property2");
    assertThat(sourceNode.getSchema().getKeyConstraints()).hasSize(1);
    assertThat(sourceNode.getSchema().getKeyConstraints().get(0).getName())
        .isEqualTo("LabelA key constraint");
    assertThat(sourceNode.getSchema().getKeyConstraints().get(0).getLabel()).isEqualTo("LabelA");
    assertThat(sourceNode.getSchema().getKeyConstraints().get(0).getProperties()).hasSize(1);
    assertThat(sourceNode.getSchema().getKeyConstraints().get(0).getProperties().get(0))
        .isEqualTo("property1");

    var targetNode = nodes.get(1);
    assertThat(targetNode.getSource()).isEqualTo("b-source");
    assertThat(targetNode.getName()).isEqualTo("b-node-target");
    assertThat(targetNode.getWriteMode().name()).isEqualTo("MERGE");
    assertThat(targetNode.getLabels()).isEqualTo(List.of("LabelB"));
    assertThat(targetNode.getProperties()).hasSize(2);
    assertThat(targetNode.getProperties().get(0).getSourceField()).isEqualTo("field_1");
    assertThat(targetNode.getProperties().get(0).getTargetProperty()).isEqualTo("property1");
    assertThat(targetNode.getProperties().get(1).getSourceField()).isEqualTo("field_2");
    assertThat(targetNode.getProperties().get(1).getTargetProperty()).isEqualTo("property2");
    assertThat(targetNode.getSchema().getKeyConstraints()).hasSize(1);
    assertThat(targetNode.getSchema().getKeyConstraints().get(0).getName())
        .isEqualTo("LabelB key constraint");
    assertThat(targetNode.getSchema().getKeyConstraints().get(0).getLabel()).isEqualTo("LabelB");
    assertThat(targetNode.getSchema().getKeyConstraints().get(0).getProperties()).hasSize(1);
    assertThat(targetNode.getSchema().getKeyConstraints().get(0).getProperties().get(0))
        .isEqualTo("property1");

    var relationships = targets.getRelationships();
    assertThat(relationships).hasSize(1);

    var relationship = relationships.get(0);
    assertThat(relationship.getName()).isEqualTo("a-target");
    assertThat(relationship.getSource()).isEqualTo("a-source");
    assertThat(relationship.getType()).isEqualTo("TYPE");
    assertThat(relationship.getWriteMode().name()).isEqualTo("CREATE");
    assertThat(relationship.getNodeMatchMode().name()).isEqualTo("MERGE");
    assertThat(relationship.getStartNodeReference()).isEqualTo("a-node-target");
    assertThat(relationship.getEndNodeReference()).isEqualTo("b-node-target");
    assertThat(relationship.getProperties()).hasSize(1);
    assertThat(relationship.getProperties().get(0).getSourceField()).isEqualTo("field_1");
    assertThat(relationship.getProperties().get(0).getTargetProperty()).isEqualTo("id");
    assertThat(relationship.getSchema().getKeyConstraints()).hasSize(1);
    assertThat(relationship.getSchema().getKeyConstraints().get(0).getName())
        .isEqualTo("rel key constraint");
    assertThat(relationship.getSchema().getKeyConstraints().get(0).getProperties()).hasSize(1);
    assertThat(relationship.getSchema().getKeyConstraints().get(0).getProperties().get(0))
        .isEqualTo("id");
  }

  @Test
  public void throws_exception_invalid_json() {
    assertThrows(
        "Parsing failed: content is neither valid JSON nor valid YAML.\n"
            + "JSON parse error: Unterminated string at 21 [character 0 line 3]\n"
            + "YAML parse error: while parsing a flow mapping\n"
            + " in 'string', line 1, column 1:\n"
            + "    {\n"
            + "    ^\n"
            + "expected ',' or '}', but got <scalar>\n"
            + " in 'string', line 3, column 6:\n"
            + "        \"sources\": [\n"
            + "         ^",
        IllegalArgumentException.class,
        () -> JobSpecMapper.parse(SPEC_PATH + "/invalid-json.json", new OptionsParams()));
  }

  @Test
  public void throws_exception_invalid_yaml() {
    assertThrows(
        "Parsing failed: content is neither valid JSON nor valid YAML.\n"
            + "JSON parse error: A JSONObject text must begin with '{' at 1 [character 2 line 1]\n"
            + "YAML parse error: while parsing a block mapping\n"
            + " in 'string', line 1, column 1:\n"
            + "    version: \"1\n"
            + "    ^\n"
            + "expected <block end>, but found '<scalar>'\n"
            + " in 'string', line 5, column 13:\n"
            + "        query: \"SELECT field_1, field_2 FROM my. ... \n"
            + "                ^",
        IllegalArgumentException.class,
        () -> JobSpecMapper.parse(SPEC_PATH + "/invalid-yaml.yaml", new OptionsParams()));
  }

  @Test
  public void throws_exception_valid_json_wrong_format_legacy_spec() {
    assertThrows(
        "Unable to process Neo4j job specification",
        RuntimeException.class,
        () ->
            JobSpecMapper.parse(
                SPEC_PATH + "/valid-json-wrong-format-legacy-spec.json", new OptionsParams()));
  }

  @Test
  public void throws_exception_valid_json_wrong_format_import_spec() {
    assertThrows(
        "Unable to process Neo4j job specification",
        RuntimeException.class,
        () ->
            JobSpecMapper.parse(
                SPEC_PATH + "/valid-json-wrong-format-import-spec.json", new OptionsParams()));
  }

  @Test
  public void throws_exception_valid_yaml_wrong_format_import_spec() {
    assertThrows(
        "Unable to process Neo4j job specification",
        RuntimeException.class,
        () ->
            JobSpecMapper.parse(
                SPEC_PATH + "/valid-yaml-wrong-format-import-spec.yaml", new OptionsParams()));
  }
}
