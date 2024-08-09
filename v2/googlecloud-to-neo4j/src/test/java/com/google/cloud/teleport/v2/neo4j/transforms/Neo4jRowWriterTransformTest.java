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
package com.google.cloud.teleport.v2.neo4j.transforms;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.neo4j.database.Neo4jCapabilities;
import com.google.cloud.teleport.v2.neo4j.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetSequence;
import com.google.cloud.teleport.v2.neo4j.model.sources.InlineTextSource;
import com.google.cloud.teleport.v2.neo4j.utils.BeamUtils;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Test;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.importer.v1.ImportSpecification;
import org.neo4j.importer.v1.targets.NodeKeyConstraint;
import org.neo4j.importer.v1.targets.NodeSchema;
import org.neo4j.importer.v1.targets.NodeTarget;
import org.neo4j.importer.v1.targets.PropertyMapping;
import org.neo4j.importer.v1.targets.Targets;
import org.neo4j.importer.v1.targets.WriteMode;

public class Neo4jRowWriterTransformTest {

  @Test
  public void sends_transaction_metadata_for_schema_init() {
    var connection = mock(Neo4jConnection.class);
    when(connection.capabilities()).thenReturn(new Neo4jCapabilities("5.20", "enterprise"));
    var header = List.of("placeholder-field1", "placeholder-field2");
    var properties = List.of(new PropertyMapping("placeholder-field1", "prop1", null));
    var schema =
        new NodeSchema(
            null,
            List.of(
                new NodeKeyConstraint("a-key-constraint", "Placeholder", List.of("prop1"), null)),
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    var target =
        new NodeTarget(
            true,
            "a-target",
            "a-source",
            null,
            WriteMode.CREATE,
            null,
            List.of("Placeholder"),
            properties,
            schema);
    var spec =
        new ImportSpecification(
            "test-version",
            null,
            List.of(
                new InlineTextSource(
                    "a-source",
                    List.of(List.of("placeholder", "for"), List.of("inline", "data")),
                    header)),
            new Targets(List.of(target), null, null),
            null);
    var transform =
        new Neo4jRowWriterTransform(spec, new TargetSequence(), target, () -> connection);

    TestPipeline.create().apply(Create.empty(BeamUtils.textToBeamSchema(header))).apply(transform);

    var expectedTxMetadata =
        Map.of(
            "sink", "neo4j", "source", "Text/Inline", "target-type", "node", "step", "init-schema");
    var expectedTransactionConfig =
        TransactionConfig.builder()
            .withMetadata(Map.of("app", "dataflow", "metadata", expectedTxMetadata))
            .build();
    verify(connection).runAutocommit(any(), eq(expectedTransactionConfig));
  }
}
