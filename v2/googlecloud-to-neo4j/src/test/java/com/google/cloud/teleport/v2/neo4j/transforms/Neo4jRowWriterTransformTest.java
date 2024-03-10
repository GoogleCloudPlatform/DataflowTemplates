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

import com.google.cloud.teleport.v2.neo4j.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.model.enums.RoleType;
import com.google.cloud.teleport.v2.neo4j.model.enums.SourceType;
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.job.FieldNameTuple;
import com.google.cloud.teleport.v2.neo4j.model.job.JobSpec;
import com.google.cloud.teleport.v2.neo4j.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.model.job.Source;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import com.google.cloud.teleport.v2.neo4j.utils.BeamUtils;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Test;
import org.neo4j.driver.TransactionConfig;

public class Neo4jRowWriterTransformTest {

  @Test
  public void sendsTransactionMetadataForSchemaInit() {
    Neo4jConnection connection = mock(Neo4jConnection.class);
    JobSpec jobSpec = new JobSpec();
    Source source = new Source();
    source.setSourceType(SourceType.text);
    source.setInline(List.of(List.of("placeholder", "for"), List.of("inline", "data")));
    jobSpec.getSources().put("a-source", source);
    Target target = new Target();
    target.setName("placeholder-target");
    target.setType(TargetType.node);
    target.setSource("a-source");
    target.setSequence(42);
    target.setMappings(List.of(aLabelMapping(), aKeyMapping()));
    Neo4jRowWriterTransform transform =
        new Neo4jRowWriterTransform(jobSpec, target, () -> connection);

    TestPipeline.create()
        .apply(
            Create.empty(
                BeamUtils.textToBeamSchema(
                    new String[] {"placeholder-field1", "placeholder-field2"})))
        .apply(transform);

    Map<String, String> expectedTxMetadata =
        Map.of(
            "sink", "neo4j", "source", "Text/Inline", "target-type", "node", "step", "init-schema");
    TransactionConfig expectedTransactionConfig =
        TransactionConfig.builder()
            .withMetadata(Map.of("app", "dataflow", "metadata", expectedTxMetadata))
            .build();
    verify(connection).executeCypher(any(), eq(expectedTransactionConfig));
  }

  private static Mapping aLabelMapping() {
    FieldNameTuple tuple = new FieldNameTuple();
    tuple.setConstant("PlaceholderLabel");
    return new Mapping(FragmentType.node, RoleType.label, tuple);
  }

  private static Mapping aKeyMapping() {
    FieldNameTuple tuple = new FieldNameTuple();
    tuple.setField("field");
    tuple.setName("name");
    return new Mapping(FragmentType.node, RoleType.key, tuple);
  }
}
