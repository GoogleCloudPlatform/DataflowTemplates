/*
 * Copyright (C) 2026 Google LLC
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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.cloud.teleport.v2.neo4j.database.Neo4jCapabilities;
import com.google.cloud.teleport.v2.neo4j.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.telemetry.ReportedSourceType;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.importer.v1.pipeline.NodeTargetStep;
import org.neo4j.importer.v1.targets.NodeKeyConstraint;
import org.neo4j.importer.v1.targets.NodeSchema;
import org.neo4j.importer.v1.targets.NodeTarget;
import org.neo4j.importer.v1.targets.NodeUniqueConstraint;
import org.neo4j.importer.v1.targets.PropertyMapping;
import org.neo4j.importer.v1.targets.WriteMode;

public class Neo4jInitSchemaFnTest {

  private static final List<String> EXECUTED_STATEMENTS =
      Collections.synchronizedList(new ArrayList<>());

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Before
  public void clearExecutedStatements() {
    EXECUTED_STATEMENTS.clear();
  }

  @Test
  public void executes_all_schema_statements() {
    var connection = newConnection();

    PCollection<Integer> output =
        pipeline.apply(Create.of(1)).apply(ParDo.of(initSchemaFn(connection)));
    PAssert.that(output).containsInAnyOrder(1);

    pipeline.run();

    assertThat(EXECUTED_STATEMENTS).hasSize(2);
    assertThat(EXECUTED_STATEMENTS.toString()).contains("IS NODE KEY");
    assertThat(EXECUTED_STATEMENTS.toString()).contains("IS UNIQUE");
  }

  @Test
  public void propagates_schema_statement_failure() {
    var connection = newConnection();
    doAnswer(new RecordAndFailQuery("schema is broken"))
        .when(connection)
        .runAutocommit(anyString(), any());

    PCollection<Integer> output =
        pipeline.apply(Create.of(1)).apply(ParDo.of(initSchemaFn(connection)));
    PAssert.that(output).containsInAnyOrder(1);

    var thrown = assertThrows(PipelineExecutionException.class, pipeline::run);

    assertThat(hasCauseMessage(thrown, "Error initializing schema with Cypher")).isTrue();
    assertThat(hasCauseMessage(thrown, "schema is broken")).isTrue();
  }

  @Test
  public void stops_executing_schema_statements_after_a_failure() {
    var connection = newConnection();
    doAnswer(new RecordAndFailQuery("schema is broken"))
        .when(connection)
        .runAutocommit(anyString(), any());

    PCollection<Integer> output =
        pipeline.apply(Create.of(1)).apply(ParDo.of(initSchemaFn(connection)));
    PAssert.that(output).containsInAnyOrder(1);

    assertThrows(PipelineExecutionException.class, pipeline::run);

    assertThat(EXECUTED_STATEMENTS).hasSize(1);
  }

  private static Neo4jConnection newConnection() {
    var connection = mock(Neo4jConnection.class, withSettings().serializable());
    when(connection.capabilities()).thenReturn(new Neo4jCapabilities("5.20", "enterprise"));
    doAnswer(new RecordQuery()).when(connection).runAutocommit(anyString(), any());
    return connection;
  }

  private static Neo4jInitSchemaFn initSchemaFn(Neo4jConnection connection) {
    return new Neo4jInitSchemaFn(
        nodeTargetStep(), ReportedSourceType.TEXT_INLINE, () -> connection);
  }

  private static NodeTargetStep nodeTargetStep() {
    var properties = List.of(new PropertyMapping("placeholder-field1", "prop1", null));
    var schema =
        new NodeSchema(
            null,
            List.of(
                new NodeKeyConstraint("a-key-constraint", "Placeholder", List.of("prop1"), null)),
            List.of(
                new NodeUniqueConstraint(
                    "a-unique-constraint", "Placeholder", List.of("prop1"), null)),
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
            List.of(),
            List.of("Placeholder"),
            properties,
            schema);
    return new NodeTargetStep(target, Set.of());
  }

  private static boolean hasCauseMessage(Throwable throwable, String expected) {
    for (Throwable current = throwable; current != null; current = current.getCause()) {
      if (current.getMessage() != null && current.getMessage().contains(expected)) {
        return true;
      }
    }
    return false;
  }

  private static final class RecordQuery implements Answer<Void>, Serializable {

    @Override
    public Void answer(InvocationOnMock invocation) {
      EXECUTED_STATEMENTS.add(invocation.getArgument(0));
      return null;
    }
  }

  private record RecordAndFailQuery(String message) implements Answer<Void>, Serializable {

    @Override
    public Void answer(InvocationOnMock invocation) {
      EXECUTED_STATEMENTS.add(invocation.getArgument(0));
      throw new RuntimeException(message);
    }
  }
}
