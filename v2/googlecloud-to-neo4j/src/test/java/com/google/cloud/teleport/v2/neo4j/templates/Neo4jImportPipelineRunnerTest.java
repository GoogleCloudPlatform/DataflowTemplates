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
package com.google.cloud.teleport.v2.neo4j.templates;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.neo4j.model.connection.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.model.helpers.JobSpecMapper;
import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetQuerySpec;
import com.google.cloud.teleport.v2.neo4j.model.job.OverlayTokens;
import com.google.cloud.teleport.v2.neo4j.model.sources.BigQuerySource;
import com.google.cloud.teleport.v2.neo4j.model.sources.InlineTextSource;
import com.google.cloud.teleport.v2.neo4j.providers.SourceProvider;
import com.google.cloud.teleport.v2.neo4j.templates.Neo4jImportPipelineRunner.SourceContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.importer.v1.ImportSpecification;
import org.neo4j.importer.v1.pipeline.ActionStep;
import org.neo4j.importer.v1.pipeline.CustomQueryTargetStep;
import org.neo4j.importer.v1.pipeline.ImportPipeline;
import org.neo4j.importer.v1.pipeline.ImportStep;
import org.neo4j.importer.v1.pipeline.NodeTargetStep;
import org.neo4j.importer.v1.pipeline.RelationshipTargetStep;
import org.neo4j.importer.v1.pipeline.SourceStep;
import org.neo4j.importer.v1.pipeline.TargetStep;

@RunWith(Parameterized.class)
public class Neo4jImportPipelineRunnerTest {

  private static final String SPEC_ROOT =
      "src/test/resources/testing-specs/neo4j-import-pipeline-test";

  @Parameterized.Parameters(name = "{0}")
  public static Object[] parameters() {
    return new Object[] {"json", "yaml"};
  }

  @Rule
  public final transient TestPipeline pipeline =
      TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Rule public final transient TestName testName = new TestName();

  private final String format;

  public Neo4jImportPipelineRunnerTest(String format) {
    this.format = format;
  }

  @Test
  public void resolve_dependencies_include_startup_step_for_action_steps() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipelineRunner = newImportPipelineRunner(ImportPipeline.of(importSpecification));
    var startup = pipeline.apply("startup", Create.of(1L));
    importPipelineRunner.registerDependency("__VERIFY_OR_RESET__", startup);

    var actionStep = findTargetStep(ImportPipeline.of(importSpecification), ActionStep.class);

    List<PCollection<?>> dependencies = importPipelineRunner.resolveDependencies(actionStep);

    assertThat(dependencies).contains(startup);
  }

  @Test
  public void
      resolve_dependencies_include_start_and_end_node_dependencies_for_relationship_target_steps() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipelineRunner = newImportPipelineRunner(ImportPipeline.of(importSpecification));
    var startup = pipeline.apply("startup", Create.of(1L));
    var sourceDone = pipeline.apply("source", Create.of(10L));
    var startNodeDone = pipeline.apply("start node", Create.of(2L));
    var endNodeDone = pipeline.apply("end node", Create.of(3L));
    importPipelineRunner.registerDependency("__VERIFY_OR_RESET__", startup);
    importPipelineRunner.registerDependency("inline-source", sourceDone);
    importPipelineRunner.registerDependency("a-node-target", startNodeDone);
    importPipelineRunner.registerDependency("b-node-target", endNodeDone);

    var relationshipStep =
        findTargetStep(ImportPipeline.of(importSpecification), RelationshipTargetStep.class);

    List<PCollection<?>> dependencies = importPipelineRunner.resolveDependencies(relationshipStep);

    assertThat(dependencies).contains(startup);
    assertThat(dependencies).contains(sourceDone);
    assertThat(dependencies).contains(startNodeDone);
    assertThat(dependencies).contains(endNodeDone);
  }

  @Test
  public void
      resolve_dependencies_do_not_duplicate_self_referencing_relationship_node_dependencies() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipelineRunner = newImportPipelineRunner(ImportPipeline.of(importSpecification));
    var startup = pipeline.apply("startup", Create.of(1L));
    var sourceDone = pipeline.apply("source", Create.of(10L));
    var nodeDone = pipeline.apply("self node", Create.of(2L));
    importPipelineRunner.registerDependency("__VERIFY_OR_RESET__", startup);
    importPipelineRunner.registerDependency("inline-source", sourceDone);
    importPipelineRunner.registerDependency("a-node-target", nodeDone);

    var relationshipStep =
        (RelationshipTargetStep)
            findTargetStepNamed(ImportPipeline.of(importSpecification), "self-link-target");

    List<PCollection<?>> dependencies = importPipelineRunner.resolveDependencies(relationshipStep);

    assertThat(dependencies).containsExactly(startup, sourceDone, nodeDone);
  }

  @Test
  public void resolve_dependencies_include_explicit_step_dependencies_for_node_target_steps() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipelineRunner = newImportPipelineRunner(ImportPipeline.of(importSpecification));
    var startup = pipeline.apply("startup", Create.of(1L));
    var sourceDone = pipeline.apply("source", Create.of(10L));
    var firstNodeDone = pipeline.apply("first node", Create.of(2L));
    importPipelineRunner.registerDependency("__VERIFY_OR_RESET__", startup);
    importPipelineRunner.registerDependency("inline-source", sourceDone);
    importPipelineRunner.registerDependency("a-node-target", firstNodeDone);

    var dependentNodeStep =
        findTargetStepNamed(ImportPipeline.of(importSpecification), "b-node-target");

    List<PCollection<?>> dependencies = importPipelineRunner.resolveDependencies(dependentNodeStep);

    assertThat(dependencies).contains(startup);
    assertThat(dependencies).contains(sourceDone);
    assertThat(dependencies).contains(firstNodeDone);
  }

  @Test
  public void resolve_dependencies_include_explicit_step_dependencies_for_query_target_steps() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipelineRunner = newImportPipelineRunner(ImportPipeline.of(importSpecification));
    var startup = pipeline.apply("startup", Create.of(1L));
    var sourceDone = pipeline.apply("source", Create.of(10L));
    var queryDependency = pipeline.apply("query dep", Create.of(2L));
    importPipelineRunner.registerDependency("__VERIFY_OR_RESET__", startup);
    importPipelineRunner.registerDependency("inline-source", sourceDone);
    importPipelineRunner.registerDependency("b-node-target", queryDependency);

    var queryStep = findTargetStepNamed(ImportPipeline.of(importSpecification), "query-target");

    List<PCollection<?>> dependencies = importPipelineRunner.resolveDependencies(queryStep);

    assertThat(dependencies).contains(startup);
    assertThat(dependencies).contains(sourceDone);
    assertThat(dependencies).contains(queryDependency);
  }

  @Test
  public void
      resolve_dependencies_include_source_and_startup_for_node_targets_without_explicit_dependencies() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipelineRunner = newImportPipelineRunner(ImportPipeline.of(importSpecification));
    var startup = pipeline.apply("startup", Create.of(1L));
    var sourceDone = pipeline.apply("source", Create.of(10L));
    importPipelineRunner.registerDependency("__VERIFY_OR_RESET__", startup);
    importPipelineRunner.registerDependency("inline-source", sourceDone);
    var nodeStep = findTargetStepNamed(ImportPipeline.of(importSpecification), "a-node-target");

    List<PCollection<?>> dependencies = importPipelineRunner.resolveDependencies(nodeStep);

    assertThat(dependencies).containsExactly(startup, sourceDone);
  }

  @Test
  public void query_source_rows_reuse_base_rows_for_query_targets() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipelineRunner = newImportPipelineRunner(ImportPipeline.of(importSpecification));
    var schema = Schema.of(Schema.Field.nullable("seller_id", Schema.FieldType.STRING));
    var startup = pipeline.apply("startup", Create.of(1L));
    var provider = new CapturingSourceProvider(false);
    var sourceContext =
        new SourceContext(
            new SourceStep(new BigQuerySource("a-name", "RETURN 42")), provider, schema, startup);
    var queryTarget = findFirstQueryTarget(importSpecification);

    var baseRows = sourceContext.getOrCreateRows(pipeline);
    var queryRows = importPipelineRunner.querySourceRows(pipeline, sourceContext, queryTarget);

    assertThat(queryRows).isSameInstanceAs(baseRows);
    assertThat(provider.querySourceRowsCalls).isEqualTo(1);
    assertThat(provider.querySourceRowsForTargetCalls).isEqualTo(0);
  }

  @Test
  public void query_source_rows_delegate_non_query_targets_to_provider_specific_queries() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipelineRunner = newImportPipelineRunner(ImportPipeline.of(importSpecification));
    var schema = Schema.of(Schema.Field.nullable("field_1", Schema.FieldType.STRING));
    var startup = pipeline.apply("startup", Create.of(1L));
    var provider = new CapturingSourceProvider(true);
    var source = new InlineTextSource("inline-source", List.of(List.of("v1")), List.of("field_1"));
    var sourceContext = new SourceContext(new SourceStep(source), provider, schema, startup);
    var nodeTarget = findNodeTarget(importSpecification, "a-node-target");

    PCollection<Row> queryRows =
        importPipelineRunner.querySourceRows(pipeline, sourceContext, nodeTarget);

    assertThat(queryRows).isNotNull();
    assertThat(provider.querySourceRowsCalls).isEqualTo(0);
    assertThat(provider.querySourceRowsForTargetCalls).isEqualTo(1);
  }

  @Test
  public void source_context_caches_source_rows() {
    var importSpecification = importSpecificationOfCurrentTest();
    assertThat(importSpecification.getSources()).isNotEmpty();
    var schema = Schema.of(Schema.Field.nullable("field_1", Schema.FieldType.STRING));
    var startup = pipeline.apply("startup", Create.of(1L));
    var provider = new CapturingSourceProvider(false);
    var source = new InlineTextSource("inline-source", List.of(List.of("v1")), List.of("field_1"));
    var sourceContext = new SourceContext(new SourceStep(source), provider, schema, startup);

    var first = sourceContext.getOrCreateRows(pipeline);
    var second = sourceContext.getOrCreateRows(pipeline);

    assertThat(first).isSameInstanceAs(second);
    assertThat(provider.querySourceRowsCalls).isEqualTo(1);
  }

  @Test
  public void build_target_query_spec_includes_base_rows_for_non_pushdown_sources() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipelineRunner = newImportPipelineRunner(ImportPipeline.of(importSpecification));
    var schema = Schema.of(Schema.Field.nullable("field_1", Schema.FieldType.STRING));
    var startup = pipeline.apply("startup", Create.of(1L));
    var provider = new CapturingSourceProvider(false);
    var source = new InlineTextSource("inline-source", List.of(List.of("v1")), List.of("field_1"));
    var sourceContext = new SourceContext(new SourceStep(source), provider, schema, startup);
    var target = findNodeTarget(importSpecification, "a-node-target");

    var targetQuerySpec =
        importPipelineRunner.buildTargetQuerySpec(pipeline, sourceContext, target);

    assertThat(targetQuerySpec.getNullableSourceRows()).isNotNull();
    assertThat(provider.querySourceRowsCalls).isEqualTo(1);
  }

  @Test
  public void build_target_query_spec_does_not_include_base_rows_for_pushdown_sources() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipelineRunner = newImportPipelineRunner(ImportPipeline.of(importSpecification));
    var schema = Schema.of(Schema.Field.nullable("field_1", Schema.FieldType.STRING));
    var startup = pipeline.apply("startup", Create.of(1L));
    var provider = new CapturingSourceProvider(true);
    var source = new InlineTextSource("inline-source", List.of(List.of("v1")), List.of("field_1"));
    var sourceContext = new SourceContext(new SourceStep(source), provider, schema, startup);
    var target = findNodeTarget(importSpecification, "a-node-target");

    var targetQuerySpec =
        importPipelineRunner.buildTargetQuerySpec(pipeline, sourceContext, target);

    assertThat(targetQuerySpec.getNullableSourceRows()).isNull();
    assertThat(provider.querySourceRowsCalls).isEqualTo(0);
  }

  @Test
  public void handle_source_registers_source_context_and_metadata_dependency() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipelineRunner = newImportPipelineRunner(ImportPipeline.of(importSpecification));
    var startup = pipeline.apply("startup", Create.of(1L));
    importPipelineRunner.registerDependency("__VERIFY_OR_RESET__", startup);
    var sourceStep = findTargetStep(ImportPipeline.of(importSpecification), SourceStep.class);

    importPipelineRunner.handleSource(pipeline, sourceStep);

    assertThat(importPipelineRunner.findSourceContext(sourceStep.source().getName())).isNotNull();
    assertThat(importPipelineRunner.findDependency(sourceStep.name())).isNotNull();
  }

  @Test
  public void handle_action_registers_action_completion_dependency() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipelineRunner = newImportPipelineRunner(ImportPipeline.of(importSpecification));
    var startup = pipeline.apply("startup", Create.of(1L));
    importPipelineRunner.registerDependency("__VERIFY_OR_RESET__", startup);
    var actionStep = findTargetStep(ImportPipeline.of(importSpecification), ActionStep.class);

    importPipelineRunner.handleAction(pipeline, actionStep);

    assertThat(importPipelineRunner.findDependency(actionStep.name())).isNotNull();
  }

  @Test
  public void
      handle_target_registers_node_target_completion_dependency_using_target_specific_query() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipelineRunner = newImportPipelineRunner(ImportPipeline.of(importSpecification));
    var startup = pipeline.apply("startup", Create.of(1L));
    var schema =
        Schema.of(
            Schema.Field.nullable("field_1", Schema.FieldType.STRING),
            Schema.Field.nullable("field_2", Schema.FieldType.STRING));
    var provider = new CapturingSourceProvider(true);
    var source = new InlineTextSource("inline-source", List.of(List.of("v1")), List.of("field_1"));
    var sourceContext = new SourceContext(new SourceStep(source), provider, schema, startup);
    importPipelineRunner.registerDependency("__VERIFY_OR_RESET__", startup);
    importPipelineRunner.registerDependency(
        "inline-source", pipeline.apply("source metadata", Create.of(2L)));
    importPipelineRunner.registerSourceContext("inline-source", sourceContext);
    var nodeStep = findTargetStepNamed(ImportPipeline.of(importSpecification), "a-node-target");

    importPipelineRunner.handleTarget(pipeline, nodeStep);

    assertThat(importPipelineRunner.findDependency(nodeStep.name())).isNotNull();
    assertThat(provider.querySourceRowsCalls).isEqualTo(0);
    assertThat(provider.querySourceRowsForTargetCalls).isEqualTo(1);
  }

  @Test
  public void handle_target_registers_query_target_completion_dependency_using_base_source_rows() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipelineRunner = newImportPipelineRunner(ImportPipeline.of(importSpecification));
    var startup = pipeline.apply("startup", Create.of(1L));
    var schema = Schema.of(Schema.Field.nullable("seller_id", Schema.FieldType.STRING));
    var provider = new CapturingSourceProvider(false);
    var source = new InlineTextSource("inline-source", List.of(List.of("v1")), List.of("field_1"));
    var sourceContext = new SourceContext(new SourceStep(source), provider, schema, startup);
    importPipelineRunner.registerDependency("__VERIFY_OR_RESET__", startup);
    importPipelineRunner.registerDependency(
        "inline-source", pipeline.apply("source metadata", Create.of(2L)));
    importPipelineRunner.registerSourceContext("inline-source", sourceContext);
    var queryStep = findTargetStepNamed(ImportPipeline.of(importSpecification), "query-target");

    importPipelineRunner.handleTarget(pipeline, queryStep);

    assertThat(importPipelineRunner.findDependency(queryStep.name())).isNotNull();
    assertThat(provider.querySourceRowsCalls).isEqualTo(1);
    assertThat(provider.querySourceRowsForTargetCalls).isEqualTo(0);
  }

  private Neo4jImportPipelineRunner newImportPipelineRunner(ImportPipeline pipeline) {
    return new Neo4jImportPipelineRunner(
        this.pipeline.getOptions(),
        "test-version",
        connectionParams(),
        new OverlayTokens(Map.of()),
        pipeline);
  }

  private static ConnectionParams connectionParams() {
    return new ConnectionParams("bolt://example.com", null) {
      @Override
      public AuthToken asAuthToken() {
        return AuthTokens.none();
      }
    };
  }

  private ImportSpecification importSpecificationOf(String specBaseName) {
    return JobSpecMapper.parse(specPath(specBaseName), new OverlayTokens(Map.of()));
  }

  private ImportSpecification importSpecificationOfCurrentTest() {
    var methodName = testName.getMethodName();
    var parameterizedSuffixIndex = methodName.indexOf('[');
    if (parameterizedSuffixIndex >= 0) {
      methodName = methodName.substring(0, parameterizedSuffixIndex);
    }
    return importSpecificationOf(methodName.replace('_', '-'));
  }

  private String specPath(String specBaseName) {
    return SPEC_ROOT + "/" + specBaseName + "." + format;
  }

  private static NodeTargetStep findNodeTarget(
      ImportSpecification importSpecification, String name) {
    var nodeTarget =
        importSpecification.getTargets().getNodes().stream()
            .filter(target -> target.getName().equals(name))
            .findFirst()
            .orElseThrow();
    return new NodeTargetStep(nodeTarget, Set.of());
  }

  private static CustomQueryTargetStep findFirstQueryTarget(
      ImportSpecification importSpecification) {
    return new CustomQueryTargetStep(
        importSpecification.getTargets().getCustomQueries().stream().findFirst().orElseThrow(),
        Set.of());
  }

  private static <T extends ImportStep> T findTargetStep(
      ImportPipeline importPipeline, Class<T> type) {

    return StreamSupport.stream(importPipeline.spliterator(), false)
        .filter(type::isInstance)
        .map(type::cast)
        .findFirst()
        .orElseThrow();
  }

  private static TargetStep findTargetStepNamed(ImportPipeline importPipeline, String name) {
    for (ImportStep step : importPipeline) {
      if (step instanceof TargetStep targetStep && step.name().equals(name)) {
        return targetStep;
      }
    }
    throw new AssertionError("Could not find target step named " + name);
  }

  private static final class CapturingSourceProvider implements SourceProvider {

    private final boolean pushdownSupported;
    private int querySourceRowsCalls;
    private int querySourceRowsForTargetCalls;

    private CapturingSourceProvider(boolean pushdownSupported) {
      this.pushdownSupported = pushdownSupported;
    }

    @Override
    public void configure(OverlayTokens overlayTokens) {}

    @Override
    public boolean supportsSqlPushDown() {
      return pushdownSupported;
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> querySourceRows(Schema schema) {
      querySourceRowsCalls++;
      return Create.empty(schema);
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> querySourceRowsForTarget(
        TargetQuerySpec targetQuerySpec) {
      querySourceRowsForTargetCalls++;
      return Create.empty(targetQuerySpec.getSourceBeamSchema());
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> queryMetadata() {
      throw new UnsupportedOperationException("queryMetadata should not be called in this test");
    }
  }
}
