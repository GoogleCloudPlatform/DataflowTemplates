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
import com.google.cloud.teleport.v2.neo4j.providers.SourceProvider;
import com.google.cloud.teleport.v2.neo4j.templates.Neo4jImportPipeline.SourceContext;
import java.util.List;
import java.util.Map;
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
import org.neo4j.importer.v1.pipeline.ImportPipeline;
import org.neo4j.importer.v1.pipeline.ImportStep;
import org.neo4j.importer.v1.pipeline.RelationshipTargetStep;
import org.neo4j.importer.v1.pipeline.SourceStep;
import org.neo4j.importer.v1.pipeline.TargetStep;
import org.neo4j.importer.v1.targets.CustomQueryTarget;
import org.neo4j.importer.v1.targets.Target;

@RunWith(Parameterized.class)
public class Neo4jImportPipelineTest {

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

  public Neo4jImportPipelineTest(String format) {
    this.format = format;
  }

  @Test
  public void resolve_dependencies_include_startup_step_for_action_steps() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipeline = newImportPipeline(importSpecification);
    var startup = pipeline.apply("startup", Create.of(1L));
    importPipeline.registerDependency("__VERIFY_OR_RESET__", startup);

    var actionStep = firstStepOfType(ImportPipeline.of(importSpecification), ActionStep.class);

    List<PCollection<?>> dependencies = importPipeline.resolveDependencies(actionStep);

    assertThat(dependencies).contains(startup);
  }

  @Test
  public void
      resolve_dependencies_include_start_and_end_node_dependencies_for_relationship_target_steps() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipeline = newImportPipeline(importSpecification);
    var startup = pipeline.apply("startup", Create.of(1L));
    var sourceDone = pipeline.apply("source", Create.of(10L));
    var startNodeDone = pipeline.apply("start node", Create.of(2L));
    var endNodeDone = pipeline.apply("end node", Create.of(3L));
    importPipeline.registerDependency("__VERIFY_OR_RESET__", startup);
    importPipeline.registerDependency("inline-source", sourceDone);
    importPipeline.registerDependency("a-node-target", startNodeDone);
    importPipeline.registerDependency("b-node-target", endNodeDone);

    var relationshipStep =
        firstStepOfType(ImportPipeline.of(importSpecification), RelationshipTargetStep.class);

    List<PCollection<?>> dependencies = importPipeline.resolveDependencies(relationshipStep);

    assertThat(dependencies).contains(startup);
    assertThat(dependencies).contains(sourceDone);
    assertThat(dependencies).contains(startNodeDone);
    assertThat(dependencies).contains(endNodeDone);
  }

  @Test
  public void
      resolve_dependencies_do_not_duplicate_self_referencing_relationship_node_dependencies() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipeline = newImportPipeline(importSpecification);
    var startup = pipeline.apply("startup", Create.of(1L));
    var sourceDone = pipeline.apply("source", Create.of(10L));
    var nodeDone = pipeline.apply("self node", Create.of(2L));
    importPipeline.registerDependency("__VERIFY_OR_RESET__", startup);
    importPipeline.registerDependency("inline-source", sourceDone);
    importPipeline.registerDependency("a-node-target", nodeDone);

    var relationshipStep =
        (RelationshipTargetStep)
            targetStepNamed(ImportPipeline.of(importSpecification), "self-link-target");

    List<PCollection<?>> dependencies = importPipeline.resolveDependencies(relationshipStep);

    assertThat(dependencies).containsExactly(startup, sourceDone, nodeDone);
  }

  @Test
  public void resolve_dependencies_include_explicit_step_dependencies_for_node_target_steps() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipeline = newImportPipeline(importSpecification);
    var startup = pipeline.apply("startup", Create.of(1L));
    var sourceDone = pipeline.apply("source", Create.of(10L));
    var firstNodeDone = pipeline.apply("first node", Create.of(2L));
    importPipeline.registerDependency("__VERIFY_OR_RESET__", startup);
    importPipeline.registerDependency("inline-source", sourceDone);
    importPipeline.registerDependency("a-node-target", firstNodeDone);

    var dependentNodeStep =
        targetStepNamed(ImportPipeline.of(importSpecification), "b-node-target");

    List<PCollection<?>> dependencies = importPipeline.resolveDependencies(dependentNodeStep);

    assertThat(dependencies).contains(startup);
    assertThat(dependencies).contains(sourceDone);
    assertThat(dependencies).contains(firstNodeDone);
  }

  @Test
  public void resolve_dependencies_include_explicit_step_dependencies_for_query_target_steps() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipeline = newImportPipeline(importSpecification);
    var startup = pipeline.apply("startup", Create.of(1L));
    var sourceDone = pipeline.apply("source", Create.of(10L));
    var queryDependency = pipeline.apply("query dep", Create.of(2L));
    importPipeline.registerDependency("__VERIFY_OR_RESET__", startup);
    importPipeline.registerDependency("inline-source", sourceDone);
    importPipeline.registerDependency("b-node-target", queryDependency);

    var queryStep = targetStepNamed(ImportPipeline.of(importSpecification), "query-target");

    List<PCollection<?>> dependencies = importPipeline.resolveDependencies(queryStep);

    assertThat(dependencies).contains(startup);
    assertThat(dependencies).contains(sourceDone);
    assertThat(dependencies).contains(queryDependency);
  }

  @Test
  public void
      resolve_dependencies_include_source_and_startup_for_node_targets_without_explicit_dependencies() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipeline = newImportPipeline(importSpecification);
    var startup = pipeline.apply("startup", Create.of(1L));
    var sourceDone = pipeline.apply("source", Create.of(10L));
    importPipeline.registerDependency("__VERIFY_OR_RESET__", startup);
    importPipeline.registerDependency("inline-source", sourceDone);
    var nodeStep = targetStepNamed(ImportPipeline.of(importSpecification), "a-node-target");

    List<PCollection<?>> dependencies = importPipeline.resolveDependencies(nodeStep);

    assertThat(dependencies).containsExactly(startup, sourceDone);
  }

  @Test
  public void query_source_rows_reuse_base_rows_for_query_targets() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipeline = newImportPipeline(importSpecification);
    var schema = Schema.of(Schema.Field.nullable("seller_id", Schema.FieldType.STRING));
    var startup = pipeline.apply("startup", Create.of(1L));
    var provider = new CapturingSourceProvider(false);
    var sourceContext = newSourceContext("inline-source", provider, schema, startup);
    var queryTarget = customQueryTarget(importSpecification);

    var baseRows = sourceContext.getOrCreateRows(pipeline);
    var queryRows = importPipeline.querySourceRows(pipeline, sourceContext, queryTarget);

    assertThat(queryRows).isSameInstanceAs(baseRows);
    assertThat(provider.querySourceRowsCalls).isEqualTo(1);
    assertThat(provider.querySourceRowsForTargetCalls).isEqualTo(0);
  }

  @Test
  public void query_source_rows_delegate_non_query_targets_to_provider_specific_queries() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipeline = newImportPipeline(importSpecification);
    var schema = Schema.of(Schema.Field.nullable("field_1", Schema.FieldType.STRING));
    var startup = pipeline.apply("startup", Create.of(1L));
    var provider = new CapturingSourceProvider(true);
    var sourceContext = newSourceContext("inline-source", provider, schema, startup);
    var nodeTarget = findTarget(importSpecification, "a-node-target");

    PCollection<Row> queryRows =
        importPipeline.querySourceRows(pipeline, sourceContext, nodeTarget);

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
    var sourceContext = newSourceContext("inline-source", provider, schema, startup);

    var first = sourceContext.getOrCreateRows(pipeline);
    var second = sourceContext.getOrCreateRows(pipeline);

    assertThat(first).isSameInstanceAs(second);
    assertThat(provider.querySourceRowsCalls).isEqualTo(1);
  }

  @Test
  public void build_target_query_spec_includes_base_rows_for_non_pushdown_sources() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipeline = newImportPipeline(importSpecification);
    var schema = Schema.of(Schema.Field.nullable("field_1", Schema.FieldType.STRING));
    var startup = pipeline.apply("startup", Create.of(1L));
    var provider = new CapturingSourceProvider(false);
    var sourceContext = newSourceContext("inline-source", provider, schema, startup);
    var target = findTarget(importSpecification, "a-node-target");

    var targetQuerySpec = importPipeline.buildTargetQuerySpec(pipeline, sourceContext, target);

    assertThat(targetQuerySpec.getNullableSourceRows()).isNotNull();
    assertThat(provider.querySourceRowsCalls).isEqualTo(1);
  }

  @Test
  public void build_target_query_spec_does_not_include_base_rows_for_pushdown_sources() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipeline = newImportPipeline(importSpecification);
    var schema = Schema.of(Schema.Field.nullable("field_1", Schema.FieldType.STRING));
    var startup = pipeline.apply("startup", Create.of(1L));
    var provider = new CapturingSourceProvider(true);
    var sourceContext = newSourceContext("inline-source", provider, schema, startup);
    var target = findTarget(importSpecification, "a-node-target");

    var targetQuerySpec = importPipeline.buildTargetQuerySpec(pipeline, sourceContext, target);

    assertThat(targetQuerySpec.getNullableSourceRows()).isNull();
    assertThat(provider.querySourceRowsCalls).isEqualTo(0);
  }

  @Test
  public void build_target_query_spec_includes_relationship_endpoint_targets() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipeline = newImportPipeline(importSpecification);
    var schema = Schema.of(Schema.Field.nullable("field_1", Schema.FieldType.STRING));
    var startup = pipeline.apply("startup", Create.of(1L));
    var provider = new CapturingSourceProvider(true);
    var sourceContext = newSourceContext("inline-source", provider, schema, startup);
    var relationshipTarget = findTarget(importSpecification, "a-target");

    var targetQuerySpec =
        importPipeline.buildTargetQuerySpec(pipeline, sourceContext, relationshipTarget);

    assertThat(targetQuerySpec.getStartNodeTarget().getName()).isEqualTo("a-node-target");
    assertThat(targetQuerySpec.getEndNodeTarget().getName()).isEqualTo("b-node-target");
  }

  @Test
  public void handle_source_registers_source_context_and_metadata_dependency() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipeline = newImportPipeline(importSpecification);
    var startup = pipeline.apply("startup", Create.of(1L));
    importPipeline.registerDependency("__VERIFY_OR_RESET__", startup);
    var sourceStep = firstStepOfType(ImportPipeline.of(importSpecification), SourceStep.class);

    importPipeline.handleSource(pipeline, sourceStep);

    assertThat(importPipeline.findSourceContext(sourceStep.source().getName())).isNotNull();
    assertThat(importPipeline.findDependency(sourceStep.name())).isNotNull();
  }

  @Test
  public void handle_action_registers_action_completion_dependency() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipeline = newImportPipeline(importSpecification);
    var startup = pipeline.apply("startup", Create.of(1L));
    importPipeline.registerDependency("__VERIFY_OR_RESET__", startup);
    var actionStep = firstStepOfType(ImportPipeline.of(importSpecification), ActionStep.class);

    importPipeline.handleAction(pipeline, actionStep);

    assertThat(importPipeline.findDependency(actionStep.name())).isNotNull();
  }

  @Test
  public void
      handle_target_registers_node_target_completion_dependency_using_target_specific_query() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipeline = newImportPipeline(importSpecification);
    var startup = pipeline.apply("startup", Create.of(1L));
    var schema =
        Schema.of(
            Schema.Field.nullable("field_1", Schema.FieldType.STRING),
            Schema.Field.nullable("field_2", Schema.FieldType.STRING));
    var provider = new CapturingSourceProvider(true);
    var sourceContext = newSourceContext("inline-source", provider, schema, startup);
    importPipeline.registerDependency("__VERIFY_OR_RESET__", startup);
    importPipeline.registerDependency(
        "inline-source", pipeline.apply("source metadata", Create.of(2L)));
    importPipeline.registerSourceContext("inline-source", sourceContext);
    var nodeStep = targetStepNamed(ImportPipeline.of(importSpecification), "a-node-target");

    importPipeline.handleTarget(pipeline, nodeStep);

    assertThat(importPipeline.findDependency(nodeStep.name())).isNotNull();
    assertThat(provider.querySourceRowsCalls).isEqualTo(0);
    assertThat(provider.querySourceRowsForTargetCalls).isEqualTo(1);
  }

  @Test
  public void handle_target_registers_query_target_completion_dependency_using_base_source_rows() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipeline = newImportPipeline(importSpecification);
    var startup = pipeline.apply("startup", Create.of(1L));
    var schema = Schema.of(Schema.Field.nullable("seller_id", Schema.FieldType.STRING));
    var provider = new CapturingSourceProvider(false);
    var sourceContext = newSourceContext("inline-source", provider, schema, startup);
    importPipeline.registerDependency("__VERIFY_OR_RESET__", startup);
    importPipeline.registerDependency(
        "inline-source", pipeline.apply("source metadata", Create.of(2L)));
    importPipeline.registerSourceContext("inline-source", sourceContext);
    var queryStep = targetStepNamed(ImportPipeline.of(importSpecification), "query-target");

    importPipeline.handleTarget(pipeline, queryStep);

    assertThat(importPipeline.findDependency(queryStep.name())).isNotNull();
    assertThat(provider.querySourceRowsCalls).isEqualTo(1);
    assertThat(provider.querySourceRowsForTargetCalls).isEqualTo(0);
  }

  @Test
  public void handle_target_queries_relationship_targets_with_relationship_endpoints() {
    var importSpecification = importSpecificationOfCurrentTest();
    var importPipeline = newImportPipeline(importSpecification);
    var startup = pipeline.apply("startup", Create.of(1L));
    var startNodeDone = pipeline.apply("start node", Create.of(2L));
    var endNodeDone = pipeline.apply("end node", Create.of(3L));
    var schema = Schema.of(Schema.Field.nullable("field_1", Schema.FieldType.STRING));
    var provider = new CapturingSourceProvider(true);
    var sourceContext = newSourceContext("inline-source", provider, schema, startup);
    importPipeline.registerDependency("__VERIFY_OR_RESET__", startup);
    importPipeline.registerDependency(
        "inline-source", pipeline.apply("source metadata", Create.of(4L)));
    importPipeline.registerDependency("a-node-target", startNodeDone);
    importPipeline.registerDependency("b-node-target", endNodeDone);
    importPipeline.registerSourceContext("inline-source", sourceContext);
    var relationshipStep =
        firstStepOfType(ImportPipeline.of(importSpecification), RelationshipTargetStep.class);

    importPipeline.handleTarget(pipeline, relationshipStep);

    assertThat(importPipeline.findDependency(relationshipStep.name())).isNotNull();
    assertThat(provider.querySourceRowsForTargetCalls).isEqualTo(1);
    assertThat(provider.capturedTargetQuerySpec.getStartNodeTarget().getName())
        .isEqualTo("a-node-target");
    assertThat(provider.capturedTargetQuerySpec.getEndNodeTarget().getName())
        .isEqualTo("b-node-target");
  }

  private Neo4jImportPipeline newImportPipeline(ImportSpecification importSpecification) {
    return new Neo4jImportPipeline(
        pipeline.getOptions(),
        "test-version",
        connectionParams(),
        new OverlayTokens(Map.of()),
        importSpecification);
  }

  private static ConnectionParams connectionParams() {
    return new ConnectionParams("bolt://localhost", null) {
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

  private static Target findTarget(ImportSpecification importSpecification, String name) {
    return importSpecification.getTargets().getAllActive().stream()
        .filter(target -> target.getName().equals(name))
        .findFirst()
        .orElseThrow();
  }

  private static CustomQueryTarget customQueryTarget(ImportSpecification importSpecification) {
    return importSpecification.getTargets().getCustomQueries().stream().findFirst().orElseThrow();
  }

  private static <T extends ImportStep> T firstStepOfType(
      ImportPipeline importPipeline, Class<T> type) {
    for (ImportStep step : importPipeline) {
      if (type.isInstance(step)) {
        return type.cast(step);
      }
    }
    throw new AssertionError("Could not find step of type " + type.getSimpleName());
  }

  private static TargetStep targetStepNamed(ImportPipeline importPipeline, String name) {
    for (ImportStep step : importPipeline) {
      if (step instanceof TargetStep targetStep && step.name().equals(name)) {
        return targetStep;
      }
    }
    throw new AssertionError("Could not find target step named " + name);
  }

  private static SourceContext newSourceContext(
      String name, SourceProvider provider, Schema schema, PCollection<?> startupDependency) {
    return new SourceContext(name, provider, schema, startupDependency);
  }

  private static final class CapturingSourceProvider implements SourceProvider {

    private final boolean pushdownSupported;
    private int querySourceRowsCalls;
    private int querySourceRowsForTargetCalls;
    private TargetQuerySpec capturedTargetQuerySpec;

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
      capturedTargetQuerySpec = targetQuerySpec;
      return Create.empty(targetQuerySpec.getSourceBeamSchema());
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> queryMetadata() {
      throw new UnsupportedOperationException("queryMetadata should not be called in this test");
    }
  }
}
