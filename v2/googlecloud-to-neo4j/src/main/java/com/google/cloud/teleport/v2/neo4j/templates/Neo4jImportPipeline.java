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

import static org.neo4j.importer.v1.targets.TargetType.QUERY;

import com.google.cloud.teleport.v2.neo4j.actions.ActionDoFnFactory;
import com.google.cloud.teleport.v2.neo4j.model.connection.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.model.helpers.StepSequence;
import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetQuerySpec;
import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetQuerySpec.TargetQuerySpecBuilder;
import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;
import com.google.cloud.teleport.v2.neo4j.model.job.OverlayTokens;
import com.google.cloud.teleport.v2.neo4j.providers.SourceProvider;
import com.google.cloud.teleport.v2.neo4j.providers.SourceProviderFactory;
import com.google.cloud.teleport.v2.neo4j.transforms.Neo4jRowWriterTransform;
import com.google.cloud.teleport.v2.neo4j.transforms.VerifyOrResetDatabaseFn;
import com.google.cloud.teleport.v2.neo4j.utils.ProcessingCoder;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.neo4j.importer.v1.ImportSpecification;
import org.neo4j.importer.v1.actions.Action;
import org.neo4j.importer.v1.pipeline.ActionStep;
import org.neo4j.importer.v1.pipeline.ImportPipeline;
import org.neo4j.importer.v1.pipeline.ImportStep;
import org.neo4j.importer.v1.pipeline.SourceStep;
import org.neo4j.importer.v1.pipeline.TargetStep;
import org.neo4j.importer.v1.targets.NodeTarget;
import org.neo4j.importer.v1.targets.RelationshipTarget;
import org.neo4j.importer.v1.targets.Target;

public class Neo4jImportPipeline {

  private static final String STARTUP_STEP = "__VERIFY_OR_RESET__";

  private final String templateVersion;

  private final ConnectionParams neo4jConnectionConfig;

  private final OverlayTokens overlayTokens;

  private final ImportSpecification importSpecification;

  private final ImportPipeline pipelineDescription;

  private final PipelineRegistry pipelineRegistry;

  private final PipelineOptions options;

  private final StepSequence stepSequence;

  public Neo4jImportPipeline(
      PipelineOptions options,
      String templateVersion,
      ConnectionParams neo4jConnectionConfig,
      OverlayTokens overlayTokens,
      ImportSpecification importSpecification) {

    this.options = options;
    this.templateVersion = templateVersion;
    this.neo4jConnectionConfig = neo4jConnectionConfig;
    this.overlayTokens = overlayTokens;
    this.importSpecification = importSpecification;
    this.pipelineDescription = ImportPipeline.of(importSpecification);
    this.pipelineRegistry = new PipelineRegistry();
    this.stepSequence = new StepSequence();
  }

  public void run() {
    var pipeline = Pipeline.create(options);

    pipelineRegistry.registerDependency(STARTUP_STEP, checkConnectionOrResetDb(pipeline));

    pipelineDescription.forEach(
        step -> {
          if (step instanceof SourceStep) {
            handleSource(pipeline, (SourceStep) step);
          } else if (step instanceof TargetStep) {
            handleTarget(pipeline, (TargetStep) step);
          } else if (step instanceof ActionStep) {
            handleAction(pipeline, (ActionStep) step);
          }
        });

    pipeline.run();
  }

  void handleSource(Pipeline pipeline, SourceStep step) {
    var source = step.source();
    var provider = SourceProviderFactory.of(source, stepSequence);
    provider.configure(overlayTokens);

    var name = step.name();
    var metadata =
        pipeline.apply(String.format("Metadata for source %s", name), provider.queryMetadata());
    metadata =
        metadata
            .apply(
                String.format("Wait for source %s startup dependency", name),
                Wait.on(List.of(pipelineRegistry.findDependency(STARTUP_STEP))))
            .setCoder(metadata.getCoder());
    var schema = metadata.getSchema();
    pipelineRegistry.registerSourceContext(
        source.getName(),
        new SourceContext(name, provider, schema, pipelineRegistry.findDependency(STARTUP_STEP)));
    pipelineRegistry.registerDependency(step.name(), metadata);
  }

  void handleTarget(Pipeline pipeline, TargetStep step) {
    var target = findTarget(step.name());
    var source = pipelineRegistry.findSource(step.sourceName());
    var targetName = target.getName();
    var sourceRows = querySourceRows(pipeline, source, target);

    PCollection<Long> targetWrite =
        sourceRows
            .apply("Wait for " + targetName + " dependencies", Wait.on(resolveDependencies(step)))
            .setCoder(sourceRows.getCoder())
            .apply(
                "Write " + targetName,
                new Neo4jRowWriterTransform(
                    importSpecification,
                    neo4jConnectionConfig,
                    templateVersion,
                    stepSequence,
                    target))
            .apply("Completion " + targetName, Count.globally());

    pipelineRegistry.registerDependency(targetName, targetWrite);
  }

  PCollection<Row> querySourceRows(Pipeline pipeline, SourceContext source, Target target) {
    if (target.getTargetType() == QUERY) {
      return source.getOrCreateRows(pipeline);
    }
    var sourceProvider = source.provider();
    var targetQuerySpec = buildTargetQuerySpec(pipeline, source, target);
    return pipeline.apply(
        "Query " + target.getName(),
        // apply transforms, either:
        // - by pushing down changes to SQL (if source supports SQL pushdown)
        // - or by applying them to the reused source rows
        sourceProvider.querySourceRowsForTarget(targetQuerySpec));
  }

  TargetQuerySpec buildTargetQuerySpec(Pipeline pipeline, SourceContext source, Target target) {
    PCollection<Row> baseRows = null;
    if (!source.provider().supportsSqlPushDown()) {
      // re-use source rows since source query cannot be modified by pushdown
      baseRows = source.getOrCreateRows(pipeline);
    }
    var specBuilder =
        new TargetQuerySpecBuilder()
            .sourceBeamSchema(source.schema())
            .nullableSourceRows(baseRows)
            .target(target);
    if (target instanceof RelationshipTarget relationshipTarget) {
      specBuilder
          .startNodeTarget(findNodeTarget(relationshipTarget.getStartNodeReference().getName()))
          .endNodeTarget(findNodeTarget(relationshipTarget.getEndNodeReference().getName()));
    }
    return specBuilder.build();
  }

  void handleAction(Pipeline pipeline, ActionStep step) {
    var action = step.action();
    var actionName = action.getName();
    var actionRows =
        pipeline
            .apply(String.format("** Setup %s", actionName), Create.of(1))
            .apply(
                String.format("** Wait on %s dependencies", action.getStage()),
                Wait.on(resolveDependencies(step)))
            .setCoder(VarIntCoder.of())
            .apply(
                String.format("Running action %s", actionName),
                ParDo.of(ActionDoFnFactory.of(newActionContext(action))))
            .setCoder(ProcessingCoder.of());

    pipelineRegistry.registerDependency(actionName, actionRows);
  }

  List<PCollection<?>> resolveDependencies(ImportStep step) {
    var dependencies = new ArrayList<PCollection<?>>();
    dependencies.add(pipelineRegistry.findDependency(STARTUP_STEP));
    for (ImportStep dependency : step.dependencies()) {
      dependencies.add(pipelineRegistry.findDependency(dependency.name()));
    }
    return dependencies;
  }

  void registerDependency(String name, PCollection<?> dependency) {
    pipelineRegistry.registerDependency(name, dependency);
  }

  void registerSourceContext(String name, SourceContext context) {
    pipelineRegistry.registerSourceContext(name, context);
  }

  PCollection<?> findDependency(String name) {
    return pipelineRegistry.findDependency(name);
  }

  SourceContext findSourceContext(String name) {
    return pipelineRegistry.findSource(name);
  }

  private PCollection<Long> checkConnectionOrResetDb(Pipeline pipeline) {
    var resetDb = pipelineDescription.configuration().get(Boolean.class, "reset_db").orElse(false);
    var description = resetDb ? "Database reset" : "Connectivity check";

    return pipeline
        .apply("Start " + description, Create.of(1L))
        .apply(
            description,
            ParDo.of(new VerifyOrResetDatabaseFn(neo4jConnectionConfig, templateVersion, resetDb)));
  }

  private ActionContext newActionContext(Action action) {
    return new ActionContext(action, neo4jConnectionConfig, templateVersion);
  }

  private Target findTarget(String targetName) {
    return importSpecification.getTargets().getAllActive().stream()
        .filter(target -> target.getName().equals(targetName))
        .findFirst()
        .orElseThrow(
            () -> new IllegalArgumentException("Could not find active target: " + targetName));
  }

  private NodeTarget findNodeTarget(String nodeTargetName) {
    return importSpecification.getTargets().getNodes().stream()
        .filter(Target::isActive)
        .filter(target -> target.getName().equals(nodeTargetName))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Could not find active node target: " + nodeTargetName));
  }

  private static class PipelineRegistry {

    private final Map<String, SourceContext> sources = new HashMap<>();

    private final Map<String, PCollection<?>> dependencies = new HashMap<>();

    public void registerSourceContext(String sourceName, SourceContext context) {
      sources.put(sourceName, context);
    }

    public void registerDependency(String targetName, PCollection<?> completion) {
      dependencies.put(targetName, completion);
    }

    public SourceContext findSource(String name) {
      return sources.get(name);
    }

    public PCollection<?> findDependency(String name) {
      return dependencies.get(name);
    }
  }

  static final class SourceContext implements Serializable {

    private final String name;

    private final SourceProvider provider;

    private final Schema schema;

    private final PCollection<?> startupDependency;

    private PCollection<Row> rows;

    SourceContext(
        String name, SourceProvider provider, Schema schema, PCollection<?> startupDependency) {
      this.name = name;
      this.provider = provider;
      this.schema = schema;
      this.startupDependency = startupDependency;
    }

    public PCollection<Row> getOrCreateRows(Pipeline pipeline) {
      if (rows == null) {
        rows =
            pipeline
                .apply(String.format("Query for source %s", name), provider.querySourceRows(schema))
                .apply(
                    String.format("Wait for source %s startup dependency", name),
                    Wait.on(List.of(startupDependency)))
                .setRowSchema(schema);
      }
      return rows;
    }

    public SourceProvider provider() {
      return provider;
    }

    public Schema schema() {
      return schema;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof SourceContext that)) {
        return false;
      }
      return Objects.equals(name, that.name)
          && Objects.equals(provider, that.provider)
          && Objects.equals(schema, that.schema);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, provider, schema);
    }

    @Override
    public String toString() {
      return "SourceContext{"
          + "name='"
          + name
          + '\''
          + ", provider="
          + provider
          + ", schema="
          + schema
          + '}';
    }
  }
}
