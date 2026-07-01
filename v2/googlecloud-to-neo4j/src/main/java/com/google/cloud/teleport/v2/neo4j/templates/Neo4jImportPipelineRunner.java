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

import com.google.cloud.teleport.v2.neo4j.actions.ActionDoFnFactory;
import com.google.cloud.teleport.v2.neo4j.model.connection.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.model.helpers.StepSequence;
import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetQuerySpec;
import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetQuerySpec.TargetQuerySpecBuilder;
import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;
import com.google.cloud.teleport.v2.neo4j.model.job.OverlayTokens;
import com.google.cloud.teleport.v2.neo4j.providers.SourceProvider;
import com.google.cloud.teleport.v2.neo4j.providers.SourceProviderFactory;
import com.google.cloud.teleport.v2.neo4j.telemetry.ReportedSourceType;
import com.google.cloud.teleport.v2.neo4j.transforms.Neo4jRowWriterTransform;
import com.google.cloud.teleport.v2.neo4j.transforms.VerifyOrResetDatabaseFn;
import com.google.cloud.teleport.v2.neo4j.utils.ProcessingCoder;
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
import org.neo4j.importer.v1.actions.Action;
import org.neo4j.importer.v1.pipeline.ActionStep;
import org.neo4j.importer.v1.pipeline.CustomQueryTargetStep;
import org.neo4j.importer.v1.pipeline.ImportPipeline;
import org.neo4j.importer.v1.pipeline.ImportStep;
import org.neo4j.importer.v1.pipeline.SourceStep;
import org.neo4j.importer.v1.pipeline.TargetStep;

public class Neo4jImportPipelineRunner {

  private static final String STARTUP_STEP = "__VERIFY_OR_RESET__";

  private final String templateVersion;

  private final ConnectionParams neo4jConnectionConfig;

  private final OverlayTokens overlayTokens;

  private final ImportPipeline pipelineDescription;

  private final PipelineRegistry pipelineRegistry;

  private final PipelineOptions options;

  private final StepSequence stepSequence;

  public Neo4jImportPipelineRunner(
      PipelineOptions options,
      String templateVersion,
      ConnectionParams neo4jConnectionConfig,
      OverlayTokens overlayTokens,
      ImportPipeline pipeline) {

    this.options = options;
    this.templateVersion = templateVersion;
    this.neo4jConnectionConfig = neo4jConnectionConfig;
    this.overlayTokens = overlayTokens;
    this.pipelineDescription = pipeline;
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
        new SourceContext(step, provider, schema, pipelineRegistry.findDependency(STARTUP_STEP)));
    pipelineRegistry.registerDependency(step.name(), metadata);
  }

  void handleTarget(Pipeline pipeline, TargetStep step) {
    var stepName = step.name();
    var sourceContext = pipelineRegistry.findSourceContext(step.sourceName());
    var dependencies = resolveDependencies(step);
    var sourceRows = querySourceRows(pipeline, sourceContext, step);
    PCollection<Long> targetWrite =
        sourceRows
            .apply("Wait for %s dependencies".formatted(stepName), Wait.on(dependencies))
            .setCoder(sourceRows.getCoder())
            .apply(
                "Write %s".formatted(stepName),
                new Neo4jRowWriterTransform(
                    pipelineDescription.configuration(),
                    ReportedSourceType.reportedSourceTypeOf(sourceContext.step),
                    neo4jConnectionConfig,
                    templateVersion,
                    stepSequence,
                    dependencies,
                    step))
            .apply("Completion of %s".formatted(stepName), Count.globally());

    pipelineRegistry.registerDependency(stepName, targetWrite);
  }

  PCollection<Row> querySourceRows(Pipeline pipeline, SourceContext source, TargetStep step) {
    if (step instanceof CustomQueryTargetStep) {
      return source.getOrCreateRows(pipeline);
    }

    var sourceProvider = source.provider();
    var targetQuerySpec = buildTargetQuerySpec(pipeline, source, step);
    return pipeline.apply(
        "Query %s".formatted(step.name()),
        // apply transforms, either:
        // - by pushing down changes to SQL (if source supports SQL pushdown)
        // - or by applying them to the reused source rows
        sourceProvider.querySourceRowsForTarget(targetQuerySpec));
  }

  TargetQuerySpec buildTargetQuerySpec(Pipeline pipeline, SourceContext source, TargetStep step) {
    PCollection<Row> baseRows = null;
    if (!source.provider().supportsSqlPushDown()) {
      // re-use source rows since source query cannot be modified by pushdown
      baseRows = source.getOrCreateRows(pipeline);
    }
    return new TargetQuerySpecBuilder()
        .sourceBeamSchema(source.schema())
        .nullableSourceRows(baseRows)
        .targetStep(step)
        .build();
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
    return pipelineRegistry.findSourceContext(name);
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

  private static class PipelineRegistry {

    private final Map<String, SourceContext> sources = new HashMap<>();

    private final Map<String, PCollection<?>> dependencies = new HashMap<>();

    public void registerSourceContext(String sourceName, SourceContext context) {
      sources.put(sourceName, context);
    }

    public void registerDependency(String targetName, PCollection<?> completion) {
      dependencies.put(targetName, completion);
    }

    public SourceContext findSourceContext(String name) {
      return sources.get(name);
    }

    public PCollection<?> findDependency(String name) {
      return dependencies.get(name);
    }
  }

  static final class SourceContext {

    private final SourceStep step;

    private final SourceProvider provider;

    private final Schema schema;

    private final PCollection<?> startupDependency;

    private PCollection<Row> rows;

    SourceContext(
        SourceStep step, SourceProvider provider, Schema schema, PCollection<?> startupDependency) {
      this.step = step;
      this.provider = provider;
      this.schema = schema;
      this.startupDependency = startupDependency;
    }

    public PCollection<Row> getOrCreateRows(Pipeline pipeline) {
      if (rows == null) {
        var name = step.name();
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
      return Objects.equals(step, that.step)
          && Objects.equals(provider, that.provider)
          && Objects.equals(schema, that.schema)
          && Objects.equals(startupDependency, that.startupDependency)
          && Objects.equals(rows, that.rows);
    }

    @Override
    public int hashCode() {
      return Objects.hash(step, provider, schema, startupDependency, rows);
    }

    @Override
    public String toString() {
      return "SourceContext{"
          + "step="
          + step
          + ", provider="
          + provider
          + ", schema="
          + schema
          + ", startupDependency="
          + startupDependency
          + ", rows="
          + rows
          + '}';
    }
  }
}
