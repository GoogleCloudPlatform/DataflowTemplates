/*
 * Copyright (C) 2021 Google LLC
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

import com.google.cloud.teleport.v2.neo4j.actions.ActionBeamFactory;
import com.google.cloud.teleport.v2.neo4j.actions.ActionFactory;
import com.google.cloud.teleport.v2.neo4j.actions.preload.PreloadAction;
import com.google.cloud.teleport.v2.neo4j.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.model.InputRefactoring;
import com.google.cloud.teleport.v2.neo4j.model.InputValidator;
import com.google.cloud.teleport.v2.neo4j.model.connection.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.model.enums.ActionExecuteAfter;
import com.google.cloud.teleport.v2.neo4j.model.enums.ArtifactType;
import com.google.cloud.teleport.v2.neo4j.model.helpers.JobSpecMapper;
import com.google.cloud.teleport.v2.neo4j.model.helpers.OptionsParamsMapper;
import com.google.cloud.teleport.v2.neo4j.model.helpers.SourceQuerySpec;
import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetQuerySpec;
import com.google.cloud.teleport.v2.neo4j.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;
import com.google.cloud.teleport.v2.neo4j.model.job.JobSpec;
import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.model.job.Source;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import com.google.cloud.teleport.v2.neo4j.options.Neo4jFlexTemplateOptions;
import com.google.cloud.teleport.v2.neo4j.providers.Provider;
import com.google.cloud.teleport.v2.neo4j.providers.ProviderFactory;
import com.google.cloud.teleport.v2.neo4j.transforms.GcsLogTransform;
import com.google.cloud.teleport.v2.neo4j.transforms.Neo4jRowWriterTransform;
import com.google.cloud.teleport.v2.neo4j.utils.BeamBlock;
import com.google.cloud.teleport.v2.neo4j.utils.ModelUtils;
import com.google.cloud.teleport.v2.neo4j.utils.ProcessingCoder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dataflow template which reads Google Cloud data (Text, BigQuery) and writes it to Neo4j.
 *
 * <p>In case of BigQuery, the source data can be either a table or a SQL query.
 */
public class GoogleCloudToNeo4j {

  private static final Logger LOG = LoggerFactory.getLogger(GoogleCloudToNeo4j.class);

  private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

  private final OptionsParams optionsParams;
  private final ConnectionParams neo4jConnection;
  private final JobSpec jobSpec;
  private final Pipeline pipeline;

  /**
   * Main class for template. Initializes job using run-time on pipelineOptions.
   *
   * @param pipelineOptions framework supplied arguments
   */
  public GoogleCloudToNeo4j(Neo4jFlexTemplateOptions pipelineOptions) {

    ////////////////////////////
    // Job name gets a date on it when running within the container, but not with DirectRunner
    // final String jobName = pipelineOptions.getJobName() + "-" + System.currentTimeMillis();
    // pipelineOptions.setJobName(jobName);

    // Set pipeline options
    this.pipeline = Pipeline.create(pipelineOptions);
    FileSystems.setDefaultPipelineOptions(pipelineOptions);
    this.optionsParams = OptionsParamsMapper.fromPipelineOptions(pipelineOptions);

    // Validate pipeline
    processValidations(
        "Errors found validating pipeline options: ",
        InputValidator.validateNeo4jPipelineOptions(pipelineOptions));

    this.neo4jConnection = new ConnectionParams(pipelineOptions.getNeo4jConnectionUri());

    // Validate connection
    processValidations(
        "Errors found validating Neo4j connection: ",
        InputValidator.validateNeo4jConnection(this.neo4jConnection));

    this.jobSpec = JobSpecMapper.fromUri(pipelineOptions.getJobSpecUri());

    // Validate job spec
    processValidations(
        "Errors found validating job specification: ",
        InputValidator.validateJobSpec(this.jobSpec));

    ///////////////////////////////////
    // Refactor job spec
    InputRefactoring inputRefactoring = new InputRefactoring(this.optionsParams);

    // Variable substitution
    inputRefactoring.refactorJobSpec(this.jobSpec);

    // Optimizations
    inputRefactoring.optimizeJobSpec(this.jobSpec);

    // Source specific validations
    for (Source source : jobSpec.getSourceList()) {
      // get provider implementation for source
      Provider providerImpl = ProviderFactory.of(source.getSourceType());
      providerImpl.configure(optionsParams, jobSpec);
    }

    // Output debug log spec
    LOG.info("JobSpec: {}{}", System.lineSeparator(), gson.toJson(this.jobSpec));
  }

  /**
   * Runs a pipeline which reads data from various sources and writes it to Neo4j.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    Neo4jFlexTemplateOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(Neo4jFlexTemplateOptions.class);

    // Allow users to supply their own list of disabled algorithms if necessary
    if (StringUtils.isNotBlank(options.getDisabledAlgorithms())) {
      options.setDisabledAlgorithms(
          "SSLv3, RC4, DES, MD5withRSA, DH keySize < 1024, EC keySize < 224, 3DES_EDE_CBC, anon, NULL");
    }

    LOG.info("Job: {}", options.getJobSpecUri());
    GoogleCloudToNeo4j template = new GoogleCloudToNeo4j(options);
    template.run();
  }

  /** Raises RuntimeExceptions for validation errors. */
  private void processValidations(String description, List<String> validationMessages) {
    StringBuilder sb = new StringBuilder();
    if (!validationMessages.isEmpty()) {
      for (String msg : validationMessages) {
        sb.append(msg);
        sb.append(System.lineSeparator());
      }
      throw new RuntimeException(description + " " + sb);
    }
  }

  public void run() {

    ////////////////////////////
    // Reset db
    if (jobSpec.getConfig().getResetDb()) {
      Neo4jConnection directConnect = new Neo4jConnection(this.neo4jConnection);
      directConnect.resetDatabase();
    }
    ////////////////////////////
    // Run preload actions
    runPreloadActions(jobSpec.getPreloadActions());

    ////////////////////////////
    // If an action transformation has no upstream PCollection, it will use this default context
    PCollection<Row> defaultActionContext =
        pipeline.apply(
            "Default Context",
            Create.empty(TypeDescriptor.of(Row.class)).withCoder(ProcessingCoder.of()));

    // Creating serialization handle
    BeamBlock processingQueue = new BeamBlock(defaultActionContext);

    ////////////////////////////
    // Process sources
    for (Source source : jobSpec.getSourceList()) {

      // get provider implementation for source
      Provider providerImpl = ProviderFactory.of(source.getSourceType());
      providerImpl.configure(optionsParams, jobSpec);
      PCollection<Row> sourceMetadata =
          pipeline.apply("Source metadata", providerImpl.queryMetadata(source));
      Schema sourceBeamSchema = sourceMetadata.getSchema();
      processingQueue.addToQueue(
          ArtifactType.source, false, source.getName(), defaultActionContext, sourceMetadata);
      PCollection<Row> nullableSourceBeamRows = null;

      ////////////////////////////
      // Optimization: if single source query, reuse this PCollection rather than write it again
      boolean targetsHaveTransforms = ModelUtils.targetsHaveTransforms(jobSpec, source);
      if (!targetsHaveTransforms || !providerImpl.supportsSqlPushDown()) {
        SourceQuerySpec sourceQuerySpec =
            SourceQuerySpec.builder().source(source).sourceSchema(sourceBeamSchema).build();
        nullableSourceBeamRows =
            pipeline
                .apply("Common Query", providerImpl.querySourceBeamRows(sourceQuerySpec))
                .setRowSchema(sourceBeamSchema);
      }

      ////////////////////////////
      // Optimization: if we're not mixing nodes and edges, then run in parallel
      // For relationship updates, max workers should be max 2.  This parameter is job configurable.

      ////////////////////////////
      // No optimization possible so write nodes then edges.
      // Write node targets
      List<Target> nodeTargets = jobSpec.getActiveNodeTargetsBySource(source.getName());
      for (Target target : nodeTargets) {
        TargetQuerySpec targetQuerySpec =
            TargetQuerySpec.builder()
                .source(source)
                .sourceBeamSchema(sourceBeamSchema)
                .nullableSourceRows(nullableSourceBeamRows)
                .target(target)
                .build();
        PCollection<Row> preInsertBeamRows;
        if (ModelUtils.targetHasTransforms(target)) {
          preInsertBeamRows =
              pipeline.apply(
                  target.getSequence() + ": Nodes query " + target.getName(),
                  providerImpl.queryTargetBeamRows(targetQuerySpec));
        } else {
          preInsertBeamRows = nullableSourceBeamRows;
        }
        Neo4jRowWriterTransform targetWriterTransform =
            new Neo4jRowWriterTransform(jobSpec, neo4jConnection, target);

        PCollection<Row> emptyReturn = null;
        // We don't need to add an explicit queue step under these conditions.
        // Implicitly, the job will queue until after its source is complete.
        if (target.getExecuteAfter() == ActionExecuteAfter.start
            || target.getExecuteAfter() == ActionExecuteAfter.sources
            || target.getExecuteAfter() == ActionExecuteAfter.async) {
          emptyReturn =
              preInsertBeamRows.apply(
                  target.getSequence() + ": Writing Neo4j " + target.getName(),
                  targetWriterTransform);
        } else {
          emptyReturn =
              preInsertBeamRows
                  .apply(
                      "Executing " + target.getExecuteAfter() + " (" + target.getName() + ")",
                      Wait.on(
                          processingQueue.waitOnCollection(
                              target.getExecuteAfter(),
                              target.getExecuteAfterName(),
                              source.getName() + " nodes")))
                  .setCoder(preInsertBeamRows.getCoder())
                  .apply(
                      target.getSequence() + ": Writing Neo4j " + target.getName(),
                      targetWriterTransform);
        }
        if (!StringUtils.isEmpty(jobSpec.getConfig().getAuditGsUri())) {
          GcsLogTransform logTransform = new GcsLogTransform(jobSpec, target);
          preInsertBeamRows.apply(
              target.getSequence() + ": Logging " + target.getName(), logTransform);
        }
        processingQueue.addToQueue(
            ArtifactType.node, false, target.getName(), emptyReturn, preInsertBeamRows);
      }

      ////////////////////////////
      // Write relationship targets
      List<Target> relationshipTargets =
          jobSpec.getActiveRelationshipTargetsBySource(source.getName());
      for (Target target : relationshipTargets) {
        TargetQuerySpec targetQuerySpec =
            TargetQuerySpec.builder()
                .source(source)
                .nullableSourceRows(nullableSourceBeamRows)
                .sourceBeamSchema(sourceBeamSchema)
                .target(target)
                .build();
        PCollection<Row> preInsertBeamRows;
        if (ModelUtils.targetHasTransforms(target)) {
          preInsertBeamRows =
              pipeline.apply(
                  target.getSequence() + ": Edges query " + target.getName(),
                  providerImpl.queryTargetBeamRows(targetQuerySpec));
        } else {
          preInsertBeamRows = nullableSourceBeamRows;
        }
        Neo4jRowWriterTransform targetWriterTransform =
            new Neo4jRowWriterTransform(jobSpec, neo4jConnection, target);
        PCollection<Row> emptyReturn = null;
        // We don't need to add an explicit queue step under these conditions
        // Implicitly, the job will queue until after its source is complete.
        if (target.getExecuteAfter() == ActionExecuteAfter.start
            || target.getExecuteAfter() == ActionExecuteAfter.sources
            || target.getExecuteAfter() == ActionExecuteAfter.async) {

          emptyReturn =
              preInsertBeamRows.apply(
                  target.getSequence() + ": Writing Neo4j " + target.getName(),
                  targetWriterTransform);

        } else {
          emptyReturn =
              preInsertBeamRows
                  .apply(
                      "Executing " + target.getExecuteAfter() + " (" + target.getName() + ")",
                      Wait.on(
                          processingQueue.waitOnCollection(
                              target.getExecuteAfter(),
                              target.getExecuteAfterName(),
                              source.getName() + " nodes")))
                  .setCoder(preInsertBeamRows.getCoder())
                  .apply(
                      target.getSequence() + ": Writing Neo4j " + target.getName(),
                      targetWriterTransform);
        }
        if (!StringUtils.isEmpty(jobSpec.getConfig().getAuditGsUri())) {
          GcsLogTransform logTransform = new GcsLogTransform(jobSpec, target);
          preInsertBeamRows.apply(
              target.getSequence() + ": Logging " + target.getName(), logTransform);
        }
        // serialize relationships
        processingQueue.addToQueue(
            ArtifactType.edge, false, target.getName(), emptyReturn, preInsertBeamRows);
      }
    }

    ////////////////////////////
    // Process actions (first pass)
    runBeamActions(jobSpec.getPostloadActions(), processingQueue);

    // For a Dataflow Flex Template, do NOT waitUntilFinish().
    pipeline.run();
  }

  private void runPreloadActions(List<Action> actions) {
    for (Action action : actions) {
      LOG.info("Executing preload action: {}", action.name);
      // Get targeted execution context
      ActionContext context = new ActionContext();
      context.jobSpec = this.jobSpec;
      context.neo4jConnection = this.neo4jConnection;
      PreloadAction actionImpl = ActionFactory.of(action, context);
      List<String> msgs = actionImpl.execute();
      for (String msg : msgs) {
        LOG.info("Preload action {}: {}", action.name, msg);
      }
    }
  }

  private void runBeamActions(List<Action> actions, BeamBlock blockingQueue) {
    for (Action action : actions) {
      // LOG.info("Registering action: " + gson.toJson(action));
      ArtifactType artifactType = ArtifactType.action;
      if (action.executeAfter == ActionExecuteAfter.source) {
        artifactType = ArtifactType.source;
      } else if (action.executeAfter == ActionExecuteAfter.node) {
        artifactType = ArtifactType.node;
      } else if (action.executeAfter == ActionExecuteAfter.edge) {
        artifactType = ArtifactType.edge;
      }
      LOG.info("Executing delayed action: {}", action.name);
      // Get targeted execution context
      PCollection<Row> executionContext =
          blockingQueue.getContextCollection(artifactType, action.executeAfterName);
      ActionContext context = new ActionContext();
      context.dataContext = executionContext;
      context.jobSpec = this.jobSpec;
      context.neo4jConnection = this.neo4jConnection;
      PTransform<PCollection<Row>, PCollection<Row>> actionImpl =
          ActionBeamFactory.of(action, context);
      // Action execution context will be rendered as "Default Context"
      PCollection<Row> actionTransformed =
          executionContext
              .apply(
                  "Executing " + action.executeAfter + " (" + action.name + ")",
                  Wait.on(
                      blockingQueue.waitOnCollection(
                          action.executeAfter, action.executeAfterName, action.name)))
              .setCoder(executionContext.getCoder())
              .apply("Action " + action.name, actionImpl);
      blockingQueue.addToQueue(
          ArtifactType.action,
          action.executeAfter == ActionExecuteAfter.start,
          action.name,
          actionTransformed,
          executionContext);
    }
  }
}
