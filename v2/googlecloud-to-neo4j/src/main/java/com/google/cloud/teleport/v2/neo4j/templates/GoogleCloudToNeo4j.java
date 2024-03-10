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

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.Template.AdditionalDocumentationBlock;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.neo4j.actions.ActionDoFnFactory;
import com.google.cloud.teleport.v2.neo4j.actions.ActionPreloadFactory;
import com.google.cloud.teleport.v2.neo4j.actions.preload.PreloadAction;
import com.google.cloud.teleport.v2.neo4j.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.model.InputRefactoring;
import com.google.cloud.teleport.v2.neo4j.model.InputValidator;
import com.google.cloud.teleport.v2.neo4j.model.Json;
import com.google.cloud.teleport.v2.neo4j.model.Json.ParsingResult;
import com.google.cloud.teleport.v2.neo4j.model.connection.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.model.enums.ActionExecuteAfter;
import com.google.cloud.teleport.v2.neo4j.model.enums.ArtifactType;
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.helpers.JobSpecMapper;
import com.google.cloud.teleport.v2.neo4j.model.helpers.OptionsParamsMapper;
import com.google.cloud.teleport.v2.neo4j.model.helpers.SourceQuerySpec;
import com.google.cloud.teleport.v2.neo4j.model.helpers.SourceQuerySpec.SourceQuerySpecBuilder;
import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetQuerySpec;
import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetQuerySpec.TargetQuerySpecBuilder;
import com.google.cloud.teleport.v2.neo4j.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;
import com.google.cloud.teleport.v2.neo4j.model.job.JobSpec;
import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.model.job.Source;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import com.google.cloud.teleport.v2.neo4j.options.Neo4jFlexTemplateOptions;
import com.google.cloud.teleport.v2.neo4j.providers.Provider;
import com.google.cloud.teleport.v2.neo4j.providers.ProviderFactory;
import com.google.cloud.teleport.v2.neo4j.transforms.Neo4jRowWriterTransform;
import com.google.cloud.teleport.v2.neo4j.utils.BeamBlock;
import com.google.cloud.teleport.v2.neo4j.utils.FileSystemUtils;
import com.google.cloud.teleport.v2.neo4j.utils.ModelUtils;
import com.google.cloud.teleport.v2.neo4j.utils.ProcessingCoder;
import com.google.cloud.teleport.v2.utils.SecretManagerUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
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
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/googlecloud-to-neo4j/README_Google_Cloud_to_Neo4j.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Google_Cloud_to_Neo4j",
    category = TemplateCategory.BATCH,
    displayName = "Google Cloud to Neo4j",
    description =
        "The Google Cloud to Neo4j template lets you import a dataset into a Neo4j database through a Dataflow job, "
            + "sourcing data from CSV files hosted in Google Cloud Storage buckets. It also lets you to manipulate and transform the data "
            + "at various steps of the import. You can use the template for both first-time imports and incremental imports.",
    optionsClass = Neo4jFlexTemplateOptions.class,
    flexContainerName = "googlecloud-to-neo4j",
    contactInformation = "https://support.neo4j.com/",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/google-cloud-to-neo4j",
    requirements = {
      "A running Neo4j instance",
      "A Google Cloud Storage bucket",
      "A dataset to import, in the form of CSV files",
      "A job specification file to use"
    },
    additionalDocumentation = {
      @AdditionalDocumentationBlock(
          name = "Create a job specification file",
          content = {
            "The job specification file consists of a JSON object with the following sections:\n"
                + "- `config` - global flags affecting how the import is performed.\n"
                + "- `sources` - data source definitions (relational).\n"
                + "- `targets` - data target definitions (graph: nodes/relationships).\n"
                + "- `actions` - pre/post-load actions.\n"
                + "For more information, see <a href=\"https://neo4j.com/docs/dataflow-google-cloud/job-specification/\" class=\"external\">Create a job specification file</a> in the Neo4j documentation."
          })
    },
    preview = true)
public class GoogleCloudToNeo4j {

  private static final Logger LOG = LoggerFactory.getLogger(GoogleCloudToNeo4j.class);

  private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

  private final OptionsParams optionsParams;
  private final ConnectionParams neo4jConnection;
  private final JobSpec jobSpec;
  private final Pipeline pipeline;
  private final String templateVersion;

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

    this.templateVersion = readTemplateVersion(pipelineOptions);

    String neo4jConnectionJson = readConnectionSettings(pipelineOptions);
    ParsingResult parsingResult = InputValidator.validateNeo4jConnection(neo4jConnectionJson);
    if (!parsingResult.isSuccessful()) {
      processValidations(
          "Errors found validating Neo4j connection: ",
          parsingResult.formatErrors("Could not validate connection JSON"));
    }
    this.neo4jConnection = Json.map(parsingResult, ConnectionParams.class);

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
    LOG.debug("Normalized JobSpec: {}", gson.toJson(this.jobSpec));
  }

  private static String readTemplateVersion(Neo4jFlexTemplateOptions options) {
    Map<String, String> labels = options.as(DataflowPipelineOptions.class).getLabels();
    String defaultVersion = "UNKNOWN";
    if (labels == null) {
      return defaultVersion;
    }
    return labels.getOrDefault("goog-dataflow-provided-template-version", defaultVersion);
  }

  private static String readConnectionSettings(Neo4jFlexTemplateOptions options) {
    String secretId = options.getNeo4jConnectionSecretId();
    if (StringUtils.isNotEmpty(secretId)) {
      return SecretManagerUtils.getSecret(secretId);
    }
    String uri = options.getNeo4jConnectionUri();
    try {
      return FileSystemUtils.getPathContents(uri);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Unable to read Neo4j configuration at URI %s: ", uri), e);
    }
  }

  /**
   * Runs a pipeline which reads data from various sources and writes it to Neo4j.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    Neo4jFlexTemplateOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(Neo4jFlexTemplateOptions.class);

    // Allow users to supply their own list of disabled algorithms if necessary
    if (StringUtils.isNotBlank(options.getDisabledAlgorithms())) {
      options.setDisabledAlgorithms(
          "SSLv3, RC4, DES, MD5withRSA, DH keySize < 1024, EC keySize < 224, 3DES_EDE_CBC, anon,"
              + " NULL");
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

    try (Neo4jConnection directConnect =
        new Neo4jConnection(this.neo4jConnection, this.templateVersion)) {
      boolean resetDb = jobSpec.getConfig().getResetDb();
      if (!resetDb) {
        directConnect.verifyConnectivity();
      } else {
        directConnect.resetDatabase();
      }
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
          pipeline.apply(
              String.format("Metadata for source %s", source.getName()),
              providerImpl.queryMetadata(source));
      Schema sourceBeamSchema = sourceMetadata.getSchema();
      processingQueue.addToQueue(
          ArtifactType.source, false, source.getName(), defaultActionContext, sourceMetadata);
      PCollection<Row> nullableSourceBeamRows = null;

      ////////////////////////////
      // Optimization: if single source query, reuse this PCollection rather than write it again
      boolean targetsHaveTransforms = ModelUtils.targetsHaveTransforms(jobSpec, source);
      if (!targetsHaveTransforms || !providerImpl.supportsSqlPushDown()) {
        SourceQuerySpec sourceQuerySpec =
            new SourceQuerySpecBuilder().source(source).sourceSchema(sourceBeamSchema).build();
        nullableSourceBeamRows =
            pipeline
                .apply(
                    "Query " + source.getName(), providerImpl.querySourceBeamRows(sourceQuerySpec))
                .setRowSchema(sourceBeamSchema);
      }

      String sourceName = source.getName();

      ////////////////////////////
      // Optimization: if we're not mixing nodes and edges, then run in parallel
      // For relationship updates, max workers should be max 2.  This parameter is job configurable.

      ////////////////////////////
      // No optimization possible so write nodes then edges.
      // Write node targets
      List<Target> nodeTargets =
          jobSpec.getActiveTargetsBySourceAndType(sourceName, TargetType.node);
      for (Target nodeTarget : nodeTargets) {
        TargetQuerySpec targetQuerySpec =
            new TargetQuerySpecBuilder()
                .source(source)
                .sourceBeamSchema(sourceBeamSchema)
                .nullableSourceRows(nullableSourceBeamRows)
                .target(nodeTarget)
                .build();
        String nodeStepDescription =
            nodeTarget.getSequence()
                + ": "
                + source.getName()
                + "->"
                + nodeTarget.getName()
                + " nodes";
        PCollection<Row> preInsertBeamRows =
            pipeline.apply(
                "Query " + nodeStepDescription, providerImpl.queryTargetBeamRows(targetQuerySpec));

        Neo4jRowWriterTransform targetWriterTransform =
            new Neo4jRowWriterTransform(jobSpec, neo4jConnection, templateVersion, nodeTarget);

        PCollection<Row> blockingReturn =
            preInsertBeamRows
                .apply(
                    "** Unblocking "
                        + nodeStepDescription
                        + "(after "
                        + nodeTarget.getExecuteAfter()
                        + "."
                        + nodeTarget.getExecuteAfterName()
                        + ")",
                    Wait.on(
                        processingQueue.waitOnCollection(
                            nodeTarget.getExecuteAfter(),
                            nodeTarget.getExecuteAfterName(),
                            nodeStepDescription)))
                .setCoder(preInsertBeamRows.getCoder())
                .apply("Writing " + nodeStepDescription, targetWriterTransform)
                .setCoder(preInsertBeamRows.getCoder());

        processingQueue.addToQueue(
            ArtifactType.node, false, nodeTarget.getName(), blockingReturn, preInsertBeamRows);
      }

      ////////////////////////////
      // Write relationship targets
      List<Target> relationshipTargets =
          jobSpec.getActiveTargetsBySourceAndType(sourceName, TargetType.edge);
      for (Target relationshipTarget : relationshipTargets) {
        TargetQuerySpec targetQuerySpec =
            new TargetQuerySpecBuilder()
                .source(source)
                .nullableSourceRows(nullableSourceBeamRows)
                .sourceBeamSchema(sourceBeamSchema)
                .target(relationshipTarget)
                .build();
        PCollection<Row> preInsertBeamRows;
        String relationshipStepDescription =
            relationshipTarget.getSequence()
                + ": "
                + source.getName()
                + "->"
                + relationshipTarget.getName()
                + " edges";
        if (ModelUtils.targetHasTransforms(relationshipTarget)) {
          preInsertBeamRows =
              pipeline.apply(
                  "Query " + relationshipStepDescription,
                  providerImpl.queryTargetBeamRows(targetQuerySpec));
        } else {
          preInsertBeamRows = nullableSourceBeamRows;
        }
        Neo4jRowWriterTransform targetWriterTransform =
            new Neo4jRowWriterTransform(
                jobSpec, neo4jConnection, templateVersion, relationshipTarget);

        PCollection<Row> blockingReturn =
            preInsertBeamRows
                .apply(
                    "** Unblocking "
                        + relationshipStepDescription
                        + "(after "
                        + relationshipTarget.getExecuteAfter()
                        + "."
                        + relationshipTarget.getExecuteAfterName()
                        + ")",
                    Wait.on(
                        processingQueue.waitOnCollection(
                            relationshipTarget.getExecuteAfter(),
                            relationshipTarget.getExecuteAfterName(),
                            relationshipStepDescription)))
                .setCoder(preInsertBeamRows.getCoder())
                .apply("Writing " + relationshipStepDescription, targetWriterTransform)
                .setCoder(preInsertBeamRows.getCoder());

        // serialize relationships
        processingQueue.addToQueue(
            ArtifactType.edge,
            false,
            relationshipTarget.getName(),
            blockingReturn,
            preInsertBeamRows);
      }
      ////////////////////////////
      // Custom query targets
      List<Target> customQueryTargets =
          jobSpec.getActiveTargetsBySourceAndType(sourceName, TargetType.custom_query);
      for (Target customQueryTarget : customQueryTargets) {
        String customQueryStepDescription =
            customQueryTarget.getSequence()
                + ": "
                + source.getName()
                + "->"
                + customQueryTarget.getName()
                + " (custom query)";
        Neo4jRowWriterTransform targetWriterTransform =
            new Neo4jRowWriterTransform(
                jobSpec, neo4jConnection, templateVersion, customQueryTarget);

        PCollection<Row> blockingReturn =
            nullableSourceBeamRows
                .apply(
                    "** Unblocking "
                        + customQueryStepDescription
                        + "(after "
                        + customQueryTarget.getExecuteAfter()
                        + "."
                        + customQueryTarget.getExecuteAfterName()
                        + ")",
                    Wait.on(
                        processingQueue.waitOnCollection(
                            customQueryTarget.getExecuteAfter(),
                            customQueryTarget.getExecuteAfterName(),
                            customQueryStepDescription)))
                .setCoder(nullableSourceBeamRows.getCoder())
                .apply("Writing " + customQueryStepDescription, targetWriterTransform)
                .setCoder(nullableSourceBeamRows.getCoder());

        processingQueue.addToQueue(
            ArtifactType.custom_query,
            false,
            customQueryTarget.getName(),
            blockingReturn,
            nullableSourceBeamRows);
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
      context.neo4jConnectionParams = this.neo4jConnection;
      PreloadAction actionImpl = ActionPreloadFactory.of(action, context);
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
      } else if (action.executeAfter == ActionExecuteAfter.custom_query) {
        artifactType = ArtifactType.custom_query;
      }
      LOG.info("Registering action: {}", action.name);
      // Get targeted execution context
      PCollection<Row> executionContextCollection =
          blockingQueue.getContextCollection(artifactType, action.executeAfterName);
      ActionContext context = new ActionContext();
      context.action = action;
      context.jobSpec = this.jobSpec;
      context.neo4jConnectionParams = this.neo4jConnection;
      context.templateVersion = this.templateVersion;

      // We have chosen a DoFn pattern applied to a single Integer row so that @ProcessElement
      // evaluates only once per invocation.
      // For future actions (i.e. logger) that consume upstream data context, we would use a
      // Transform pattern
      // The challenge in this case of the Transform pattern is that @FinishBundle could execute
      // many times.
      // We return <Row> from each DoFn which get rolled up into PCollection<Row> at run-time.
      // A side effect of this pattern is lots of housekeeping "**" elements in the rendered DAG.
      // Housekeeping elements are named for flow but not function.  For instance ** Setup is
      // synthetically creating a single tuple collection!
      DoFn<Integer, Row> doFnActionImpl = ActionDoFnFactory.of(context);
      PCollection<Row> blockingActionReturn =
          pipeline
              .apply("** Setup " + action.name, Create.of(1))
              .apply(
                  "** Unblocking "
                      + action.name
                      + "(after "
                      + action.executeAfter
                      + "."
                      + action.executeAfterName
                      + ")",
                  Wait.on(
                      blockingQueue.waitOnCollection(
                          action.executeAfter, action.executeAfterName, action.name)))
              .setCoder(VarIntCoder.of())
              .apply("Running " + action.name, ParDo.of(doFnActionImpl))
              .setCoder(executionContextCollection.getCoder());

      // Add action to blocking queue since action could be a dependency.
      blockingQueue.addToQueue(
          ArtifactType.action,
          action.executeAfter == ActionExecuteAfter.start,
          action.name,
          blockingActionReturn,
          executionContextCollection);
    }
  }
}
