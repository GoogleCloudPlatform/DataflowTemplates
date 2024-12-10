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

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.Template.AdditionalDocumentationBlock;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.neo4j.actions.ActionDoFnFactory;
import com.google.cloud.teleport.v2.neo4j.actions.ActionPreloadFactory;
import com.google.cloud.teleport.v2.neo4j.actions.preload.PreloadAction;
import com.google.cloud.teleport.v2.neo4j.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.model.InputValidator;
import com.google.cloud.teleport.v2.neo4j.model.Json;
import com.google.cloud.teleport.v2.neo4j.model.Json.ParsingResult;
import com.google.cloud.teleport.v2.neo4j.model.connection.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.model.enums.ArtifactType;
import com.google.cloud.teleport.v2.neo4j.model.helpers.JobSpecMapper;
import com.google.cloud.teleport.v2.neo4j.model.helpers.OptionsParamsMapper;
import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetQuerySpec;
import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetQuerySpec.TargetQuerySpecBuilder;
import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetSequence;
import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;
import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.options.Neo4jFlexTemplateOptions;
import com.google.cloud.teleport.v2.neo4j.providers.Provider;
import com.google.cloud.teleport.v2.neo4j.providers.ProviderFactory;
import com.google.cloud.teleport.v2.neo4j.transforms.Neo4jRowWriterTransform;
import com.google.cloud.teleport.v2.neo4j.utils.BeamBlock;
import com.google.cloud.teleport.v2.neo4j.utils.FileSystemUtils;
import com.google.cloud.teleport.v2.neo4j.utils.ModelUtils;
import com.google.cloud.teleport.v2.neo4j.utils.ProcessingCoder;
import com.google.cloud.teleport.v2.utils.SecretManagerUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.neo4j.importer.v1.Configuration;
import org.neo4j.importer.v1.ImportSpecification;
import org.neo4j.importer.v1.actions.Action;
import org.neo4j.importer.v1.actions.ActionStage;
import org.neo4j.importer.v1.sources.Source;
import org.neo4j.importer.v1.targets.CustomQueryTarget;
import org.neo4j.importer.v1.targets.NodeTarget;
import org.neo4j.importer.v1.targets.RelationshipTarget;
import org.neo4j.importer.v1.targets.Target;
import org.neo4j.importer.v1.targets.TargetType;
import org.neo4j.importer.v1.targets.Targets;
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
                + "- `targets` - data target definitions (graph: nodes/relationships/custom queries).\n"
                + "- `actions` - pre/post-load actions.\n"
                + "For more information, see <a href=\"https://neo4j.com/docs/dataflow-google-cloud/job-specification/\" class=\"external\">Create a job specification file</a> in the Neo4j documentation."
          })
    },
    preview = true)
public class GoogleCloudToNeo4j {

  private static final Logger LOG = LoggerFactory.getLogger(GoogleCloudToNeo4j.class);

  private final OptionsParams optionsParams;
  private final ConnectionParams neo4jConnection;
  private final ImportSpecification importSpecification;
  private final Configuration globalSettings;
  private final Pipeline pipeline;
  private final String templateVersion;
  private final TargetSequence targetSequence = new TargetSequence();

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

    this.importSpecification = JobSpecMapper.parse(pipelineOptions.getJobSpecUri(), optionsParams);
    globalSettings = importSpecification.getConfiguration();

    ///////////////////////////////////

    // Source specific validations
    for (Source source : importSpecification.getSources()) {
      // get provider implementation for source
      Provider providerImpl = ProviderFactory.of(source, targetSequence);
      providerImpl.configure(optionsParams);
    }
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
    if (StringUtils.isBlank(options.getDisabledAlgorithms())) {
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
      boolean resetDb = globalSettings.get(Boolean.class, "reset_db").orElse(false);
      if (!resetDb) {
        directConnect.verifyConnectivity();
      } else {
        directConnect.resetDatabase();
      }
    }

    ////////////////////////////
    // If an action transformation has no upstream PCollection, it will use this default context
    PCollection<Row> defaultActionContext =
        pipeline.apply(
            "Default Context",
            Create.empty(TypeDescriptor.of(Row.class)).withCoder(ProcessingCoder.of()));

    var processingQueue = new BeamBlock(defaultActionContext);

    runPreloadActions(findActionsByStage(ActionStage.START).collect(toList()));

    Map<ActionStage, List<PCollection<?>>> preActionRows =
        findActionsByStages(
                Set.of(
                    ActionStage.PRE_NODES, ActionStage.PRE_RELATIONSHIPS, ActionStage.PRE_QUERIES))
            .map(action -> Map.entry(action.getStage(), runAction(action, defaultActionContext)))
            .collect(
                groupingBy(
                    Entry::getKey, mapping(Entry::getValue, Collectors.<PCollection<?>>toList())));
    var sourceRows = new ArrayList<PCollection<?>>(importSpecification.getSources().size());
    var targetRows = new HashMap<TargetType, List<PCollection<?>>>(targetCount());
    var allActiveNodeTargets =
        importSpecification.getTargets().getNodes().stream()
            .filter(Target::isActive)
            .collect(toList());

    ////////////////////////////
    // Process sources
    for (var source : importSpecification.getSources()) {

      // get provider implementation for source
      Provider provider = ProviderFactory.of(source, targetSequence);
      provider.configure(optionsParams);
      PCollection<Row> sourceMetadata =
          pipeline.apply(
              String.format("Metadata for source %s", source.getName()), provider.queryMetadata());
      sourceRows.add(sourceMetadata);
      Schema sourceBeamSchema = sourceMetadata.getSchema();
      processingQueue.addToQueue(
          ArtifactType.source, false, source.getName(), defaultActionContext, sourceMetadata);
      PCollection<Row> nullableSourceBeamRows = null;

      ////////////////////////////
      // Optimization: if single source query, reuse this PCollection rather than write it again
      boolean targetsHaveTransforms = ModelUtils.targetsHaveTransforms(importSpecification, source);
      if (!targetsHaveTransforms || !provider.supportsSqlPushDown()) {
        nullableSourceBeamRows =
            pipeline
                .apply("Query " + source.getName(), provider.querySourceBeamRows(sourceBeamSchema))
                .setRowSchema(sourceBeamSchema);
      }

      String sourceName = source.getName();

      ////////////////////////////
      // Optimization: if we're not mixing nodes and edges, then run in parallel
      // For relationship updates, max workers should be max 2.  This parameter is job configurable.

      ////////////////////////////
      // No optimization possible so write nodes then edges.
      // Write node targets
      List<NodeTarget> nodeTargets =
          getActiveTargetsBySourceAndType(importSpecification, sourceName, TargetType.NODE);
      for (NodeTarget target : nodeTargets) {
        TargetQuerySpec targetQuerySpec =
            new TargetQuerySpecBuilder()
                .sourceBeamSchema(sourceBeamSchema)
                .nullableSourceRows(nullableSourceBeamRows)
                .target(target)
                .build();
        String nodeStepDescription =
            targetSequence.getSequenceNumber(target)
                + ": "
                + source.getName()
                + "->"
                + target.getName()
                + " nodes";
        PCollection<Row> preInsertBeamRows =
            pipeline.apply(
                "Query " + nodeStepDescription, provider.queryTargetBeamRows(targetQuerySpec));

        List<PCollection<?>> dependencies =
            new ArrayList<>(preActionRows.getOrDefault(ActionStage.PRE_NODES, List.of()));
        dependencies.add(
            processingQueue.waitOnCollections(target.getDependencies(), nodeStepDescription));

        PCollection<Row> blockingReturn =
            preInsertBeamRows
                .apply(
                    "** Unblocking "
                        + nodeStepDescription
                        + "(after "
                        + String.join(", ", target.getDependencies())
                        + " and pre-nodes actions)",
                    Wait.on(dependencies))
                .setCoder(preInsertBeamRows.getCoder())
                .apply(
                    "Writing " + nodeStepDescription,
                    new Neo4jRowWriterTransform(
                        importSpecification,
                        neo4jConnection,
                        templateVersion,
                        targetSequence,
                        target))
                .setCoder(preInsertBeamRows.getCoder());

        targetRows
            .computeIfAbsent(TargetType.NODE, (type) -> new ArrayList<>(nodeTargets.size()))
            .add(blockingReturn);

        processingQueue.addToQueue(
            ArtifactType.node, false, target.getName(), blockingReturn, preInsertBeamRows);
      }

      ////////////////////////////
      // Write relationship targets
      List<RelationshipTarget> relationshipTargets =
          getActiveTargetsBySourceAndType(importSpecification, sourceName, TargetType.RELATIONSHIP);
      for (var target : relationshipTargets) {
        var targetQuerySpec =
            new TargetQuerySpecBuilder()
                .nullableSourceRows(nullableSourceBeamRows)
                .sourceBeamSchema(sourceBeamSchema)
                .target(target)
                .startNodeTarget(
                    findNodeTargetByName(allActiveNodeTargets, target.getStartNodeReference()))
                .endNodeTarget(
                    findNodeTargetByName(allActiveNodeTargets, target.getEndNodeReference()))
                .build();
        PCollection<Row> preInsertBeamRows;
        String relationshipStepDescription =
            targetSequence.getSequenceNumber(target)
                + ": "
                + source.getName()
                + "->"
                + target.getName()
                + " edges";
        if (ModelUtils.targetHasTransforms(target)) {
          preInsertBeamRows =
              pipeline.apply(
                  "Query " + relationshipStepDescription,
                  provider.queryTargetBeamRows(targetQuerySpec));
        } else {
          preInsertBeamRows = nullableSourceBeamRows;
        }

        List<PCollection<?>> dependencies =
            new ArrayList<>(preActionRows.getOrDefault(ActionStage.PRE_RELATIONSHIPS, List.of()));
        Set<String> dependencyNames = new LinkedHashSet<>(target.getDependencies());
        dependencyNames.add(target.getStartNodeReference());
        dependencyNames.add(target.getEndNodeReference());
        dependencies.add(
            processingQueue.waitOnCollections(dependencyNames, relationshipStepDescription));

        PCollection<Row> blockingReturn =
            preInsertBeamRows
                .apply(
                    "** Unblocking "
                        + relationshipStepDescription
                        + "(after "
                        + String.join(", ", dependencyNames)
                        + " and pre-relationships actions)",
                    Wait.on(dependencies))
                .setCoder(preInsertBeamRows.getCoder())
                .apply(
                    "Writing " + relationshipStepDescription,
                    new Neo4jRowWriterTransform(
                        importSpecification,
                        neo4jConnection,
                        templateVersion,
                        targetSequence,
                        target))
                .setCoder(preInsertBeamRows.getCoder());

        targetRows
            .computeIfAbsent(
                TargetType.RELATIONSHIP, (type) -> new ArrayList<>(relationshipTargets.size()))
            .add(blockingReturn);
        // serialize relationships
        processingQueue.addToQueue(
            ArtifactType.edge, false, target.getName(), blockingReturn, preInsertBeamRows);
      }
      ////////////////////////////
      // Custom query targets
      List<CustomQueryTarget> customQueryTargets =
          getActiveTargetsBySourceAndType(importSpecification, sourceName, TargetType.QUERY);
      for (Target target : customQueryTargets) {
        String customQueryStepDescription =
            targetSequence.getSequenceNumber(target)
                + ": "
                + source.getName()
                + "->"
                + target.getName()
                + " (custom query)";

        List<PCollection<?>> dependencies =
            new ArrayList<>(preActionRows.getOrDefault(ActionStage.PRE_QUERIES, List.of()));
        dependencies.add(
            processingQueue.waitOnCollections(
                target.getDependencies(), customQueryStepDescription));

        PCollection<Row> blockingReturn =
            nullableSourceBeamRows
                .apply(
                    "** Unblocking "
                        + customQueryStepDescription
                        + "(after "
                        + String.join(", ", target.getDependencies())
                        + ")",
                    Wait.on(dependencies))
                .setCoder(nullableSourceBeamRows.getCoder())
                .apply(
                    "Writing " + customQueryStepDescription,
                    new Neo4jRowWriterTransform(
                        importSpecification,
                        neo4jConnection,
                        templateVersion,
                        targetSequence,
                        target))
                .setCoder(nullableSourceBeamRows.getCoder());

        targetRows
            .computeIfAbsent(TargetType.QUERY, (type) -> new ArrayList<>(customQueryTargets.size()))
            .add(blockingReturn);
        processingQueue.addToQueue(
            ArtifactType.custom_query,
            false,
            target.getName(),
            blockingReturn,
            nullableSourceBeamRows);
      }
    }

    // Process POST-* actions, gather outputs and run END actions
    List<PCollection<?>> endActionDependencies =
        findActionsByStage(ActionStage.POST_SOURCES)
            .map(action -> runAction(action, defaultActionContext, sourceRows))
            .collect(Collectors.toCollection(ArrayList::new));
    endActionDependencies.addAll(
        findActionsByStage(ActionStage.POST_NODES)
            .map(
                action ->
                    runAction(
                        action,
                        defaultActionContext,
                        targetRows.getOrDefault(TargetType.NODE, List.of())))
            .collect(toList()));
    endActionDependencies.addAll(
        findActionsByStage(ActionStage.POST_RELATIONSHIPS)
            .map(
                action ->
                    runAction(
                        action,
                        defaultActionContext,
                        targetRows.getOrDefault(TargetType.RELATIONSHIP, List.of())))
            .collect(toList()));
    endActionDependencies.addAll(
        findActionsByStage(ActionStage.POST_QUERIES)
            .map(
                action ->
                    runAction(
                        action,
                        defaultActionContext,
                        targetRows.getOrDefault(TargetType.QUERY, List.of())))
            .collect(toList()));
    findActionsByStage(ActionStage.END)
        .map(action -> runAction(action, defaultActionContext, endActionDependencies))
        .forEach(GoogleCloudToNeo4j::noOp);

    // For a Dataflow Flex Template, do NOT waitUntilFinish().
    pipeline.run();
  }

  private PCollection<Row> runAction(Action action, PCollection<Row> defaultActionContext) {
    return runAction(action, defaultActionContext, List.of());
  }

  private PCollection<Row> runAction(
      Action action, PCollection<Row> defaultActionContext, List<PCollection<?>> dependencies) {
    var actionName = action.getName();
    return pipeline
        .apply(String.format("** Setup %s", actionName), Create.of(1))
        .apply(
            String.format("** Wait on %s dependencies", action.getStage()), Wait.on(dependencies))
        .setCoder(VarIntCoder.of())
        .apply(
            String.format("Running action %s", actionName),
            ParDo.of(ActionDoFnFactory.of(newActionContext(action))))
        .setCoder(defaultActionContext.getCoder());
  }

  private Stream<Action> findActionsByStage(ActionStage stage) {
    return findActionsByStages(Set.of(stage));
  }

  private Stream<Action> findActionsByStages(Set<ActionStage> stages) {
    return importSpecification.getActions().stream()
        .filter(action -> stages.contains(action.getStage()));
  }

  private void runPreloadActions(List<Action> actions) {
    for (Action action : actions) {
      LOG.debug("Executing START action: {}", action.getName());
      // Get targeted execution context
      ActionContext context = new ActionContext(action, neo4jConnection, templateVersion);
      PreloadAction actionImpl = ActionPreloadFactory.of(action, context);
      List<String> msgs = actionImpl.execute();
      for (String msg : msgs) {
        LOG.info("START action {} output: {}", action.getName(), msg);
      }
    }
  }

  @NotNull
  private ActionContext newActionContext(Action action) {
    return new ActionContext(action, this.neo4jConnection, this.templateVersion);
  }

  private static NodeTarget findNodeTargetByName(List<NodeTarget> nodes, String reference) {
    return nodes.stream()
        .filter(target -> reference.equals(target.getName()))
        .findFirst()
        .orElseThrow(
            () -> new IllegalArgumentException("Could not find active node target: " + reference));
  }

  @SuppressWarnings("unchecked")
  private <T extends Target> List<T> getActiveTargetsBySourceAndType(
      ImportSpecification importSpecification, String sourceName, TargetType targetType) {
    Targets targets = importSpecification.getTargets();
    return targets.getAll().stream()
        .filter(
            target ->
                target.getTargetType() == targetType
                    && target.isActive()
                    && sourceName.equals(target.getSource()))
        .map(target -> (T) target)
        .collect(toList());
  }

  private static <T> void noOp(T item) {}

  private int targetCount() {
    var targets = this.importSpecification.getTargets();
    return targets.getNodes().size()
        + targets.getRelationships().size()
        + targets.getCustomQueries().size();
  }
}
