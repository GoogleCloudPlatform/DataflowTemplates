/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.sqllauncher;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.sqllauncher.ParameterTranslator.Parameter;
import com.google.cloud.teleport.v2.sqllauncher.ParameterTranslator.ParameterMode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.zetasql.SqlException;
import com.google.zetasql.Value;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.extensions.sql.impl.ParseException;
import org.apache.beam.sdk.extensions.sql.impl.SqlConversionException;
import org.apache.beam.sdk.extensions.sql.meta.provider.InvalidTableException;
import org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog.DataCatalogPipelineOptions;
import org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog.DataCatalogTableProvider;
import org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner;
import org.apache.beam.sdk.extensions.sql.zetasql.ZetaSqlException;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Pipeline to execute Dataflow SQL query. */
@Template(
    name = "SQL_Launcher",
    category = TemplateCategory.BATCH,
    displayName = "SQL Launcher",
    description =
        "The SQL Launcher pipeline allows customers to run a Dataflow SQL query, and save the results to BigQuery or Pub/Sub.",
    optionsClass = DataflowSqlLauncherOptions.class,
    flexContainerName = "sql-launcher",
    documentation = "https://cloud.google.com/dataflow/docs/guides/sql/dataflow-sql-intro",
    contactInformation = "https://cloud.google.com/support")
public class DataflowSqlLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(DataflowSqlLauncher.class);

  private static final int BAD_TEMPLATE_ARGUMENTS_EXIT_CODE = 42;

  // Not technically in the Java language spec, so we do it explicitly here
  private static final int UNCAUGHT_EXCEPTION_EXIT_CODE = 1;

  @VisibleForTesting static final String EXIT_DELAY_PROPERTY = "dataflow.sqllauncher.exitDelay";

  private static final long DEFAULT_EXIT_DELAY_MILLIS = 2 * 60 * 1000;

  public static void main(String[] args) throws Exception {
    UncaughtExceptionLogger.register();

    DataflowSqlLauncherOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowSqlLauncherOptions.class);
    run(options);
  }

  public static void run(DataflowSqlLauncherOptions options) throws IOException {

    Pipeline pipeline = buildPipeline(options);
    if (!options.getDryRun()) {
      try {
        pipeline.run();
      } catch (IllegalStateException e) {
        throw new BadTemplateArgumentsException(
            "Query performs bad state operation"
                + (e.getMessage() == null ? "" : ": " + e.getMessage()),
            e);
      }
    }
  }

  @VisibleForTesting
  static Pipeline buildPipeline(DataflowSqlLauncherOptions options) throws IOException {
    try {
      return buildPipelineOrThrow(options);
    } catch (InvalidSinkException e) {
      throw new BadTemplateArgumentsException(
          "Invalid output specification" + (e.getMessage() == null ? "" : ": " + e.getMessage()),
          e);
    } catch (InvalidTableException e) {
      throw new BadTemplateArgumentsException(
          "Invalid table specification in Data Catalog"
              + (e.getMessage() == null ? "" : ": " + e.getMessage()),
          e);
    } catch (ParseException | SqlException | ZetaSqlException e) {
      throw new BadTemplateArgumentsException(
          "Error in SQL query" + (e.getMessage() == null ? "" : ": " + e.getMessage()), e);
    } catch (UnsupportedOperationException | SqlConversionException e) {
      throw new BadTemplateArgumentsException(
          "Query uses unsupported SQL features"
              + (e.getMessage() == null ? "" : ": " + e.getMessage()),
          e);
    }
  }

  /** Tries to build a pipeline from the options, otherwise throwing a relevant exception. */
  private static Pipeline buildPipelineOrThrow(DataflowSqlLauncherOptions options)
      throws IOException {
    options
        .as(BeamSqlPipelineOptions.class)
        .setPlannerName(ZetaSQLQueryPlanner.class.getCanonicalName());
    options.as(DataCatalogPipelineOptions.class).setTruncateTimestamps(true);

    // TODO(bhulette): Consider validating that there's only one sink per BQ table/PubSub topic
    List<SinkDefinition> sinks = getSinkDefinitions(options);
    if (sinks.isEmpty()) {
      throw new InvalidSinkException("No output sinks defined! Please define one with --outputs.");
    }

    Pipeline p;
    try {
      p = Pipeline.create(options);
    } catch (IllegalArgumentException e) {
      // DataflowRunner does some validation of pipeline options in Pipeline.create, invalid options
      // raise an IllegalArgumentException.
      throw new BadTemplateArgumentsException("Bad pipeline options", e);
    }

    // SqlTransform is a PTransform<PInput, PCollection<Row>>.
    // In this case it's applied to the Pipeline, so all the inputs
    // should be specified in the query directly.
    // Inputs resolution is handled by the DataCatalogTableProvider,
    // which calls Data Catalog to get the table schemas and then creates
    // the PCollections for them.
    LOG.info("Executing SQL query: {}", options.getQueryString());

    try (final DataCatalogTableProvider tableProvider =
        DataCatalogTableProvider.create(options.as(DataCatalogPipelineOptions.class))) {
      SqlTransform query =
          addParameters(
              SqlTransform.query(options.getQueryString())
                  .withDefaultTableProvider("datacatalog", tableProvider),
              options.getQueryParameters());

      PCollection<Row> queryResult = p.apply("Run SQL Query", query);
      for (String fieldName : queryResult.getSchema().getFieldNames()) {
        // ZetaSQL prefixes internal aliases with "$", and "$" is illegal in user queries.
        if (fieldName.startsWith("$")) {
          throw new BadTemplateArgumentsException(
              "All output columns in the top-level SELECT must be named. Columns "
                  + "from source tables have names by default. Other newly created "
                  + "columns (e.g., aggregated columns) can be named by explicit "
                  + "alias.");
        }
      }

      for (SinkDefinition sink : sinks) {
        LOG.info("Creating sink: " + sink.getTransformName());
        sink.applyTransform(queryResult, options, tableProvider);
      }
    }

    // shuffle_mode=auto is only safe for Dataflow batch mode
    // The DataflowRunner will automatically set isStreaming()==true if there is unbounded
    // PCollection, plus we expect this launcher to eventually be invoked with arbitrary added
    // pipeline options
    if (!(options.as(StreamingOptions.class).isStreaming() || containsUnboundedPCollection(p))) {
      // If the experiment is already set, we do not override it.
      // Note that in production shuffle_mode=auto is always set,
      // so disabling it in a test makes that test less accurate.
      ExperimentalOptions exOptions = options.as(ExperimentalOptions.class);
      List<String> experiments =
          MoreObjects.firstNonNull(exOptions.getExperiments(), Collections.emptyList());
      List<String> shuffleModeFlags =
          experiments.stream()
              .filter(exp -> exp.startsWith("shuffle_mode="))
              .collect(Collectors.toList());
      if (shuffleModeFlags.size() > 0) {
        LOG.warn("Not setting shuffle_mode=auto because it was forced: {}", shuffleModeFlags);
      } else {
        exOptions.setExperiments(
            ImmutableList.<String>builder().addAll(experiments).add("shuffle_mode=auto").build());
      }
    }

    return p;
  }

  /** Returns the result of parsing query parameters and inserting them into the sql transform. */
  private static SqlTransform addParameters(SqlTransform sqlTransform, String queryParameters)
      throws IOException {
    if (Strings.isNullOrEmpty(queryParameters)) {
      return sqlTransform;
    }
    List<Parameter> parameters = ParameterTranslator.parseJson(queryParameters);
    ParameterMode parameterMode = ParameterTranslator.inferParameterMode(parameters);
    switch (parameterMode) {
      case NAMED:
        Map<String, Value> namedParameters =
            ParameterTranslator.translateNamedParameters(parameters);
        return sqlTransform.withNamedParameters(namedParameters);
      case POSITIONAL:
        List<Value> positionalParameters =
            ParameterTranslator.translatePositionalParameters(parameters);
        return sqlTransform.withPositionalParameters(positionalParameters);
      case NONE:
        // fall through
      default:
        return sqlTransform;
    }
  }

  /**
   * Parses either --outputs or --outputTable (but not both), and returns a list of configured sink
   * definitions. Throws InvalidSinkException when input is incorrect (e.g. neither flag is defined,
   * or JSON value is not parseable).
   */
  private static List<SinkDefinition> getSinkDefinitions(DataflowSqlLauncherOptions options) {
    if (options.getOutputTable() != null) {
      if (options.getOutputs() != null) {
        throw new InvalidSinkException(
            "Both --outputs and --outputTable were defined! Please use just --outputs.");
      }
      LOG.warn("--outputTable is deprecated. Please prefer --outputs in the future.");
      try {
        TableReference tableReference =
            Transport.getJsonFactory().fromString(options.getOutputTable(), TableReference.class);
        return ImmutableList.of(
            new BigQuerySinkDefinition(tableReference, WriteDisposition.WRITE_EMPTY));
      } catch (IOException e) {
        throw new InvalidSinkException("Error parsing --outputTable: ", e);
      }
    } else if (options.getOutputs() != null) {
      ObjectMapper mapper = new ObjectMapper();
      JavaType listOfDefinitions =
          mapper.getTypeFactory().constructCollectionType(List.class, SinkDefinition.class);
      try {
        return mapper.readValue(options.getOutputs(), listOfDefinitions);
      } catch (IOException e) {
        throw new InvalidSinkException("Error parsing --outputs: ", e);
      }
    } else {
      throw new InvalidSinkException("Missing output specification. Please include --outputs.");
    }
  }

  private static boolean containsUnboundedPCollection(Pipeline p) {
    class BoundednessVisitor extends PipelineVisitor.Defaults {
      private IsBounded boundedness = IsBounded.BOUNDED;

      @Override
      public void visitValue(PValue value, Node producer) {
        if (value instanceof PCollection) {
          boundedness = boundedness.and(((PCollection) value).isBounded());
        }
      }
    }

    BoundednessVisitor visitor = new BoundednessVisitor();
    p.traverseTopologically(visitor);
    return visitor.boundedness == IsBounded.UNBOUNDED;
  }
}
