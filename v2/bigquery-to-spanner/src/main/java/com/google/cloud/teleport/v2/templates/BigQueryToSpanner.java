/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions;
import com.google.cloud.spanner.Mutation.Op;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.ConversionOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.ConversionOptions.TruncateTimestamps;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link BigQueryToSpanner} pipeline exports data from a BigQuery table to Parquet file(s) in a
 * Google Cloud Storage bucket.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>BigQuery Table exists.
 *   <li>Google Cloud Storage bucket exists.
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT=my-project
 * BUCKET_NAME=my-bucket
 * TABLE={$PROJECT}:my-dataset.my-table
 *
 * # Set containerization vars
 * IMAGE_NAME=my-image-name
 * TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
 * BASE_CONTAINER_IMAGE=my-base-container-image
 * BASE_CONTAINER_IMAGE_VERSION=my-base-container-image-version
 * APP_ROOT=/path/to/app-root
 * COMMAND_SPEC=/path/to/command-spec
 *
 * # Build and upload image
 * mvn clean package \
 * -Dimage=${TARGET_GCR_IMAGE} \
 * -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
 * -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
 * -Dapp-root=${APP_ROOT} \
 * -Dcommand-spec=${COMMAND_SPEC}
 *
 * # Create an image spec in GCS that contains the path to the image
 * {
 *    "docker_template_spec": {
 *       "docker_image": $TARGET_GCR_IMAGE
 *     }
 *  }
 *
 * # Execute template:
 * API_ROOT_URL="https://dataflow.googleapis.com"
 * TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT}/templates:launch"
 * JOB_NAME="bigquery-to-spanner-`date +%Y%m%d-%H%M%S-%N`"
 * time curl -X POST -H "Content-Type: application/json"     \
 *      -H "Authorization: Bearer $(gcloud auth print-access-token)" \
 *      "${TEMPLATES_LAUNCH_API}"`
 *      `"?validateOnly=false"`
 *      `"&dynamicTemplate.gcsPath=gs://path/to/image/spec"`
 *      `"&dynamicTemplate.stagingLocation=gs://path/to/stagingLocation" \
 *      -d '
 *       {
 *         "jobName":"'$JOB_NAME'",
 *         "parameters": {
 *           "bigQueryTables" "'$BIG_QUERY_TABLES'",
 *           "spannerTables": "$SPANNER_TABLES",
 *           "spannerDatabase": "$SPANNER_DATABASE",
 *           "spannerInstance": "$SPANNER_INSTANCE"
 *         }
 *       }
 *       '
 * </pre>
 */
public class BigQueryToSpanner {

  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryToSpanner.class);

  /**
   * File suffix for file to be written.
   */
  private static final String FILE_SUFFIX = ".parquet";

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args) {
    BigQueryToSpannerOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQueryToSpannerOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  private static PipelineResult run(BigQueryToSpannerOptions options) {

    // Create the pipeline.
    Pipeline pipeline = Pipeline.create(options);

    List<String> bigQueryTables = options.getBigQueryTables();
    List<String> spannerTables = options.getSpannerTables();
    if (spannerTables == null) {
      spannerTables = new ArrayList<>();
      for (String ref : bigQueryTables) {
        spannerTables.add(BigQueryHelpers.parseTableSpec(ref).getTableId());
      }
    } else {
      Preconditions.checkArgument(bigQueryTables.size() == spannerTables.size(),
          "spannerTables option must have the same number of tables as bigQueryTableRefs if provided");
    }

    for (int i = 0; i < bigQueryTables.size(); i++) {
      String bigQueryTable = bigQueryTables.get(i);
      String spannerTable = spannerTables.get(i);
      /*
       * Steps: 1) Read records from BigQuery
       *        2) Write records to Spanner Table
       */

      // Convert to Beam Schema
      Schema schema = new BigQueryBeamSchemaSupplier(bigQueryTable,
          TableReadOptions.newBuilder().build()).get();
      ConversionOptions conversionOptions = ConversionOptions.builder().setTruncateTimestamps(
          TruncateTimestamps.TRUNCATE).build();

      PCollection<Row> rows = pipeline
          .apply(
              String.format("ReadFromBigQuery:%s", bigQueryTable),
              BigQueryIO.read(
                  (schemaAndRecord) -> BigQueryUtils
                      .toBeamRow(schemaAndRecord.getRecord(), schema, conversionOptions))
                  .from(bigQueryTable)
                  .withMethod(Method.DIRECT_READ)
                  .withCoder(SchemaCoder.of(schema))
          ).setRowSchema(schema);

      SpannerIO.Write spannerWrite = SpannerIO.write().withDatabaseId(options.getSpannerDatabase())
          .withInstanceId(options.getSpannerInstance());
      if (options.getSpannerProject() != null) {
        spannerWrite = spannerWrite.withProjectId(options.getSpannerProject());
      }

      rows.apply(SpannerConverters.toMutation(spannerTable, options.getSpannerOperation()))
          .apply(String.format("WriteToSpanner:%s", spannerTable), spannerWrite);
    }
    // Execute the pipeline and return the result.
    return pipeline.run();
  }

  /**
   * The {@link BigQueryToSpannerOptions} class provides the custom execution options passed by the
   * executor at the command-line.
   */
  public interface BigQueryToSpannerOptions extends PipelineOptions {

    @Description("BigQuery table to export from in the form <project>:<dataset>.<table>, multiple comma separated tables can be provided")
    @Required
    List<String> getBigQueryTables();

    void setBigQueryTables(List<String> bigQueryTables);

    @Description("Spanner tables to write to, if provided there must be one output table for every input bigQueryTableRef. If not provided, we will use the same names as the BigQuery tables")
    List<String> getSpannerTables();

    void setSpannerTables(List<String> spannerTables);

    @Description("Spanner instance to write to")
    @Required
    String getSpannerInstance();

    void setSpannerInstance(String spannerInstance);

    @Description("Spanner database to write to")
    @Required
    String getSpannerDatabase();

    void setSpannerDatabase(String spannerDatabase);

    @Description("Project where spanner instance lives, defaults to same project as Dataflow")
    String getSpannerProject();

    void setSpannerProject(String spannerProject);

    @Description("Spanner operation to use when writing, must be one of INSERT_OR_UPDATE (default), INSERT, UPDATE or REPLACE")
    @Default.Enum(value = "INSERT_OR_UPDATE")
    Op getSpannerOperation();

    void setSpannerOperation(Op spannerDatabase);
  }
}
