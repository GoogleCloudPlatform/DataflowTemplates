/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.v2.transforms.BigQueryConverters.BigQueryReadOptions;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.ReadBigQuery;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.TableRowToJsonFn;
import com.google.cloud.teleport.v2.transforms.ElasticsearchTransforms.WriteToElasticsearch;
import com.google.cloud.teleport.v2.transforms.ElasticsearchTransforms.WriteToElasticsearchOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * The {@link BigQueryToElasticsearch} pipeline exports data from a BigQuery table to Elasticsearch.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>BigQuery Table exists.
 *   <li>Elasticsearch node exists and is reachable by Dataflow workers.
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT=my-project
 * BUCKET_NAME=my-bucket
 * TABLE={$PROJECT}:my-dataset.my-table
 * NODE_ADDRESSES=comma-separated-list-nodes
 * INDEX=my-index
 * DOCUMENT_TYPE=my-type
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
 * JOB_NAME="bigquery-to-elasticsearch-`date +%Y%m%d-%H%M%S-%N`"
 *
 * time curl -X POST -H "Content-Type: application/json"     \
 *     -H "Authorization: Bearer $(gcloud auth print-access-token)" \
 *     "${TEMPLATES_LAUNCH_API}"`
 *     `"?validateOnly=false"`
 *     `"&dynamicTemplate.gcsPath=${BUCKET_NAME}/path/to/image-spec"`
 *     `"&dynamicTemplate.stagingLocation=${BUCKET_NAME}/staging" \
 *     -d '
 *      {
 *       "jobName":"'$JOB_NAME'",
 *       "parameters": {
 *            "inputTableSpec":"'$TABLE'",
 *            "nodeAddresses":"'$NODE_ADDRESSES'",
 *            "index":"'$INDEX'",
 *            "documentType":"'$DOCUMENT_TYPE'"
 *        }
 *       }
 *      '
 * </pre>
 */
public class BigQueryToElasticsearch {

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args) {
    BigQueryToElasticsearchReadOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(BigQueryToElasticsearchReadOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  private static PipelineResult run(BigQueryToElasticsearchReadOptions options) {

    // Create the pipeline.
    Pipeline pipeline = Pipeline.create(options);

    /*
     * Steps: 1) Read records from BigQuery via BigQueryIO.
     *        2) Create json string from Table Row.
     *        3) Write records to Elasticsearch.
     *
     *
     * Step #1: Read from BigQuery. If a query is provided then it is used to get the TableRows.
     */
    pipeline
        .apply(
            "ReadFromBigQuery",
            ReadBigQuery.newBuilder()
                .setOptions(options.as(BigQueryToElasticsearchReadOptions.class))
                .build())

        /*
         * Step #2: Convert table rows to JSON documents.
         */
        .apply("TableRowsToJsonDocument", ParDo.of(new TableRowToJsonFn()))

        /*
         * Step #3: Write converted records to Elasticsearch
         */
        .apply(
            "WriteToElasticsearch",
            WriteToElasticsearch.newBuilder()
                .setOptions(options.as(WriteToElasticsearchOptions.class))
                .build());

    return pipeline.run();
  }

  /**
   * The {@link BigQueryToElasticsearchReadOptions} class provides the custom execution options
   * passed by the executor at the command-line.
   */
  public interface BigQueryToElasticsearchReadOptions
      extends PipelineOptions, BigQueryReadOptions, WriteToElasticsearchOptions {}
}
