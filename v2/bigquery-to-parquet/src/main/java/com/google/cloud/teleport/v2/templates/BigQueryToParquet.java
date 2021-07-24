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

import com.google.api.gax.rpc.InvalidArgumentException;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions;
import com.google.cloud.bigquery.storage.v1beta1.Storage.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1beta1.TableReferenceProto;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link BigQueryToParquet} pipeline exports data from a BigQuery table to Parquet file(s) in a
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
 * JOB_NAME="bigquery-to-parquet-`date +%Y%m%d-%H%M%S-%N`"
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
 *           "tableRef":"'$TABLE'",
 *           "bucket":"'$BUCKET_NAME/results'",
 *           "numShards":"5",
 *           "fields":"field1,field2"
 *        }
 *       }
 *      '
 * </pre>
 */
public class BigQueryToParquet {

  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryToParquet.class);

  /** File suffix for file to be written. */
  private static final String FILE_SUFFIX = ".parquet";

  /** Factory to create BigQueryStorageClients. */
  static class BigQueryStorageClientFactory {

    /**
     * Creates BigQueryStorage client for use in extracting table schema.
     *
     * @return BigQueryStorageClient
     */
    static BigQueryStorageClient create() {
      try {
        return BigQueryStorageClient.create();
      } catch (IOException e) {
        LOG.error("Error connecting to BigQueryStorage API: " + e.getMessage());
        throw new RuntimeException(e);
      }
    }
  }

  /** Factory to create ReadSessions. */
  static class ReadSessionFactory {

    /**
     * Creates ReadSession for schema extraction.
     *
     * @param client BigQueryStorage client used to create ReadSession.
     * @param tableString String that represents table to export from.
     * @param tableReadOptions TableReadOptions that specify any fields in the table to filter on.
     * @return session ReadSession object that contains the schema for the export.
     */
    static ReadSession create(
        BigQueryStorageClient client, String tableString, TableReadOptions tableReadOptions) {
      TableReference tableReference = BigQueryHelpers.parseTableSpec(tableString);
      String parentProjectId = "projects/" + tableReference.getProjectId();

      TableReferenceProto.TableReference storageTableRef =
          TableReferenceProto.TableReference.newBuilder()
              .setProjectId(tableReference.getProjectId())
              .setDatasetId(tableReference.getDatasetId())
              .setTableId(tableReference.getTableId())
              .build();

      CreateReadSessionRequest.Builder builder =
          CreateReadSessionRequest.newBuilder()
              .setParent(parentProjectId)
              .setReadOptions(tableReadOptions)
              .setTableReference(storageTableRef);
      try {
        return client.createReadSession(builder.build());
      } catch (InvalidArgumentException iae) {
        LOG.error("Error creating ReadSession: " + iae.getMessage());
        throw new RuntimeException(iae);
      }
    }
  }

  /**
   * The {@link BigQueryToParquetOptions} class provides the custom execution options passed by the
   * executor at the command-line.
   */
  public interface BigQueryToParquetOptions extends PipelineOptions {
    @Description("BigQuery table to export from in the form <project>:<dataset>.<table>")
    @Required
    String getTableRef();

    void setTableRef(String tableRef);

    @Description("GCS bucket to export BigQuery table data to (e.g. gs://mybucket/folder/).")
    @Required
    String getBucket();

    void setBucket(String bucket);

    @Description("Optional: Number of shards for output file.")
    @Default.Integer(0)
    Integer getNumShards();

    void setNumShards(Integer numShards);

    @Description("Optional: Comma separated list of fields to select from the table.")
    String getFields();

    void setFields(String fields);
  }

  /**
   * The {@link BigQueryToParquet#getTableSchema(ReadSession)} method gets Avro schema for table
   * using from the {@link ReadSession} object.
   *
   * @param session ReadSession that contains schema for table, filtered by fields if any.
   * @return avroSchema Avro schema for table. If fields are provided then schema will only contain
   *     those fields.
   */
  private static Schema getTableSchema(ReadSession session) {
    Schema avroSchema;

    avroSchema = new Schema.Parser().parse(session.getAvroSchema().getSchema());
    LOG.info("Schema for export is: " + avroSchema.toString());

    return avroSchema;
  }

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args) {
    BigQueryToParquetOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQueryToParquetOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  private static PipelineResult run(BigQueryToParquetOptions options) {

    // Create the pipeline.
    Pipeline pipeline = Pipeline.create(options);

    TableReadOptions.Builder builder = TableReadOptions.newBuilder();

    /* Add fields to filter export on, if any. */
    if (options.getFields() != null) {
      builder.addAllSelectedFields(Arrays.asList(options.getFields().split(",\\s*")));
    }

    TableReadOptions tableReadOptions = builder.build();
    BigQueryStorageClient client = BigQueryStorageClientFactory.create();
    ReadSession session =
        ReadSessionFactory.create(client, options.getTableRef(), tableReadOptions);

    // Extract schema from ReadSession
    Schema schema = getTableSchema(session);
    client.close();

    TypedRead<GenericRecord> readFromBQ =
        BigQueryIO.read(SchemaAndRecord::getRecord)
            .from(options.getTableRef())
            .withTemplateCompatibility()
            .withMethod(Method.DIRECT_READ)
            .withCoder(AvroCoder.of(schema));

    if (options.getFields() != null) {
      List<String> selectedFields = Splitter.on(",").splitToList(options.getFields());
      readFromBQ =
          selectedFields.isEmpty() ? readFromBQ : readFromBQ.withSelectedFields(selectedFields);
    }

    /*
     * Steps: 1) Read records from BigQuery via BigQueryIO.
     *        2) Write records to Google Cloud Storage in Parquet format.
     */
    pipeline
        /*
         * Step 1: Read records via BigQueryIO using supplied schema as a PCollection of
         *         {@link GenericRecord}.
         */
        .apply("ReadFromBigQuery", readFromBQ)
        /*
         * Step 2: Write records to Google Cloud Storage as one or more Parquet files
         *         via {@link ParquetIO}.
         */
        .apply(
            "WriteToParquet",
            FileIO.<GenericRecord>write()
                .via(ParquetIO.sink(schema))
                .to(options.getBucket())
                .withNumShards(options.getNumShards())
                .withSuffix(FILE_SUFFIX));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }
}
