/*
 * Copyright (C) 2020 Google Inc.
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

import static com.google.cloud.teleport.v2.templates.ProtegrityDataTokenizationConstants.GCS_WRITING_WINDOW_DURATION;
import static com.google.cloud.teleport.v2.transforms.io.BigQueryIO.write;
import static com.google.cloud.teleport.v2.utils.DurationUtils.parseDuration;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.io.BigTableIO;
import com.google.cloud.teleport.v2.options.ProtegrityDataTokenizationOptions;
import com.google.cloud.teleport.v2.transforms.ErrorConverters;
import com.google.cloud.teleport.v2.transforms.JsonToBeamRow;
import com.google.cloud.teleport.v2.transforms.ProtegrityDataProtectors.RowToTokenizedRow;
import com.google.cloud.teleport.v2.transforms.SerializableFunctions;
import com.google.cloud.teleport.v2.transforms.io.BigQueryIO;
import com.google.cloud.teleport.v2.transforms.io.GcsIO;
import com.google.cloud.teleport.v2.utils.RowToCsv;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.cloud.teleport.v2.utils.SchemasUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@link ProtegrityDataTokenization} pipeline reads data from one of the supported sources,
 * tokenizes data with external API calls to Protegrity Data Security Gateway (DSG), and writes data
 * into one of the supported sinks.
 * <br>
 * <p><b>Pipeline Requirements</b>
 * <ul>
 *   <li>Java 8</li>
 *   <li>Data schema (JSON with an array of fields described in BigQuery format)</li>
 *   <li>1 of supported sources to read data from</li>
 *   <ul>
 *     <li><a href=https://cloud.google.com/storage>Google Cloud Storage</a> (Only JSON or CSV)</li>
 *     <li><a href=https://cloud.google.com/pubsub>Google Pub/Sub</a></li>
 *   </ul>
 *   <li>1 of supported destination sinks to write data into</li>
 *   <ul>
 *     <li><a href=https://cloud.google.com/storage>Google Cloud Storage</a> (Only JSON or CSV)</li>
 *     <li><a href=https://cloud.google.com/bigquery>Google Cloud BigQuery</a></li>
 *     <li><a href=https://cloud.google.com/bigtable>Cloud BigTable</a></li>
 *   </ul>
 *   <li>A configured Protegrity DSG</li>
 * </ul>
 *
 * <p><b>Example Usage</b>
 * <pre>
 * <b>Setting Up Project Environment</b>
 *   {@code
 *   # Set the pipeline vars
 *   PROJECT=id-of-my-project
 *   BUCKET_NAME=my-bucket
 *   REGION=my-region
 *
 *   # Set containerization vars
 *   IMAGE_NAME=my-image-name
 *   TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
 *   BASE_CONTAINER_IMAGE=JAVA8
 *   TEMPLATE_PATH="gs://${BUCKET_NAME}/templates/protegrity-data-tokenization.json"
 *
 *   # Create bucket in the cloud storage
 *   gsutil mb gs://${BUCKET_NAME}
 *   }
 * <b>Creating the Dataflow Flex Template</b>
 *   {@code
 *   # Go to the v2 folder
 *   cd /path/to/DataflowTemplates/v2
 *
 *   # Assemble jar with dependencies
 *    mvn package -am -pl protegrity-data-tokenization
 *
 *   # Go to the template folder
 *   cd /path/to/DataflowTemplates/v2/protegrity-data-tokenization
 *
 *   # Build the Dataflow Flex Template:
 *   gcloud dataflow flex-template build ${TEMPLATE_PATH} \
 *        --image-gcr-path "${TARGET_GCR_IMAGE}" \
 *        --sdk-language "JAVA" \
 *        --flex-template-base-image ${BASE_CONTAINER_IMAGE} \
 *        --metadata-file "src/main/resources/protegrity_data_tokenization_metadata.json" \
 *        --jar "target/protegrity-data-tokenization-1.0-SNAPSHOT.jar" \
 *        --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="com.google.cloud.teleport.v2.templates.ProtegrityDataTokenization"
 *   }
 * <b>Executing Template</b>
 *   {@code
 *   API_ROOT_URL="https://dataflow.googleapis.com"
 *   TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT}/locations/${REGION}/flexTemplates:launch"
 *   JOB_NAME="protegrity-data-tokenization-`date +%Y%m%d-%H%M%S-%N`"
 *
 *   # This example shows the set of parameters specific for GCS as input source and BigQuery as output sink
 *   # For specifying other sources and sinks please see the README file.
 *   time curl -X POST -H "Content-Type: application/json" \
 *       -H "Authorization: Bearer $(gcloud auth print-access-token)" \
 *       -d '
 *        {
 *            "launch_parameter": {
 *                "jobName": "'$JOB_NAME'",
 *                "containerSpecGcsPath": "'$TEMPLATE_PATH'",
 *                "parameters": {
 *                    "dataSchemaGcsPath": "gs://<my-bucket>/<path-to-schema>",
 *                    "inputGcsFilePattern": "gs://<my-bucket>/<path-to-data>/*",
 *                    "inputGcsFileFormat": "JSON",
 *                    "bigQueryTableName": "<project:dataset.tablename>",
 *                    "dsgUri": "https://<dsg-uri>/tokenize",
 *                    "batchSize": 10,
 *                    "payloadConfigGcsPath": "gs://<my-bucket>/<path-to-payload-config>",
 *                    "nonTokenizedDeadLetterGcsPath": "gs://<my-bucket>/<path-to-folder-for-errors>"
 *                }
 *            }
 *        }
 *       '
 *       "${TEMPLATES_LAUNCH_API}"
 *   }
 * </pre>
 */
public class ProtegrityDataTokenization {

  /**
   * Logger for class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(ProtegrityDataTokenization.class);

  /**
   * String/String Coder for FailsafeElement.
   */
  public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(
          NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of()));


  /**
   * The default suffix for error tables if dead letter table is not specified.
   */
  private static final String DEFAULT_DEADLETTER_TABLE_SUFFIX = "_error_records";

  /**
   * The tag for the main output for the UDF.
   */
  private static final TupleTag<Row> TOKENIZATION_OUT = new TupleTag<Row>() {
  };


  /**
   * The tag for the dead-letter output of the udf.
   */
  static final TupleTag<FailsafeElement<Row, Row>> TOKENIZATION_DEADLETTER_OUT =
      new TupleTag<FailsafeElement<Row, Row>>() {
      };


  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args) {
    ProtegrityDataTokenizationOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(ProtegrityDataTokenizationOptions.class);
    FileSystems.setDefaultPipelineOptions(options);

    String serviceAccount;
    if (DataflowRunner.class.isAssignableFrom(options.getRunner())) {
      DataflowPipelineOptions dataflowOptions = PipelineOptionsFactory.fromArgs(args)
          .withoutStrictParsing()
          .withValidation()
          .as(DataflowPipelineOptions.class);
      serviceAccount = dataflowOptions.getServiceAccount();
    } else {
      serviceAccount = StringUtils.EMPTY;
    }

    run(options, serviceAccount);
  }

  /**
   * Runs the pipeline to completion with the specified options.
   *
   * @param options        The execution options.
   * @param serviceAccount The email address representing Google account.
   * @return The pipeline result.
   */
  public static PipelineResult run(ProtegrityDataTokenizationOptions options,
      String serviceAccount) {
    checkArgument(StringUtils.isNoneBlank(options.getDataSchemaGcsPath()),
        "Missing required value for --dataSchemaGcsPath.");

    SchemasUtils schema = null;
    try {
      schema = new SchemasUtils(options.getDataSchemaGcsPath(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      LOG.error("Failed to retrieve schema for data.", e);
    }
    checkArgument(schema != null, "Data schema is mandatory.");

    Map<String, String> dataElements = schema
        .getDataElementsToTokenize(options.getPayloadConfigGcsPath());

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);
    // Register the coder for pipeline
    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(
        FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);
    coderRegistry
        .registerCoderForType(RowCoder.of(schema.getBeamSchema()).getEncodedTypeDescriptor(),
            RowCoder.of(schema.getBeamSchema()));
    /*
     * Row/Row Coder for FailsafeElement.
     */
    FailsafeElementCoder<Row, Row> coder = FailsafeElementCoder.of(
        RowCoder.of(schema.getBeamSchema()),
        RowCoder.of(schema.getBeamSchema())
    );
    coderRegistry
        .registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    PCollection<Row> records;
    if (options.getInputGcsFilePattern() != null) {
      records = new GcsIO(options).read(pipeline, schema);
    } else if (options.getInputSubscription() != null) {
      records = pipeline
          .apply("ReadMessagesFromPubsub",
              PubsubIO.readStrings().fromSubscription(options.getInputSubscription()))
          .apply("TransformToBeamRow",
              new JsonToBeamRow(options.getNonTokenizedDeadLetterGcsPath(), schema));
      if (options.getOutputGcsDirectory() != null) {

        records = records
            .apply(
                Window.into(FixedWindows.of(parseDuration(
                    options.getInputSubscription() != null ? options.getWindowDuration()
                        : GCS_WRITING_WINDOW_DURATION
                ))));
      }
    } else {
      throw new IllegalStateException("No source is provided, please configure GCS or Pub/Sub");
    }

    /*
    Tokenize data using remote API call
     */
    PCollectionTuple tokenizedRows = records
        .apply(MapElements
            .into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.rows()))
            .via((Row row) -> KV.of(0, row)))
        .setCoder(KvCoder.of(VarIntCoder.of(), RowCoder.of(schema.getBeamSchema())))
        .apply("DsgTokenization",
            RowToTokenizedRow.newBuilder()
                .setBatchSize(options.getBatchSize())
                .setDsgURI(options.getDsgUri())
                .setSchema(schema.getBeamSchema())
                .setDataElements(dataElements)
                .setServiceAccount(serviceAccount)
                .setSuccessTag(TOKENIZATION_OUT)
                .setFailureTag(TOKENIZATION_DEADLETTER_OUT).build());

    String csvDelimiter = options.getCsvDelimiter();
    if (options.getNonTokenizedDeadLetterGcsPath() != null) {
    /*
    Write tokenization errors to dead-letter sink
     */
      tokenizedRows.get(TOKENIZATION_DEADLETTER_OUT)
          .apply("ConvertToCSV", MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
              .via((FailsafeElement<Row, Row> fse) ->
                  FailsafeElement.of(
                      new RowToCsv(csvDelimiter).getCsvFromRow(fse.getOriginalPayload()),
                      new RowToCsv(csvDelimiter).getCsvFromRow(fse.getPayload())))
          )
          .apply("WriteTokenizationErrorsToGcs",
              ErrorConverters.WriteErrorsToTextIO.<String, String>newBuilder()
                  .setErrorWritePath(options.getNonTokenizedDeadLetterGcsPath())
                  .setTranslateFunction(SerializableFunctions.getCsvErrorConverter())
                  .build());
    }

    if (options.getOutputGcsDirectory() != null) {
      new GcsIO(options).write(
          tokenizedRows.get(TOKENIZATION_OUT),
          schema.getBeamSchema()
      );
    } else if (options.getBigQueryTableName() != null) {
      WriteResult writeResult = write(tokenizedRows.get(TOKENIZATION_OUT),
          options.getBigQueryTableName(),
          schema.getBigQuerySchema());
      writeResult
          .getFailedInsertsWithErr()
          .apply(
              "WrapInsertionErrors",
              MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                  .via(BigQueryIO::wrapBigQueryInsertError))
          .setCoder(FAILSAFE_ELEMENT_CODER)
          .apply(
              "WriteInsertionFailedRecords",
              ErrorConverters.WriteStringMessageErrors.newBuilder()
                  .setErrorRecordsTable(
                      options.getBigQueryTableName() + DEFAULT_DEADLETTER_TABLE_SUFFIX)
                  .setErrorRecordsTableSchema(SchemaUtils.DEADLETTER_SCHEMA)
                  .build());
    } else if (options.getBigTableInstanceId() != null) {
      tokenizedRows.get(TOKENIZATION_OUT)
          .apply(
              BigTableIO.write(options, schema.getBeamSchema())
          );
    } else {
      throw new IllegalStateException(
          "No sink is provided, please configure BigQuery or BigTable.");
    }

    return pipeline.run();
  }
}
