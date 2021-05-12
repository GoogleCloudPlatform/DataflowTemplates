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
import static com.google.cloud.teleport.v2.transforms.BeamRowConverters.FAILSAFE_ELEMENT_CODER;
import static com.google.cloud.teleport.v2.transforms.BeamRowConverters.TRANSFORM_DEADLETTER_OUT;
import static com.google.cloud.teleport.v2.transforms.BeamRowConverters.TRANSFORM_OUT;
import static com.google.cloud.teleport.v2.transforms.io.BigQueryIO.write;
import static com.google.cloud.teleport.v2.utils.DurationUtils.parseDuration;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.io.BigTableIO;
import com.google.cloud.teleport.v2.io.GoogleCloudStorageIO;
import com.google.cloud.teleport.v2.options.ProtegrityDataTokenizationOptions;
import com.google.cloud.teleport.v2.transforms.BeamRowConverters;
import com.google.cloud.teleport.v2.transforms.BeamRowConverters.FailsafeRowToFailsafeCsv;
import com.google.cloud.teleport.v2.transforms.ErrorConverters;
import com.google.cloud.teleport.v2.transforms.ProtegrityDataProtectors.RowToTokenizedRow;
import com.google.cloud.teleport.v2.transforms.io.BigQueryIO;
import com.google.cloud.teleport.v2.utils.BeamSchemaUtils;
import com.google.cloud.teleport.v2.utils.BeamSchemaUtils.SchemaParseException;
import com.google.cloud.teleport.v2.utils.FailsafeElementToStringCsvSerializableFunction;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.cloud.teleport.v2.utils.TokenizationSchemaUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
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
 * <p>
 * For pipeline options and execution instructions refer <a href="{@docRoot}/../../../README.md">README.md</a>
 */
public class ProtegrityDataTokenization {

  /**
   * Logger for class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(ProtegrityDataTokenization.class);


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

    String serviceAccount = StringUtils.EMPTY;
    if (DataflowRunner.class.isAssignableFrom(options.getRunner())) {
      DataflowPipelineOptions dataflowOptions = PipelineOptionsFactory.fromArgs(args)
          .withoutStrictParsing()
          .withValidation()
          .as(DataflowPipelineOptions.class);
      serviceAccount = dataflowOptions.getServiceAccount();
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

    Schema schema = null;
    try {
      schema = BeamSchemaUtils
          .fromJson(SchemaUtils.getGcsFileAsString(options.getDataSchemaGcsPath()));
    } catch (IOException | SchemaParseException e) {
      LOG.error("Failed to retrieve schema for data.", e);
    }
    checkArgument(schema != null, "Data schema is mandatory.");

    Map<String, String> dataElements = TokenizationSchemaUtils
        .getDataElementsToTokenize(options.getPayloadConfigGcsPath(), schema);

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);
    // Register the coder for pipeline
    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(
        FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);
    coderRegistry
        .registerCoderForType(RowCoder.of(schema).getEncodedTypeDescriptor(),
            RowCoder.of(schema));
    /*
     * Row/Row Coder for FailsafeElement.
     */
    FailsafeElementCoder<Row, Row> coder = FailsafeElementCoder
        .of(RowCoder.of(schema), RowCoder.of(schema));
    coderRegistry
        .registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    PCollection<Row> records;
    if (options.getInputGcsFilePattern() != null) {
      GoogleCloudStorageIO.Read read = GoogleCloudStorageIO.read(options, schema);
      switch (options.getInputGcsFileFormat()) {
        case CSV:
          records = pipeline.apply(read.csv(options));
          break;
        case JSON:
          records = pipeline.apply(read.json());
          break;
        case AVRO:
          records = pipeline.apply(read.avro());
          break;
        default:
          throw new IllegalStateException(
              "No valid format for input data is provided. Please, choose JSON, CSV or AVRO.");
      }
    } else if (options.getInputSubscription() != null) {
      PCollectionTuple recordsTuple = pipeline
          .apply("ReadMessagesFromPubsub",
              PubsubIO.readStrings().fromSubscription(options.getInputSubscription()))
          .apply("StringToFailsafe",
              MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                  .via((String element) -> FailsafeElement.of(element, element)))
          .apply("FailsafeJsonToBeamRow",
              BeamRowConverters.FailsafeJsonToBeamRow.<String>newBuilder()
                  .setBeamSchema(schema)
                  .setSuccessTag(TRANSFORM_OUT)
                  .setFailureTag(TRANSFORM_DEADLETTER_OUT)
                  .build()
          );

      recordsTuple
          .get(TRANSFORM_DEADLETTER_OUT)
          .apply("WriteCsvConversionErrorsToGcs",
              ErrorConverters.WriteErrorsToTextIO.<String, String>newBuilder()
                  .setErrorWritePath(options.getNonTokenizedDeadLetterGcsPath())
                  .setTranslateFunction(new FailsafeElementToStringCsvSerializableFunction<>())
                  .build());

      records = recordsTuple.get(TRANSFORM_OUT);

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
        .setCoder(KvCoder.of(VarIntCoder.of(), RowCoder.of(schema)))
        .apply("DsgTokenization",
            RowToTokenizedRow.newBuilder()
                .setBatchSize(options.getBatchSize())
                .setDsgURI(options.getDsgUri())
                .setSchema(schema)
                .setDataElements(dataElements)
                .setServiceAccount(serviceAccount)
                .setSuccessTag(TOKENIZATION_OUT)
                .setFailureTag(TOKENIZATION_DEADLETTER_OUT).build());

    if (options.getNonTokenizedDeadLetterGcsPath() != null) {
    /*
    Write tokenization errors to dead-letter sink
     */
      tokenizedRows.get(TOKENIZATION_DEADLETTER_OUT)
          .apply("ConvertToCSV", FailsafeRowToFailsafeCsv.newBuilder().build())
          .apply("WriteTokenizationErrorsToGcs",
              ErrorConverters.WriteErrorsToTextIO.<String, String>newBuilder()
                  .setErrorWritePath(options.getNonTokenizedDeadLetterGcsPath())
                  .setTranslateFunction(new FailsafeElementToStringCsvSerializableFunction<>())
                  .build());
    }

    if (options.getOutputGcsDirectory() != null) {
      GoogleCloudStorageIO.Write write = GoogleCloudStorageIO.write(options);
      switch (options.getOutputGcsFileFormat()) {
        case JSON:
          tokenizedRows.get(TOKENIZATION_OUT).apply(write.json());
          break;
        case AVRO:
          tokenizedRows.get(TOKENIZATION_OUT).apply(write.avro(schema));
          break;
        case CSV:
          tokenizedRows.get(TOKENIZATION_OUT).apply(write.csv(options)
              .withFieldNames(schema.getFieldNames())
          );
          break;
        default:
          throw new IllegalStateException(
              "No valid format for output data is provided. Please, choose JSON, CSV or AVRO.");
      }
    } else if (options.getBigQueryTableName() != null) {
      WriteResult writeResult = write(tokenizedRows.get(TOKENIZATION_OUT),
          options.getBigQueryTableName(),
          BeamSchemaUtils.beamSchemaToBigQuerySchema(schema));
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
              BigTableIO.write(options, schema)
          );
    } else {
      throw new IllegalStateException(
          "No sink is provided, please configure BigQuery or BigTable.");
    }

    return pipeline.run();
  }
}
