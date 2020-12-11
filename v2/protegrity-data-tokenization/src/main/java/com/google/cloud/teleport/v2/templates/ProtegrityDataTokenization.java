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

import static com.google.cloud.teleport.v2.transforms.io.BigQueryIO.write;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.options.ProtegrityDataTokenizationOptions;
import com.google.cloud.teleport.v2.transforms.ErrorConverters;
import com.google.cloud.teleport.v2.transforms.io.BigQueryIO;
import com.google.cloud.teleport.v2.transforms.io.BigTableIO;
import com.google.cloud.teleport.v2.transforms.io.GcsIO;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.cloud.teleport.v2.utils.SchemasUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@link ProtegrityDataTokenization} pipeline.
 */
public class ProtegrityDataTokenization {

    /**
     * Logger for class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(ProtegrityDataTokenization.class);

    /**
     * String/String Coder for FailsafeElement.
     */
    private static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
            FailsafeElementCoder.of(
                    NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of()));

    /**
     * The default suffix for error tables if dead letter table is not specified.
     */
    private static final String DEFAULT_DEADLETTER_TABLE_SUFFIX = "_error_records";

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

        run(options);
    }

    /**
     * Runs the pipeline to completion with the specified options.
     *
     * @param options The execution options.
     * @return The pipeline result.
     */
    public static PipelineResult run(ProtegrityDataTokenizationOptions options) {
        SchemasUtils schema = null;
        try {
            schema = new SchemasUtils(options.getDataSchemaGcsPath(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            LOG.error("Failed to retrieve schema for data.", e);
        }
        checkArgument(schema != null, "Data schema is mandatory.");

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);
        // Register the coder for pipeline
        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForType(
                FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);

        PCollection<String> jsons;
        if (options.getInputGcsFilePattern() != null) {
            jsons = new GcsIO(options).read(pipeline, schema.getJsonBeamSchema());
        } else if (options.getPubsubTopic() != null) {
            jsons = pipeline
                    .apply("ReadMessagesFromPubsub", PubsubIO.readStrings().fromTopic(options.getPubsubTopic()));
        } else {
            throw new IllegalStateException("No source is provided, please configure GCS or Pub/Sub");
        }

        JsonToRow.ParseResult rows = jsons
                .apply("JsonToRow", JsonToRow.withExceptionReporting(schema.getBeamSchema()).withExtendedErrorInfo());

        /*
         * Write Row conversion errors to filesystem specified path
         */
        rows.getFailedToParseLines()
                .apply("ToFailsafeElement", MapElements
                        .into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                        .via((Row errRow) -> FailsafeElement
                                .of(errRow.getString("line"), errRow.getString("line"))
                                .setErrorMessage(errRow.getString("err"))
                        ))
                .apply("WriteCsvConversionErrorsToGcs",
                        ErrorConverters.WriteStringMessageErrorsAsCsv.newBuilder()
                                .setCsvDelimiter(options.getCsvDelimiter())
                                .setErrorWritePath(options.getNonTokenizedDeadLetterGcsPath())
                                .build());


        if (options.getBigQueryTableName() != null) {
            WriteResult writeResult = write(rows.getResults(), options.getBigQueryTableName(), schema.getBigQuerySchema());
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
                                    .setErrorRecordsTable(options.getBigQueryTableName() + DEFAULT_DEADLETTER_TABLE_SUFFIX)
                                    .setErrorRecordsTableSchema(SchemaUtils.DEADLETTER_SCHEMA)
                                    .build());
        } else if (options.getBigTableInstanceId() != null) {
            new BigTableIO(options).write(
                    rows.getResults(),
                    schema.getBeamSchema()
            );
        } else {
            throw new IllegalStateException("No sink is provided, please configure BigQuery or BigTable.");
        }

        return pipeline.run();
    }

}
