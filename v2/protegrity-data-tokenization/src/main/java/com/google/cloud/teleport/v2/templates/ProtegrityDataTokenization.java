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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.options.ProtegrityDataTokenizationOptions;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.cloud.teleport.v2.transforms.ErrorConverters;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.cloud.teleport.v2.utils.SchemasUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
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

        JsonToRow.ParseResult rows = pipeline
                .apply("readTextFromGCSFiles", TextIO.read().from(options.getInputGcsFilePattern()))
                .apply("jsonToRow", JsonToRow.withExceptionReporting(schema.getBeamSchema()).withExtendedErrorInfo());

        if (options.getBigQueryTableName() != null) {
            WriteResult writeResult = writeToBigQuery(rows.getResults(), options.getBigQueryTableName(), schema.getBigQuerySchema());
            writeResult
                    .getFailedInsertsWithErr()
                    .apply(
                            "WrapInsertionErrors",
                            MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                                    .via(ProtegrityDataTokenization::wrapBigQueryInsertError))
                    .setCoder(FAILSAFE_ELEMENT_CODER)
                    .apply(
                            "WriteInsertionFailedRecords",
                            ErrorConverters.WriteStringMessageErrors.newBuilder()
                                    .setErrorRecordsTable(options.getBigQueryTableName() + DEFAULT_DEADLETTER_TABLE_SUFFIX)
                                    .setErrorRecordsTableSchema(SchemaUtils.DEADLETTER_SCHEMA)
                                    .build());
        }

        return pipeline.run();
    }

    private static WriteResult writeToBigQuery(PCollection<Row> input, String bigQueryTableName, TableSchema schema) {
        return input
                .apply("RowToTableRow", ParDo.of(new BigQueryConverters.RowToTableRowFn()))
                .apply(
                        "WriteSuccessfulRecords",
                        BigQueryIO.writeTableRows()
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .withExtendedErrorInfo()
                                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                                .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                                .withSchema(schema)
                                .to(bigQueryTableName));
    }

    /**
     * Method to wrap a {@link BigQueryInsertError} into a {@link FailsafeElement}.
     *
     * @param insertError BigQueryInsert error.
     * @return FailsafeElement object.
     */
    protected static FailsafeElement<String, String> wrapBigQueryInsertError(
            BigQueryInsertError insertError) {

        FailsafeElement<String, String> failsafeElement;
        try {

            failsafeElement =
                    FailsafeElement.of(
                            insertError.getRow().toPrettyString(), insertError.getRow().toPrettyString());
            failsafeElement.setErrorMessage(insertError.getError().toPrettyString());

        } catch (IOException e) {
            LOG.error("Failed to wrap BigQuery insert error.");
            throw new RuntimeException(e);
        }
        return failsafeElement;
    }

}
