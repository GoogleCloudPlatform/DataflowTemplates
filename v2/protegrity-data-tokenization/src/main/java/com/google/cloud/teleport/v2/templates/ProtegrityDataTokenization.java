package com.google.cloud.teleport.v2.templates;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.options.ProtegrityDataTokenizationOptions;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.cloud.teleport.v2.transforms.ErrorConverters;
import com.google.cloud.teleport.v2.utils.BigQuerySchema;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.Gson;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.fromTableSchema;


/**
 * The {@link ProtegrityDataTokenization} pipeline.
 */
public class ProtegrityDataTokenization {

    /**
     * Logger for class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(ProtegrityDataTokenization.class);

    /**
     * Gson object for JSON parsing.
     */
    private static final Gson GSON = new Gson();

    /**
     * String/String Coder for FailsafeElement.
     */
    private static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
            FailsafeElementCoder.of(
                    NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of()));

    /**
     * The tag for the main output of the json transformation.
     */
    static final TupleTag<TableRow> TRANSFORM_OUT = new TupleTag<TableRow>() {
    };

    /**
     * The tag for the dead-letter output of the json to table row transform.
     */
    static final TupleTag<FailsafeElement<Map<String, String>, String>>
            TRANSFORM_DEADLETTER_OUT = new TupleTag<FailsafeElement<Map<String, String>, String>>() {
    };

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

        run(options);
    }

    /**
     * Runs the pipeline to completion with the specified options.
     *
     * @param options The execution options.
     * @return The pipeline result.
     */
    public static PipelineResult run(ProtegrityDataTokenizationOptions options) {
        TableSchema schema = new TableSchema();
        try {
            schema = new BigQuerySchema(options.getBigQueryDataSchemaGcsPath(), StandardCharsets.UTF_8).getTableSchema();
        } catch (IOException e) {
            LOG.warn("Failed to retrieve schema for BigQuery.", e);
        }
        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        PCollection<FailsafeElement<Map<String, String>, String>> failsafeJsonPCollection = pipeline
                .apply("readTextFromGCSFiles", TextIO.read().from(options.getInputGcsFilePattern()))
                //TODO think about ROW .apply("Parse JSON to Beam Rows", JsonToRow.withSchema(fromTableSchema(schema)))
                .apply("beamRowToFailsafeRow", ParDo.of(new ParseJsonToMap()));

        if (options.getBigQueryTableName() != null) {
            WriteResult writeResult = writeToBigQuery(failsafeJsonPCollection, options.getBigQueryTableName(), schema);
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

    private static WriteResult writeToBigQuery(PCollection<FailsafeElement<Map<String, String>, String>> input, String bigQueryTableName, TableSchema schema) {
        PCollectionTuple convertedTableRows = input.apply(
                "JsonToTableRow",
                BigQueryConverters.FailsafeJsonToTableRow.<Map<String, String>>newBuilder()
                        .setSuccessTag(TRANSFORM_OUT)
                        .setFailureTag(TRANSFORM_DEADLETTER_OUT)
                        .build());

        return convertedTableRows.get(TRANSFORM_OUT)
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

    static class ParseJsonToMap extends DoFn<String, FailsafeElement<Map<String, String>, String>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String json = c.element();
            TypeToken<Map<String, String>> type = new TypeToken<Map<String, String>>() {
            };
            Map<String, String> map = GSON.fromJson(json, type.getType());
            c.output(FailsafeElement.of(map, json));
        }
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
