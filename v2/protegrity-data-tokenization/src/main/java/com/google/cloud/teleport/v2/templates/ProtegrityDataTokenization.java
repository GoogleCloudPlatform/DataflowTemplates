package com.google.cloud.teleport.v2.templates;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.v2.options.ProtegrityDataTokenizationOptions;
import com.google.cloud.teleport.v2.utils.BigQuerySchema;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@link ProtegrityDataTokenization} pipeline.
 */
public class ProtegrityDataTokenization {

    /** Logger for class. */
    private static final Logger LOG = LoggerFactory.getLogger(ProtegrityDataTokenization.class);

    /** Gson object for JSON parsing. */
    private static final Gson GSON = new Gson();

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
        } catch(IOException e) {
            LOG.warn("Failed to retrieve schema for BigQuery.", e);
        }

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("readTextFromGCSFiles", TextIO.read().from(options.getInputGcsFilePattern()))
                .apply("parseJsonToMap", ParDo.of(new ParseJsonToMap()))
                .apply("writeToBigQueryTable", BigQueryIO.<Map<String, String>>write()
                        .to(options.getBigQueryTableName())
                        .withSchema(schema)
                        .withFormatFunction(
                                (Map<String, String> elem) -> {
                                    TableRow row = new TableRow();
                                    for (Map.Entry<String, String> entry: elem.entrySet()) {
                                        row.set(entry.getKey(), entry.getValue());
                                    }
                                    return row;
                                })
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

        return pipeline.run();
    }

    static class ParseJsonToMap extends DoFn<String, Map<String, String>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String json = c.element();
            Map<String, String> map = GSON.fromJson(json, Map.class);
            c.output(map);
        }
    }
}
