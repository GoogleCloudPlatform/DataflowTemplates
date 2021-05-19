package com.google.cloud.teleport.v2.elasticsearch.options;

import com.google.cloud.teleport.v2.transforms.CsvConverters;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link CsvToElasticsearchOptions} class provides the custom execution options passed by the
 * executor at the command-line.
 */
public interface CsvToElasticsearchOptions
        extends CsvConverters.CsvPipelineOptions, ElasticsearchOptions {

    @Description("Deadletter table for failed inserts in form: <project-id>:<dataset>.<table>")
    @Validation.Required
    String getDeadletterTable();

    void setDeadletterTable(String deadletterTable);
}
