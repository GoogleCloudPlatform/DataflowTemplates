package com.google.cloud.teleport.elasticsearch.options;

import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link PubSubToElasticsearchOptions} class provides the custom execution options passed by
 * the executor at the command-line.
 *
 * <p>Inherits standard configuration options, options from {@link
 * JavascriptTextTransformer.JavascriptTextTransformerOptions}, and options from {@link ElasticsearchOptions}.
 */
public interface PubSubToElasticsearchOptions
        extends JavascriptTextTransformer.JavascriptTextTransformerOptions, PipelineOptions, ElasticsearchOptions {

    @Description(
            "The Cloud Pub/Sub subscription to consume from. "
                    + "The name should be in the format of "
                    + "projects/<project-id>/subscriptions/<subscription-name>.")
    String getInputSubscription();

    void setInputSubscription(String inputSubscription);

    @Description(
            "The dead-letter table to output to within BigQuery in <project-id>:<dataset>.<table> "
                    + "format.")
    @Validation.Required
    String getDeadletterTable();

    void setDeadletterTable(String deadletterTable);
}
