package com.google.cloud.teleport.v2.elasticsearch.options;

import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.cloud.teleport.v2.transforms.ElasticsearchTransforms;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * The {@link BigQueryToElasticsearchReadOptions} class provides the custom execution options
 * passed by the executor at the command-line.
 */
public interface BigQueryToElasticsearchReadOptions
        extends PipelineOptions,
        BigQueryConverters.BigQueryReadOptions,
        ElasticsearchTransforms.WriteToElasticsearchOptions {}
