package com.google.cloud.teleport.v2.elasticsearch.options;

import com.google.cloud.teleport.v2.transforms.BigQueryConverters;

/**
 * The {@link BigQueryToElasticsearchReadOptions} class provides the custom execution options
 * passed by the executor at the command-line.
 */
public interface BigQueryToElasticsearchReadOptions
        extends BigQueryConverters.BigQueryReadOptions,
        ElasticsearchOptions {
}
