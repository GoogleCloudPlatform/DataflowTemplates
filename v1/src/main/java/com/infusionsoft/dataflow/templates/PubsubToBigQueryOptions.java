package com.infusionsoft.dataflow.templates;

import com.google.cloud.teleport.templates.common.PubsubConverters;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;

/**
 * The {@link PubsubToBigQueryOptions} class provides the custom execution options passed by
 * the executor at the command-line.
 */
public interface PubsubToBigQueryOptions
        extends PubsubConverters.PubsubReadOptions, BigQueryOptions {}
