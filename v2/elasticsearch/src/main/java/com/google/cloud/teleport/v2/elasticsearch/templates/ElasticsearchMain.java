package com.google.cloud.teleport.v2.elasticsearch.templates;

import com.google.cloud.teleport.v2.elasticsearch.options.ElasticsearchOptions;
import com.google.cloud.teleport.v2.elasticsearch.utils.ApplicationsEnum;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * The main class of Elasticsearch module, that runs specific
 * class according to application name passed via option "Application".
 */
public class ElasticsearchMain {

    public static void main(String[] args) {
        ElasticsearchOptions elasticsearchMainOptions =
                PipelineOptionsFactory.fromArgs(args)
                        .withoutStrictParsing()
                        .as(ElasticsearchOptions.class);

        if (ApplicationsEnum.CSV_TO_ELASTICSEARCH.toString()
                .equalsIgnoreCase(elasticsearchMainOptions.getApplication())) {
            CsvToElasticsearch.main(args);
        } else if (ApplicationsEnum.BIGQUERY_TO_ELASTICSEARCH.toString()
                .equalsIgnoreCase(elasticsearchMainOptions.getApplication())) {
            BigQueryToElasticsearch.main(args);
        } else if (ApplicationsEnum.PUBSUB_TO_ELASTICSEARCH.toString()
                .equalsIgnoreCase(elasticsearchMainOptions.getApplication())) {
            PubSubToElasticsearch.main(args);
        }
    }

}
