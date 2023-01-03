/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.infusionsoft.dataflow.templates;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubReadOptions;
import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubWriteOptions;
import com.google.datastore.v1.GqlQuery;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.RunQueryRequest;
import com.google.datastore.v1.RunQueryResponse;
import com.google.datastore.v1.client.Datastore;
import com.google.datastore.v1.client.DatastoreException;
import com.infusionsoft.dataflow.utils.DatastoreUtils;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A template that filters pubsub triggers so we only hit GAE with the ones we actually need to do
 * something with.
 *
 * <p>Used by strategy-engine-api
 *
 * <p>Deploy to sand: mvn compile exec:java
 * -Dexec.mainClass=com.infusionsoft.dataflow.templates.FilterPubsubTriggers
 * -Dexec.args="--project=is-strategy-engine-api-sand
 * --stagingLocation=gs://dataflow-is-strategy-engine-api-sand/staging
 * --templateLocation=gs://dataflow-is-strategy-engine-api-sand/templates/filter
 * --runner=DataflowRunner --serviceAccount=is-strategy-engine-api-sand@appspot.gserviceaccount.com
 * --datastoreProjectId=is-strategy-engine-api-sand"
 *
 * <p>projects/is-tactics-api-sand/topics/v1.contact-events
 * projects/is-strategy-engine-api-sand/topics/v1.filtered
 *
 * <p>Deploy to prod: mvn compile exec:java
 * -Dexec.mainClass=com.infusionsoft.dataflow.templates.FilterPubsubTriggers
 * -Dexec.args="--project=is-strategy-engine-api-prod
 * --stagingLocation=gs://dataflow-is-strategy-engine-api-prod/staging
 * --templateLocation=gs://dataflow-is-strategy-engine-api-prod/templates/filter
 * --runner=DataflowRunner --serviceAccount=is-strategy-engine-api-prod@appspot.gserviceaccount.com
 * --datastoreProjectId=is-strategy-engine-api-prod"
 *
 * <p>projects/is-tactics-api-prod/topics/v1.contact-events
 * projects/is-strategy-engine-api-prod/topics/v1.filtered
 */
public class FilterPubsubTriggers {

  /**
   * Options supported by {@link FilterPubsubTriggers}.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options
      extends PipelineOptions, StreamingOptions, PubsubReadOptions, PubsubWriteOptions {

    @Description("GCP Project Id of where the datastore entities live")
    ValueProvider<String> getDatastoreProjectId();

    void setDatastoreProjectId(ValueProvider<String> datastoreProjectId);
  }

  public static class ExtractAndFilterEventsFn extends DoFn<String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(ExtractAndFilterEventsFn.class);

    private static final ObjectMapper OBJECT_MAPPER =
        new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private String projectId;

    @StartBundle
    public void startBundle(StartBundleContext context) {
      if (this.projectId != null) {
        return; // has been evaluated already
      }

      final Options options = context.getPipelineOptions().as(Options.class);
      projectId =
          (options.getDatastoreProjectId() == null ? null : options.getDatastoreProjectId().get());
      LOG.info("Enabling event filter [projectId: {}]", projectId);
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
      final String message = context.element();
      LOG.debug("processing... {}", message);

      final Map<String, Object> json = OBJECT_MAPPER.readValue(message, Map.class);
      final List<Map<String, Object>> events = (List<Map<String, Object>>) json.get("events");

      events.stream().forEach(map -> processEvent(context, map));
    }

    private void processEvent(ProcessContext context, Map<String, Object> json) {
      final String channelName = (String) json.get("channel_name");
      final String eventType = (String) json.get("event_type");
      final String accountId = (String) json.get("account_id");
      final String sourceType = (String) json.get("source_type");
      final String sourceId = (String) json.get("source_id");

      final Datastore datastore =
          DatastoreUtils.getDatastore(context.getPipelineOptions(), projectId);

      if (hasTriggers(datastore, channelName, eventType, accountId, sourceType, sourceId)) {
        LOG.info("has triggers: {}", json);
        try {
          context.output(OBJECT_MAPPER.writeValueAsString(json));
        } catch (JsonProcessingException e) {
          LOG.error("FATAL! unable to re-emit: " + json, e);
        }
      } else {
        LOG.info("no triggers found: {}", json);
      }
    }

    private boolean hasTriggers(
        Datastore datastore,
        String channelName,
        String eventType,
        String accountId,
        @Nullable String sourceType,
        @Nullable String sourceId) {

      boolean triggers = true; // in case of error, assume there are triggers

      final StringBuilder gql =
          new StringBuilder("SELECT __key__ FROM Trigger")
              .append(" WHERE ")
              .append(String.format("accountId = '%s'", accountId))
              .append(" AND ")
              .append(String.format("channelName = '%s'", channelName))
              .append(" AND ")
              .append(String.format("eventType = '%s'", eventType));

      if (StringUtils.isNotBlank(sourceType) && StringUtils.isNotBlank(sourceId)) {
        gql.append(" AND ").append(String.format("sourceType = '%s'", sourceType));
        gql.append(" AND ").append(String.format("sourceIds = '%s'", sourceId));

      } else {
        gql.append(" AND ").append("sourceType = null");
      }

      LOG.debug(gql.toString());

      final RunQueryRequest request =
          RunQueryRequest.newBuilder()
              .setGqlQuery(
                  GqlQuery.newBuilder()
                      .setQueryString(gql.toString())
                      .setAllowLiterals(true)
                      .build())
              .build();

      try {
        final RunQueryResponse response = datastore.runQuery(request);
        final QueryResultBatch batch = response.getBatch();
        final int numTriggers = batch.getEntityResultsCount();

        triggers = numTriggers > 0;

      } catch (DatastoreException e) {
        LOG.error("Couldn't tell if there were any triggers or not... Assuming there are.", e);
      }

      return triggers;
    }
  }

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {

    // Parse the user options passed from the command-line
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    options.setStreaming(true);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(Options options) {
    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("Read Events", PubsubIO.readStrings().fromTopic(options.getPubsubReadTopic()))
        .apply(
            "Shard Events",
            Reshuffle.viaRandomKey()) // this ensures that we filter the events in parallel
        .apply("Filter Events", ParDo.of(new ExtractAndFilterEventsFn()))
        .apply("Write Events", PubsubIO.writeStrings().to(options.getPubsubWriteTopic()));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }
}
