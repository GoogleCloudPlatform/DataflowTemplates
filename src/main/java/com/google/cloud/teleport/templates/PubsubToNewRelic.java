/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.templates;

import com.google.cloud.teleport.newrelic.NewRelicPipeline;
import com.google.cloud.teleport.newrelic.config.NewRelicConfig;
import com.google.cloud.teleport.newrelic.config.PubsubToNewRelicPipelineOptions;
import com.google.cloud.teleport.newrelic.dtos.NewRelicLogRecord;
import com.google.cloud.teleport.newrelic.ptransforms.NewRelicIO;
import com.google.cloud.teleport.newrelic.ptransforms.ReadMessagesFromPubSub;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link PubsubToNewRelic} pipeline is a streaming pipeline which ingests data from Cloud
 * Pub/Sub, converts the output to {@link NewRelicLogRecord}s and writes those records into
 * NewRelic's API endpoint.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>The source Pub/Sub subscription exists.
 *   <li>API end-point is routable from the VPC where the Dataflow job executes.
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <p>
 *
 * <p># Set the pipeline vars PROJECT_ID=PROJECT ID HERE BUCKET_NAME=BUCKET NAME HERE
 * PIPELINE_BUCKET_FOLDER=gs://${BUCKET_NAME}/dataflow/pipelines/pubsub-to-newrelic
 *
 * <p># Set the runner RUNNER=DataflowRunner
 *
 * <p># Build the template mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.teleport.templates.PubsubToNewRelic \
 * -Dexec.cleanupDaemonThreads=false \ -Dexec.args=" \ --project=${PROJECT_ID} \
 * --stagingLocation=${PIPELINE_BUCKET_FOLDER}/staging \
 * --tempLocation=${PIPELINE_BUCKET_FOLDER}/temp \
 * --templateLocation=${PIPELINE_BUCKET_FOLDER}/template/PubsubToNewRelic \ --runner=${RUNNER} "
 *
 * <p># Execute the template JOB_NAME=pubsub-to-NewRelic-$USER-`date +"%Y%m%d-%H%M%S%z"`
 * BATCH_COUNT=10 FLUSH_DELAY=2 PARALLELISM=5 REGION=us-west1
 *
 * <p>INPUT_SUB_NAME=SUB NAME WHERE LOGS ARE
 *
 * <p>NR_LOG_ENDPOINT=https://log-api.newrelic.com/log/v1
 *
 * <p>NR_LICENSE_KEY=YOUR NEW RELIC LICENSE KEY
 *
 * <p>
 *
 * <p># Execute the templated pipeline: gcloud dataflow jobs run ${JOB_NAME} \
 * --gcs-location=${PIPELINE_FOLDER}/template/PubsubToNewRelic \ --region=${REGION} \ --parameters \
 * "inputSubscription=projects/${PROJECT_ID}/subscriptions/${INPUT_SUB_NAME},\
 * licenseKey=${NR_LICENSE_KEY},\ logsApiUrl=${NR_LOG_ENDPOINT},\ batchCount=${BATCH_COUNT},\
 * flushDelay=${FLUSH_DELAY},\ parallelism=${PARALLELISM},\ disableCertificateValidation=false,\
 * useCompression=true"
 */
public class PubsubToNewRelic {

  public static final String PLUGIN_VERSION = "1.0.0";

  private static final Logger LOG = LoggerFactory.getLogger(PubsubToNewRelic.class);

  /**
   * The main entry-point for pipeline execution. This method will start the pipeline but will not
   * wait for it's execution to finish. If blocking execution is required, use the {@link
   * PubsubToNewRelic#run(PubsubToNewRelicPipelineOptions)} method to start the pipeline and invoke
   * {@code result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line args passed by the executor.
   */
  public static void main(String[] args) {

    final PubsubToNewRelicPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(PubsubToNewRelicPipelineOptions.class);

    run(options);
  }

  public static PipelineResult run(PubsubToNewRelicPipelineOptions options) {
    final Pipeline pipeline = Pipeline.create(options);

    final NewRelicConfig newRelicConfig = NewRelicConfig.fromPipelineOptions(options);
    LOG.debug("Using configuration: {}", newRelicConfig);

    final NewRelicPipeline nrPipeline =
        new NewRelicPipeline(
            pipeline,
            new ReadMessagesFromPubSub(options.getInputSubscription()),
            new NewRelicIO(newRelicConfig));

    return nrPipeline.run();
  }
}
