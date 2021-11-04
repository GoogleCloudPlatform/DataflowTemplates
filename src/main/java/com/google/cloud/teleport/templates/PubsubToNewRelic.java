package com.google.cloud.teleport.templates;

import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.newrelic.dtos.NewRelicLogRecord;
import com.google.cloud.teleport.newrelic.NewRelicPipeline;
import com.google.cloud.teleport.newrelic.config.NewRelicConfig;
import com.google.cloud.teleport.newrelic.config.PubsubToNewRelicPipelineOptions;
import com.google.cloud.teleport.newrelic.ptransforms.ReadMessagesFromPubSub;
import com.google.cloud.teleport.newrelic.ptransforms.NewRelicIO;
import com.google.cloud.teleport.newrelic.utils.ConfigHelper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * The {@link PubsubToNewRelic} pipeline is a streaming pipeline which ingests data from Cloud
 * Pub/Sub, converts the output to {@link NewRelicLogRecord}s and writes those records
 * into NewRelic's API endpoint.
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
 * <pre>
 * # Set the pipeline vars
 * PROJECT_ID=PROJECT ID HERE
 * BUCKET_NAME=BUCKET NAME HERE
 * PIPELINE_BUCKET_FOLDER=gs://${BUCKET_NAME}/dataflow/pipelines/pubsub-to-newrelic
 *
 * # Set the runner
 * RUNNER=DataflowRunner
 *
 * # Build the template
 * mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.teleport.templates.PubsubToNewRelic \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=${PIPELINE_BUCKET_FOLDER}/staging \
 * --tempLocation=${PIPELINE_BUCKET_FOLDER}/temp \
 * --templateLocation=${PIPELINE_BUCKET_FOLDER}/template/PubsubToNewRelic \
 * --runner=${RUNNER}
 * "
 *
 * # Execute the template
 * JOB_NAME=pubsub-to-NewRelic-$USER-`date +"%Y%m%d-%H%M%S%z"`
 * BATCH_COUNT=1
 * PARALLELISM=5
 * REGION=us-west1
 *
 * INPUT_SUB_NAME=SUB NAME WHERE LOGS ARE
 *
 * NR_LOG_ENDPOINT=https://log-api.newrelic.com/log/v1
 *
 * NR_LICENSE_KEY=YOUR NEW RELIC LICENSE KEY
 *
 *
 * # Execute the templated pipeline:
 * gcloud dataflow jobs run ${JOB_NAME} \
 * --gcs-location=${PIPELINE_FOLDER}/template/PubsubToNewRelic \
 * --region=${REGION} \
 * --parameters \
 * "inputSubscription=projects/${PROJECT_ID}/subscriptions/${INPUT_SUB_NAME},\
 * licenseKey=${NR_LICENSE_KEY},\
 * logsApiUrl=${NR_LOG_ENDPOINT},\
 * batchCount=${BATCH_COUNT},\
 * parallelism=${PARALLELISM},\
 * disableCertificateValidation=false,\
 * useCompression=true"
 */
public class PubsubToNewRelic {

    public static final String PLUGIN_VERSION = "1.0.0";

    /**
     * String/String Coder for FailsafeElement.
     */
    public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
            FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    /**
     * The main entry-point for pipeline execution. This method will start the pipeline but will not
     * wait for it's execution to finish. If blocking execution is required, use the {@link
     * PubsubToNewRelic#run(PubsubToNewRelicPipelineOptions)} method to start the pipeline and invoke {@code
     * result.waitUntilFinish()} on the {@link PipelineResult}.
     *
     * @param args The command-line args passed by the executor.
     */
    public static void main(String[] args) {

        final PubsubToNewRelicPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(PubsubToNewRelicPipelineOptions.class);

        run(options);
    }

    public static PipelineResult run(PubsubToNewRelicPipelineOptions options) {
        final Pipeline pipeline = Pipeline.create(options);

        final NewRelicConfig newRelicConfig = NewRelicConfig.fromPipelineOptions(options);
        ConfigHelper.logConfiguration(newRelicConfig);

        final NewRelicPipeline nrPipeline = new NewRelicPipeline(
                pipeline,
                new ReadMessagesFromPubSub(options.getInputSubscription()),
                new NewRelicIO(newRelicConfig)
        );

        return nrPipeline.run();
    }
}
