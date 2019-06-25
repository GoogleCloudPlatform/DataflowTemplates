package com.infusionsoft.dataflow.templates;

import static com.google.cloud.teleport.templates.TextToBigQueryStreaming.wrapBigQueryInsertError;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.templates.common.BigQueryConverters.FailsafeJsonToTableRow;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.values.FailsafeElement;
import java.nio.charset.StandardCharsets;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link com.google.cloud.teleport.templates.PubSubToBigQuery} pipeline is a streaming pipeline which ingests data in JSON format
 * from Cloud Pub/Sub, executes a UDF, and outputs the resulting records to BigQuery. Any errors
 * which occur in the transformation of the data or execution of the UDF will be output to a
 * separate errors table in BigQuery. The errors table will be created if it does not exist prior to
 * execution. Both output and error tables are specified by the user as template parameters.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>The Pub/Sub topic exists.
 *   <li>The BigQuery output table exists.
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT_ID=PROJECT ID HERE
 * BUCKET_NAME=BUCKET NAME HERE
 * PIPELINE_FOLDER=gs://${BUCKET_NAME}/dataflow/pipelines/pubsub-to-bigquery
 * USE_SUBSCRIPTION=true or false depending on whether the pipeline should read
 *                  from a Pub/Sub Subscription or a Pub/Sub Topic.
 *
 * # Set the runner
 * RUNNER=DataflowRunner
 *
 * # Build the template
 * mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.teleport.templates.PubSubToBigQuery \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=${PIPELINE_FOLDER}/staging \
 * --tempLocation=${PIPELINE_FOLDER}/temp \
 * --templateLocation=${PIPELINE_FOLDER}/template \
 * --runner=${RUNNER}
 * --useSubscription=${USE_SUBSCRIPTION}
 * "
 *
 * # Execute the template
 * JOB_NAME=pubsub-to-bigquery-$USER-`date +"%Y%m%d-%H%M%S%z"`
 *
 * # Execute a pipeline to read from a Topic.
 * gcloud dataflow jobs run ${JOB_NAME} \
 * --gcs-location=${PIPELINE_FOLDER}/template \
 * --zone=us-east1-d \
 * --parameters \
 * "inputTopic=projects/${PROJECT_ID}/topics/input-topic-name,\
 * outputTableSpec=${PROJECT_ID}:dataset-id.output-table,\
 * outputDeadletterTable=${PROJECT_ID}:dataset-id.deadletter-table"
 *
 * # Execute a pipeline to read from a Subscription.
 * gcloud dataflow jobs run ${JOB_NAME} \
 * --gcs-location=${PIPELINE_FOLDER}/template \
 * --zone=us-east1-d \
 * --parameters \
 * "inputSubscription=projects/${PROJECT_ID}/subscriptions/input-subscription-name,\
 * outputTableSpec=${PROJECT_ID}:dataset-id.output-table,\
 * outputDeadletterTable=${PROJECT_ID}:dataset-id.deadletter-table"
 * </pre>
 */
public class PubsubToBigQuery {

    /** The log to output status messages to. */
    private static final Logger LOG = LoggerFactory
        .getLogger(com.google.cloud.teleport.templates.PubSubToBigQuery.class);

    /** The tag for the main output for the UDF. */
    public static final TupleTag<FailsafeElement<PubsubMessage, String>> UDF_OUT =
        new TupleTag<FailsafeElement<PubsubMessage, String>>() {};

    /** The tag for the main output of the json transformation. */
    public static final TupleTag<TableRow> TRANSFORM_OUT = new TupleTag<TableRow>() {};

    /** The tag for the dead-letter output of the udf. */
    public static final TupleTag<FailsafeElement<PubsubMessage, String>> UDF_DEADLETTER_OUT =
        new TupleTag<FailsafeElement<PubsubMessage, String>>() {};

    /** The tag for the dead-letter output of the json to table row transform. */
    public static final TupleTag<FailsafeElement<PubsubMessage, String>> TRANSFORM_DEADLETTER_OUT =
        new TupleTag<FailsafeElement<PubsubMessage, String>>() {};

    /** The default suffix for error tables if dead letter table is not specified. */
    public static final String DEFAULT_DEADLETTER_TABLE_SUFFIX = "_error_records";

    /** Pubsub message/string coder for pipeline. */
    public static final FailsafeElementCoder<PubsubMessage, String> CODER =
        FailsafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), StringUtf8Coder.of());

    /** String/String Coder for FailsafeElement. */
    public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
        FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    /**
     * The {@link com.google.cloud.teleport.templates.PubSubToBigQuery.Options} class provides the custom execution options passed by the executor at the
     * command-line.
     */
    public interface Options extends PipelineOptions, JavascriptTextTransformerOptions,
        DataflowPipelineOptions {
        @Description("Table spec to write the output to")
        ValueProvider<String> getOutputTableSpec();

        void setOutputTableSpec(ValueProvider<String> value);

        @Description("Pub/Sub topic to read the input from")
        ValueProvider<String> getInputTopic();

        void setInputTopic(ValueProvider<String> value);

        @Description(
            "The Cloud Pub/Sub subscription to consume from. "
                + "The name should be in the format of "
                + "projects/<project-id>/subscriptions/<subscription-name>.")
        ValueProvider<String> getInputSubscription();

        void setInputSubscription(ValueProvider<String> value);

        @Description(
            "This determines whether the template reads from " + "a pub/sub subscription or a topic")
        @Default.Boolean(false)
        Boolean getUseSubscription();

        void setUseSubscription(Boolean value);

        @Description(
            "The dead-letter table to output to within BigQuery in <project-id>:<dataset>.<table> "
                + "format. If it doesn't exist, it will be created during pipeline execution.")
        ValueProvider<String> getOutputDeadletterTable();

        void setOutputDeadletterTable(ValueProvider<String> value);
    }

    /**
     * The main entry-point for pipeline execution. This method will start the pipeline but will not
     * wait for it's execution to finish. If blocking execution is required, use the {@link
     * com.google.cloud.teleport.templates.PubSubToBigQuery#run(com.google.cloud.teleport.templates.PubSubToBigQuery.Options)} method to start the pipeline and invoke {@code
     * result.waitUntilFinish()} on the {@link PipelineResult}.
     *
     * @param args The command-line args passed by the executor.
     */
    public static void main(String[] args) {
        com.google.cloud.teleport.templates.PubSubToBigQuery.Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(
            com.google.cloud.teleport.templates.PubSubToBigQuery.Options.class);

        run(options);
    }

    /**
     * Runs the pipeline to completion with the specified options. This method does not wait until the
     * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
     * object to block until the pipeline is finished running if blocking programmatic execution is
     * required.
     *
     * @param options The execution options.
     * @return The pipeline result.
     */
    public static PipelineResult run(
        com.google.cloud.teleport.templates.PubSubToBigQuery.Options options) {

        Pipeline pipeline = Pipeline.create(options);

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForType(CODER.getEncodedTypeDescriptor(), CODER);

        /*
         * Steps:
         *  1) Read messages in from Pub/Sub
         *  2) Transform the PubsubMessages into TableRows
         *     - Transform message payload via UDF
         *     - Convert UDF result to TableRow objects
         *  3) Write successful records out to BigQuery
         */

        PCollection<PubsubMessage> messages = null;
        if (options.getUseSubscription()) {
            messages =
                pipeline.apply(
                    "ReadPubSubSubscription",
                    PubsubIO.readMessagesWithAttributes()
                        .fromSubscription(options.getInputSubscription()));
        } else {
            messages =
                pipeline.apply(
                    "ReadPubSubTopic",
                    PubsubIO.readMessagesWithAttributes().fromTopic(options.getInputTopic()));
        }
        PCollectionTuple convertedTableRows =
            messages
                /*
                 * Step #2: Transform the PubsubMessages into TableRows
                 */
                .apply("ConvertMessageToTableRow", new PubsubMessageToTableRow(options));

        /*
         * Step #3: Write the successful records out to BigQuery
         */
        ValueProvider<String> valueProvider = null;

        WriteResult writeResult =
            convertedTableRows
                .get(TRANSFORM_OUT)
                .apply(
                    "WriteSuccessfulRecords",
                    BigQueryIO.writeTableRows()
                        .withoutValidation()
                        .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                        .withExtendedErrorInfo()
                        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                        .to((SerializableFunction<ValueInSingleWindow<TableRow>, TableDestination>) element -> {
                            TableRow row = element.getValue();
                            String event = row.get("event").toString();
                            String projectAndDataset = "is-events-dataflow-sand:crm_prod";
                            String table_name = null;
                            if (event.equals("user_login")) {
                                table_name = "user_login";
                            }
                            else if (event.equals("user_logout")) {
                                table_name = "user_logout";
                            }
                            else if (event.equals("api_call_made")) {
                                table_name = "user_logout";
                            }
                            String destination = String.format("%s.%s", projectAndDataset, table_name);
                            return new TableDestination(destination, null);
                        }));

        /*

         */

        PCollection<FailsafeElement<String, String>> failedInserts =
            writeResult
                .getFailedInsertsWithErr()
                .apply(
                    "WrapInsertionErrors",
                    MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                        .via((BigQueryInsertError e) -> wrapBigQueryInsertError(e)))
                .setCoder(FAILSAFE_ELEMENT_CODER);

            return pipeline.run();
        }


    static class PubsubMessageToTableRow
        extends PTransform<PCollection<PubsubMessage>, PCollectionTuple> {

        private final com.google.cloud.teleport.templates.PubSubToBigQuery.Options options;

        PubsubMessageToTableRow(com.google.cloud.teleport.templates.PubSubToBigQuery.Options options) {
            this.options = options;
        }

        @Override
        public PCollectionTuple expand(PCollection<PubsubMessage> input) {

            PCollectionTuple udfOut =
                input
                    // Map the incoming messages into FailsafeElements so we can recover from failures
                    // across multiple transforms.
                    .apply("MapToRecord", ParDo.of(new PubsubMessageToFailsafeElementFn()))
                    .apply(
                        "InvokeUDF",
                        FailsafeJavascriptUdf.<PubsubMessage>newBuilder()
                            .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                            .setFunctionName(options.getJavascriptTextTransformFunctionName())
                            .setSuccessTag(UDF_OUT)
                            .setFailureTag(UDF_DEADLETTER_OUT)
                            .build());

            // Convert the records which were successfully processed by the UDF into TableRow objects.
            PCollectionTuple jsonToTableRowOut =
                udfOut
                    .get(UDF_OUT)
                    .apply(
                        "JsonToTableRow",
                        FailsafeJsonToTableRow.<PubsubMessage>newBuilder()
                            .setSuccessTag(TRANSFORM_OUT)
                            .setFailureTag(TRANSFORM_DEADLETTER_OUT)
                            .build());

            // Re-wrap the PCollections so we can return a single PCollectionTuple
            return PCollectionTuple.of(UDF_OUT, udfOut.get(UDF_OUT))
                .and(UDF_DEADLETTER_OUT, udfOut.get(UDF_DEADLETTER_OUT))
                .and(TRANSFORM_OUT, jsonToTableRowOut.get(TRANSFORM_OUT))
                .and(TRANSFORM_DEADLETTER_OUT, jsonToTableRowOut.get(TRANSFORM_DEADLETTER_OUT));
        }
    }

    static class PubsubMessageToFailsafeElementFn
        extends DoFn<PubsubMessage, FailsafeElement<PubsubMessage, String>> {
        @ProcessElement
        public void processElement(ProcessContext context) {
            PubsubMessage message = context.element();
            context.output(
                FailsafeElement.of(message, new String(message.getPayload(), StandardCharsets.UTF_8)));
        }
    }

}
