package com.google.cloud.teleport.templates;

import static com.google.cloud.teleport.templates.TextToBigQueryStreaming.wrapBigQueryInsertError;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.templates.common.BigQueryConverters.FailsafeJsonToTableRow;
import com.google.cloud.teleport.templates.common.ErrorConverters;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.util.ResourceUtils;
import com.google.cloud.teleport.util.ValueProviderUtils;
import com.google.cloud.teleport.values.FailsafeElement;
import com.google.common.annotations.VisibleForTesting;
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
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link PubSubToBigQuery} pipeline is a streaming pipeline which ingests data in JSON format
 * from Cloud Pub/Sub, executes a UDF, and outputs the resulting records to BigQuery. Any errors
 * which occur in the transformation of the data or execution of the UDF will be output to a
 * separate errors table in BigQuery. The errors table will be created if it does not exist prior to
 * execution. Both output and error tables are specified by the user as template parameters.
 *
 * <p>
 * <b>Pipeline Requirements</b>
 *
 * <ul>
 * <li>The Pub/Sub topic exists.
 * <li>The BigQuery output table exists.
 * </ul>
 *
 * <p>
 * <b>Example Usage</b>
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
public class PubSubToBigQueryHPI {

    /** The log to output status messages to. */
    private static final Logger LOG = LoggerFactory.getLogger(PubSubToBigQueryHPI.class);

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
     * The {@link Options} class provides the custom execution options passed by the executor at the
     * command-line.
     */
    public interface Options extends PipelineOptions, JavascriptTextTransformerOptions {

        @Description("Pub/Sub topic to read the input from")
        ValueProvider<String> getInputTopic();

        void setInputTopic(ValueProvider<String> value);

        @Description("The Cloud Pub/Sub subscription to consume from. "
                + "The name should be in the format of "
                + "projects/<project-id>/subscriptions/<subscription-name>.")
        ValueProvider<String> getInputSubscription();

        void setInputSubscription(ValueProvider<String> value);

        @Description("This determines whether the template reads from "
                + "a pub/sub subscription or a topic")
        @Default.Boolean(false)
        Boolean getUseSubscription();

        void setUseSubscription(Boolean value);

        @Description("The name of the attribute which will contain the table name to route the message to.")
        @Required
        String getTableNameAttr();

        void setTableNameAttr(String value);

        @Description("The project where output table is stored.")
        @Required
        String getOutputTableProject();

        void setOutputTableProject(String value);

        @Description("The dataset where output table is stored.")
        @Required
        String getOutputTableDataset();

        void setOutputTableDataset(String value);

        @Description("The name deadletter table.")
        @Required
        String getOutputDeadletterTable();

        void setOutputDeadletterTable(String value);
    }

    /**
     * The main entry-point for pipeline execution. This method will start the pipeline but will not
     * wait for it's execution to finish. If blocking execution is required, use the
     * {@link PubSubToBigQuery#run(Options)} method to start the pipeline and invoke {@code
     * result.waitUntilFinish()} on the {@link PipelineResult}.
     *
     * @param args The command-line args passed by the executor.
     */
    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        run(options);
    }

    /**
     * Runs the pipeline to completion with the specified options. This method does not wait until
     * the pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the
     * result object to block until the pipeline is finished running if blocking programmatic
     * execution is required.
     *
     * @param options The execution options.
     * @return The pipeline result.
     */
    public static PipelineResult run(Options options) {

        Pipeline pipeline = Pipeline.create(options);

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForType(CODER.getEncodedTypeDescriptor(), CODER);

        // Retrieve non-serializable parameters
        String tableNameAttr = options.getTableNameAttr();
        String outputTableProject = options.getOutputTableProject();
        String outputTableDataset = options.getOutputTableDataset();

        /*
         * Steps: 1) Read messages in from Pub/Sub 2) Transform the PubsubMessages into TableRows -
         * Transform message payload via UDF - Convert UDF result to TableRow objects 3) Write
         * successful records out to BigQuery 4) Write failed records out to BigQuery
         */

        /*
         * Step #1: Read messages in from Pub/Sub Either from a Subscription or Topic
         */

        PCollection<PubsubMessage> messages = null;

        if (options.getUseSubscription()) {
            messages = pipeline.apply("ReadPubSubSubscription",
                    PubsubIO.readMessagesWithAttributes().withIdAttribute("bq_id")
                            .fromSubscription(options.getInputSubscription()));
        } else {
            messages = pipeline.apply("ReadPubSubTopic", PubsubIO.readMessagesWithAttributes()
                    .withIdAttribute("bq_id").fromTopic(options.getInputTopic()));
        }

        /*
         * Step #2: Write the successful records out to BigQuery
         */
        WriteResult writeResult = messages.apply("WriteSuccessfulRecords",
                BigQueryIO.<PubsubMessage>write().withoutValidation()
                        .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(WriteDisposition.WRITE_APPEND).withExtendedErrorInfo()
                        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                        .to(input -> getTableDestination(input, tableNameAttr, outputTableProject,
                                outputTableDataset))
                        .withFormatFunction((PubsubMessage msg) -> convertJsonToTableRow(
                                new String(msg.getPayload(), StandardCharsets.UTF_8))));

        /*
         * Step 2 Contd. Elements that failed inserts into BigQuery are extracted and converted to
         * FailsafeElement
         */
        PCollection<FailsafeElement<String, String>> failedInserts =
                writeResult.getFailedInsertsWithErr()
                        .apply("WrapInsertionErrors",
                                MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                                        .via((BigQueryInsertError e) -> wrapBigQueryInsertError(e)))
                        .setCoder(FAILSAFE_ELEMENT_CODER);



        /*
         * Step #3: Write records that failed table row transformation or conversion out to BigQuery
         * deadletter table.
         */
        failedInserts.apply("WriteFailedRecords", ErrorConverters.WriteStringMessageErrors
                .newBuilder()
                .setErrorRecordsTable(
                        ValueProvider.StaticValueProvider.of(options.getOutputDeadletterTable()))
                .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson()).build());

        return pipeline.run();
    }

    /**
     * The {@link PubsubMessageToTableRow} class is a {@link PTransform} which transforms incoming
     * {@link PubsubMessage} objects into {@link TableRow} objects for insertion into BigQuery while
     * applying an optional UDF to the input. The executions of the UDF and transformation to
     * {@link TableRow} objects is done in a fail-safe way by wrapping the element with it's
     * original payload inside the {@link FailsafeElement} class. The
     * {@link PubsubMessageToTableRow} transform will output a {@link PCollectionTuple} which
     * contains all output and dead-letter {@link PCollection}.
     *
     * <p>
     * The {@link PCollectionTuple} output will contain the following {@link PCollection}:
     *
     * <ul>
     * <li>{@link PubSubToBigQuery#UDF_OUT} - Contains all {@link FailsafeElement} records
     * successfully processed by the optional UDF.
     * <li>{@link PubSubToBigQuery#UDF_DEADLETTER_OUT} - Contains all {@link FailsafeElement}
     * records which failed processing during the UDF execution.
     * <li>{@link PubSubToBigQuery#TRANSFORM_OUT} - Contains all records successfully converted from
     * JSON to {@link TableRow} objects.
     * <li>{@link PubSubToBigQuery#TRANSFORM_DEADLETTER_OUT} - Contains all {@link FailsafeElement}
     * records which couldn't be converted to table rows.
     * </ul>
     */
    static class PubsubMessageToTableRow
            extends PTransform<PCollection<PubsubMessage>, PCollectionTuple> {

        private final Options options;

        PubsubMessageToTableRow(Options options) {
            this.options = options;
        }

        @Override
        public PCollectionTuple expand(PCollection<PubsubMessage> input) {

            PCollectionTuple udfOut = input
                    // Map the incoming messages into FailsafeElements so we can
                    // recover from
                    // failures
                    // across multiple transforms.
                    .apply("MapToRecord", ParDo.of(new PubsubMessageToFailsafeElementFn()))
                    .apply("InvokeUDF", FailsafeJavascriptUdf.<PubsubMessage>newBuilder()
                            .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                            .setFunctionName(options.getJavascriptTextTransformFunctionName())
                            .setSuccessTag(UDF_OUT).setFailureTag(UDF_DEADLETTER_OUT).build());

            // Convert the records which were successfully processed by the UDF into
            // TableRow
            // objects.
            PCollectionTuple jsonToTableRowOut = udfOut.get(UDF_OUT).apply("JsonToTableRow",
                    FailsafeJsonToTableRow.<PubsubMessage>newBuilder().setSuccessTag(TRANSFORM_OUT)
                            .setFailureTag(TRANSFORM_DEADLETTER_OUT).build());

            // Re-wrap the PCollections so we can return a single PCollectionTuple
            return PCollectionTuple.of(UDF_OUT, udfOut.get(UDF_OUT))
                    .and(UDF_DEADLETTER_OUT, udfOut.get(UDF_DEADLETTER_OUT))
                    .and(TRANSFORM_OUT, jsonToTableRowOut.get(TRANSFORM_OUT))
                    .and(TRANSFORM_DEADLETTER_OUT, jsonToTableRowOut.get(TRANSFORM_DEADLETTER_OUT));
        }
    }

    /**
     * The {@link PubsubMessageToFailsafeElementFn} wraps an incoming {@link PubsubMessage} with the
     * {@link FailsafeElement} class so errors can be recovered from and the original message can be
     * output to a error records table.
     */
    static class PubsubMessageToFailsafeElementFn
            extends DoFn<PubsubMessage, FailsafeElement<PubsubMessage, String>> {
        @ProcessElement
        public void processElement(ProcessContext context) {
            PubsubMessage message = context.element();
            context.output(FailsafeElement.of(message,
                    new String(message.getPayload(), StandardCharsets.UTF_8)));
        }
    }

    /**
     * Retrieves the {@link TableDestination} for the {@link PubsubMessage} by extracting and
     * formatting the value of the {@code tableNameAttr} attribute. If the message is null, a
     * {@link RuntimeException} will be thrown because the message is unable to be routed.
     *
     * @param input The message to extract the table name from.
     * @param tableNameAttr The name of the attribute within the message which contains the table
     *        name.
     * @param outputProject The project which the table resides.
     * @param outputDataset The dataset which the table resides.
     * @return The destination to route the input message to.
     */
    @VisibleForTesting
    static TableDestination getTableDestination(ValueInSingleWindow<PubsubMessage> input,
            String tableNameAttr, String outputProject, String outputDataset) {

        PubsubMessage message = input.getValue();
        TableDestination destination;
        if (message != null) {
            String bq_data_set = null;
            try{
                bq_data_set = message.getAttributeMap().get("bq_data_set");
            }catch (ClassCastException | NullPointerException ignored){}
            if(bq_data_set != null && !bq_data_set.isEmpty()){
                outputDataset = bq_data_set;
            }
            destination = new TableDestination(String.format("%s:%s.%s", outputProject,
                    outputDataset, message.getAttributeMap().get(tableNameAttr)), null);
        } else {
            throw new RuntimeException(
                    "Cannot retrieve the dynamic table destination of an null message!");
        }
        return destination;
    }

    /**
     * Converts a JSON string to a {@link TableRow} object. If the data fails to convert, a
     * {@link RuntimeException} will be thrown.
     *
     * @param json The JSON string to parse.
     * @return The parsed {@link TableRow} object.
     */
    @VisibleForTesting
    static TableRow convertJsonToTableRow(String json) {

        TableRow row;

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            StringUtf8Coder.of().encode(json, outputStream);
            InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
            row = TableRowJsonCoder.of().decode(inputStream);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize json to table row: " + json, e);
        }

        return row;
    }
}
