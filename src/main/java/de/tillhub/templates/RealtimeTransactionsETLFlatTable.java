package de.tillhub.templates;

import static com.google.cloud.teleport.templates.TextToBigQueryStreaming.wrapBigQueryInsertError;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.templates.common.BigQueryConverters.FailsafeJsonToTableRow;
import com.google.cloud.teleport.templates.common.ErrorConverters;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.util.DualInputNestedValueProvider;
import com.google.cloud.teleport.util.DualInputNestedValueProvider.TranslatorInput;
import com.google.cloud.teleport.util.ResourceUtils;
import com.google.cloud.teleport.util.ValueProviderUtils;
import com.google.cloud.teleport.values.FailsafeElement;
import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.TimeZone;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link de.tillhub.templates.RealtimeTransactionsETLFlatTable} pipeline is a streaming pipeline which ingests data in JSON format
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
 * -Dexec.mainClass=de.tillhub.templates.RealtimeTransactionsETLFlatTable \
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
public class RealtimeTransactionsETLFlatTable {

    /** The log to output status messages to. */
    private static final Logger LOG = LoggerFactory.getLogger(de.tillhub.templates.RealtimeTransactionsETLFlatTable.class);

    /** The tag for the main output for the UDF. */
    public static  TupleTag<FailsafeElement<PubsubMessage, String>> UDF_OUT;

    /** The tag for the main output of the json transformation. */
    public static  TupleTag<TableRow> TRANSFORM_OUT;

    /** The tag for the dead-letter output of the udf. */
    public static  TupleTag<FailsafeElement<PubsubMessage, String>> UDF_DEADLETTER_OUT;

    /** The tag for the dead-letter output of the json to table row transform. */
    public static  TupleTag<FailsafeElement<PubsubMessage, String>> TRANSFORM_DEADLETTER_OUT;

    /** The default suffix for error tables if dead letter table is not specified. */
    public static final String DEFAULT_DEADLETTER_TABLE_SUFFIX = "_error_records";

    /** Pubsub message/string coder for pipeline. */
    public static final FailsafeElementCoder<PubsubMessage, String> CODER =
            FailsafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), StringUtf8Coder.of());

    /** String/String Coder for FailsafeElement. */
    public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
            FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    public static void initResultHolders() {
        UDF_OUT = new TupleTag<FailsafeElement<PubsubMessage, String>>() {};
        TRANSFORM_OUT = new TupleTag<TableRow>() {};
        UDF_DEADLETTER_OUT = new TupleTag<FailsafeElement<PubsubMessage, String>>() {};
        TRANSFORM_DEADLETTER_OUT = new TupleTag<FailsafeElement<PubsubMessage, String>>() {};
    }

    /**
     * The {@link de.tillhub.templates.RealtimeTransactionsETLFlatTable.Options} class provides the custom execution options passed by the executor at the
     * command-line.
     */
    public interface Options extends PipelineOptions, JavascriptTextTransformerOptions {
        @Description("Table spec to write the output to")
        ValueProvider<String> getOutputTableSpec();

        void setOutputTableSpec(ValueProvider<String> value);

        @Description("The root location in GCS to write messages to. Should include data and temp subfolders")
        ValueProvider<String> getOutputGCSSpec();

        void setOutputGCSSpec(ValueProvider<String> value);

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
     * de.tillhub.templates.RealtimeTransactionsETLFlatTable#run(de.tillhub.templates.RealtimeTransactionsETLFlatTable.Options)} method to start the pipeline and invoke {@code
     * result.waitUntilFinish()} on the {@link PipelineResult}.
     *
     * @param args The command-line args passed by the executor.
     */
    public static void main(String[] args) {
        de.tillhub.templates.RealtimeTransactionsETLFlatTable.Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(de.tillhub.templates.RealtimeTransactionsETLFlatTable.Options.class);

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
    public static PipelineResult run(de.tillhub.templates.RealtimeTransactionsETLFlatTable.Options options) {

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
         *  4) Write failed records out to BigQuery
         */

        /*
         * Step #1: Read messages in from Pub/Sub
         * Either from a Subscription or Topic
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

        etlToGcs(messages, options);
        etlWithChildElements(messages, options, "transformFlatTableTransaction");

        return pipeline.run();
    }

    /***
     * This function writes the messages to GCS (adding the client_account and transaction to the message body
     * The folders structure is maintained using the {@link FileNaming} class
     * @param messages
     * @param options
     */
    private static void etlToGcs(PCollection<PubsubMessage> messages, Options options) {
        String basGcsLocation = options.getOutputGCSSpec().get();
        messages
                .apply("MapToEvent", ParDo.of(new PubsubMessageToTransactionEvent()))
                .apply(Window.<TransactionEvent>into(FixedWindows.of(Duration.standardSeconds(30L))))
                .apply("writeToGCS",
                        FileIO.<String, TransactionEvent>writeDynamic()
                                .by((SerializableFunction<TransactionEvent, String>) input -> input.getClientAccount() + "###" + input.getCreatedAt() + "###" + input.getTransaction())
                                .via(
                                        Contextful.fn(
                                                (SerializableFunction<TransactionEvent, String>) input -> input.getPayload()),
                                        TextIO.sink())
                                .to(basGcsLocation + "/data")
                                .withNaming(type -> FileNaming.getNaming(type, ".json"))
                                .withDestinationCoder(StringUtf8Coder.of())
                                .withTempDirectory(
                                        String.format(basGcsLocation + "/temp"))
                                .withNumShards(1));




    }

    /**
     * Write to BigQuery a top level transaction (transformed from an object)
     * @param messages
     * @param options
     */
    private static void etlTopLevelObject(PCollection<PubsubMessage> messages, de.tillhub.templates.RealtimeTransactionsETLFlatTable.Options options) {
        initResultHolders();
        PCollectionTuple convertedTableRows =
                messages
                        /*
                         * Step #2: Transform the PubsubMessages into TableRows
                         */
                        .apply("ConvertMessageToTableRow", new de.tillhub.templates.RealtimeTransactionsETLFlatTable.PubsubMessageToTableRow(options));

        /*
         * Step #3: Write the successful records out to BigQuery
         */
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
                                        .to(options.getOutputTableSpec()));

        /*
         * Step 3 Contd.
         * Elements that failed inserts into BigQuery are extracted and converted to FailsafeElement
         */
        PCollection<FailsafeElement<String, String>> failedInserts =
                writeResult
                        .getFailedInsertsWithErr()
                        .apply(
                                "WrapInsertionErrors",
                                MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                                        .via((BigQueryInsertError e) -> wrapBigQueryInsertError(e)))
                        .setCoder(FAILSAFE_ELEMENT_CODER);

        /*
         * Step #4: Write records that failed table row transformation
         * or conversion out to BigQuery deadletter table.
         */
        PCollectionList.of(
                ImmutableList.of(
                        convertedTableRows.get(UDF_DEADLETTER_OUT),
                        convertedTableRows.get(TRANSFORM_DEADLETTER_OUT)))
                .apply("Flatten", Flatten.pCollections())
                .apply(
                        "WriteFailedRecords",
                        ErrorConverters.WritePubsubMessageErrors.newBuilder()
                                .setErrorRecordsTable(
                                        ValueProviderUtils.maybeUseDefaultDeadletterTable(
                                                options.getOutputDeadletterTable(),
                                                options.getOutputTableSpec(),
                                                DEFAULT_DEADLETTER_TABLE_SUFFIX))
                                .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson())
                                .build());

        // 5) Insert records that failed insert into deadletter table
        failedInserts.apply(
                "WriteFailedRecords",
                ErrorConverters.WriteStringMessageErrors.newBuilder()
                        .setErrorRecordsTable(
                                ValueProviderUtils.maybeUseDefaultDeadletterTable(
                                        options.getOutputDeadletterTable(),
                                        options.getOutputTableSpec(),
                                        DEFAULT_DEADLETTER_TABLE_SUFFIX))
                        .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson())
                        .build());

    }

    /**
     * Write to BigQuery the child elements of a transaction (transformed as an array of objects)
     * @param messages
     * @param options
     */
    private static void etlWithChildElements(PCollection<PubsubMessage> messages, de.tillhub.templates.RealtimeTransactionsETLFlatTable.Options options, String udfFunc) {
        initResultHolders();

        PCollectionTuple convertedTableRows =
                messages
                        /*
                         * Step #2: Transform the PubsubMessages into TableRows
                         */
                        .apply("ConvertMessageToTableRow", new de.tillhub.templates.RealtimeTransactionsETLFlatTable.PubsubMessageArrayToTableRow(options, udfFunc));

        /*
         * Step #3: Write the successful records out to BigQuery
         */
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
                                        .to(options.getOutputTableSpec()));

        /*
         * Step 3 Contd.
         * Elements that failed inserts into BigQuery are extracted and converted to FailsafeElement
         */
        PCollection<FailsafeElement<String, String>> failedInserts =
                writeResult
                        .getFailedInsertsWithErr()
                        .apply(
                                "WrapInsertionErrors",
                                MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                                        .via((BigQueryInsertError e) -> wrapBigQueryInsertError(e)))
                        .setCoder(FAILSAFE_ELEMENT_CODER);

        /*
         * Step #4: Write records that failed table row transformation
         * or conversion out to BigQuery deadletter table.
         */
        PCollectionList.of(
                ImmutableList.of(
                        convertedTableRows.get(UDF_DEADLETTER_OUT),
                        convertedTableRows.get(TRANSFORM_DEADLETTER_OUT)))
                .apply("Flatten", Flatten.pCollections())
                .apply(
                        "WriteFailedRecords",
                        ErrorConverters.WritePubsubMessageErrors.newBuilder()
                                .setErrorRecordsTable(
                                        ValueProviderUtils.maybeUseDefaultDeadletterTable(
                                                options.getOutputDeadletterTable(),
                                                options.getOutputTableSpec(),
                                                DEFAULT_DEADLETTER_TABLE_SUFFIX))
                                .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson())
                                .build());

        // 5) Insert records that failed insert into deadletter table
        failedInserts.apply(
                "WriteFailedRecords",
                ErrorConverters.WriteStringMessageErrors.newBuilder()
                        .setErrorRecordsTable(
                                ValueProviderUtils.maybeUseDefaultDeadletterTable(
                                        options.getOutputDeadletterTable(),
                                        options.getOutputTableSpec(),
                                        DEFAULT_DEADLETTER_TABLE_SUFFIX))
                        .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson())
                        .build());

    }

    /**
     * If deadletterTable is available, it is returned as is, otherwise outputTableSpec +
     * defaultDeadLetterTableSuffix is returned instead.
     */
    private static ValueProvider<String> maybeUseDefaultDeadletterTable(
            ValueProvider<String> deadletterTable,
            ValueProvider<String> outputTableSpec,
            String defaultDeadLetterTableSuffix) {
        return DualInputNestedValueProvider.of(
                deadletterTable,
                outputTableSpec,
                new SerializableFunction<TranslatorInput<String, String>, String>() {
                    @Override
                    public String apply(TranslatorInput<String, String> input) {
                        String userProvidedTable = input.getX();
                        String outputTableSpec = input.getY();
                        if (userProvidedTable == null) {
                            return outputTableSpec + defaultDeadLetterTableSuffix;
                        }
                        return userProvidedTable;
                    }
                });
    }

    /**
     * The {@link de.tillhub.templates.RealtimeTransactionsETLFlatTable.PubsubMessageToTableRow} class is a {@link PTransform} which transforms incoming
     * {@link PubsubMessage} objects into {@link TableRow} objects for insertion into BigQuery while
     * applying an optional UDF to the input. The executions of the UDF and transformation to {@link
     * TableRow} objects is done in a fail-safe way by wrapping the element with it's original payload
     * inside the {@link FailsafeElement} class. The {@link de.tillhub.templates.RealtimeTransactionsETLFlatTable.PubsubMessageToTableRow} transform will
     * output a {@link PCollectionTuple} which contains all output and dead-letter {@link
     * PCollection}.
     *
     * <p>The {@link PCollectionTuple} output will contain the following {@link PCollection}:
     *
     * <ul>
     *   <li>{@link de.tillhub.templates.RealtimeTransactionsETLFlatTable#UDF_OUT} - Contains all {@link FailsafeElement} records
     *       successfully processed by the optional UDF.
     *   <li>{@link de.tillhub.templates.RealtimeTransactionsETLFlatTable#UDF_DEADLETTER_OUT} - Contains all {@link FailsafeElement}
     *       records which failed processing during the UDF execution.
     *   <li>{@link de.tillhub.templates.RealtimeTransactionsETLFlatTable#TRANSFORM_OUT} - Contains all records successfully converted from
     *       JSON to {@link TableRow} objects.
     *   <li>{@link de.tillhub.templates.RealtimeTransactionsETLFlatTable#TRANSFORM_DEADLETTER_OUT} - Contains all {@link FailsafeElement}
     *       records which couldn't be converted to table rows.
     * </ul>
     */
    static class PubsubMessageToTableRow
            extends PTransform<PCollection<PubsubMessage>, PCollectionTuple> {

        private final de.tillhub.templates.RealtimeTransactionsETLFlatTable.Options options;

        PubsubMessageToTableRow(de.tillhub.templates.RealtimeTransactionsETLFlatTable.Options options) {
            this.options = options;
        }

        @Override
        public PCollectionTuple expand(PCollection<PubsubMessage> input) {

            PCollectionTuple udfOut =
                    input
                            // Map the incoming messages into FailsafeElements so we can recover from failures
                            // across multiple transforms.
                            .apply("MapToRecord", ParDo.of(new de.tillhub.templates.RealtimeTransactionsETLFlatTable.PubsubMessageToFailsafeElementFn()))
                            .apply(
                                    "InvokeUDF",
                                    FailsafeJavascriptUdf.<PubsubMessage>newBuilder()
                                            .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                                            .setFunctionName(ValueProvider.StaticValueProvider.of("transformTransactionTopLevel"))
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

    /**
     * The {@link de.tillhub.templates.RealtimeTransactionsETLFlatTable.PubsubMessageArrayToTableRow} class is a {@link PTransform} which transforms incoming
     * {@link PubsubMessage} objects into {@link TableRow} objects for insertion into BigQuery while
     * applying an optional UDF to the input. The executions of the UDF and transformation to {@link
     * TableRow} objects is done in a fail-safe way by wrapping the element with it's original payload
     * inside the {@link FailsafeElement} class. The {@link de.tillhub.templates.RealtimeTransactionsETLFlatTable.PubsubMessageArrayToTableRow} transform will
     * output a {@link PCollectionTuple} which contains all output and dead-letter {@link
     * PCollection}.
     *
     * <p>The {@link PCollectionTuple} output will contain the following {@link PCollection}:
     *
     * <ul>
     *   <li>{@link de.tillhub.templates.RealtimeTransactionsETLFlatTable#UDF_OUT} - Contains all {@link FailsafeElement} records
     *       successfully processed by the optional UDF.
     *   <li>{@link de.tillhub.templates.RealtimeTransactionsETLFlatTable#UDF_DEADLETTER_OUT} - Contains all {@link FailsafeElement}
     *       records which failed processing during the UDF execution.
     *   <li>{@link de.tillhub.templates.RealtimeTransactionsETLFlatTable#TRANSFORM_OUT} - Contains all records successfully converted from
     *       JSON to {@link TableRow} objects.
     *   <li>{@link de.tillhub.templates.RealtimeTransactionsETLFlatTable#TRANSFORM_DEADLETTER_OUT} - Contains all {@link FailsafeElement}
     *       records which couldn't be converted to table rows.
     * </ul>
     */
    static public class PubsubMessageArrayToTableRow
            extends PTransform<PCollection<PubsubMessage>, PCollectionTuple> {

        private final de.tillhub.templates.RealtimeTransactionsETLFlatTable.Options options;
        private final String udfFunc;

        public PubsubMessageArrayToTableRow(de.tillhub.templates.RealtimeTransactionsETLFlatTable.Options options, String udfFunc) {
            this.options = options;
            this.udfFunc = udfFunc;
        }

        @Override
        public PCollectionTuple expand(PCollection<PubsubMessage> input) {

            PCollectionTuple udfOut =
                    input
                            // Map the incoming messages into FailsafeElements so we can recover from failures
                            // across multiple transforms.
                            .apply("MapToRecord", ParDo.of(new de.tillhub.templates.RealtimeTransactionsETLFlatTable.PubsubMessageToFailsafeElementFn()))
                            .apply(
                                    "InvokeUDF",
                                    FailsafeJavascriptUdf.<PubsubMessage>newBuilder()
                                            .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                                            .setFunctionName(ValueProvider.StaticValueProvider.of(this.udfFunc))
                                            .setSuccessTag(UDF_OUT)
                                            .setFailureTag(UDF_DEADLETTER_OUT)
                                            .build());

            // Convert the records which were successfully processed by the UDF into TableRow objects.
            PCollectionTuple jsonToTableRowOut =
                    udfOut
                            .get(UDF_OUT)
                            .apply(
                                    "JsonToTableRow",
                                    de.tillhub.converters.TillhubConverters.FailsafeJsonToTableRow.<PubsubMessage>newBuilder()
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


    /**
     * The {@link de.tillhub.templates.RealtimeTransactionsETLFlatTable.PubsubMessageToFailsafeElementFn} wraps an incoming {@link PubsubMessage} with the
     * {@link FailsafeElement} class so errors can be recovered from and the original message can be
     * output to a error records table.
     */
    static class PubsubMessageToFailsafeElementFn
            extends DoFn<PubsubMessage, FailsafeElement<PubsubMessage, String>> {
        @ProcessElement
        public void processElement(ProcessContext context) {
            PubsubMessage message = context.element();
            context.output(
                    FailsafeElement.of(message, new String(message.getPayload(), StandardCharsets.UTF_8) + "~#~#~"
                            + message.getAttribute("client_account") + "~#~#~" + message.getAttribute("transaction")));
        }
    }

    /***
     * class {@link FileNaming} implements a dynamic destinations strategy for the GCS files, based on the client account
     */
    static class FileNaming implements FileIO.Write.FileNaming {
        static FileNaming getNaming(String clientAccountAndCreationDate, String suffix) {
            return new FileNaming(clientAccountAndCreationDate, suffix);
        }

        private static final DateTimeFormatter FORMATTER = DateTimeFormat
                .forPattern("yyyy-MM-dd").withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("Europe/Berlin")));

        private final String clientAccountAndCreationDate;
        private final String suffix;

        private String filenamePrefixForWindow(IntervalWindow window) {
            String[] split = clientAccountAndCreationDate.split("###");
            return String.format(
                    "%s/%s/%s", split[0], split[1], split[2]);
        }

        private FileNaming(String clientAccountAndCreationDate, String suffix) {
            this.clientAccountAndCreationDate = clientAccountAndCreationDate;
            this.suffix = suffix;
        }

        @Override
        public String getFilename(
                BoundedWindow window,
                PaneInfo pane,
                int numShards,
                int shardIndex,
                Compression compression) {

            IntervalWindow intervalWindow = (IntervalWindow) window;
            String filenamePrefix = filenamePrefixForWindow(intervalWindow);
//            String filename =
//                    String.format(
//                            "pane-%d-%s-%05d-of-%05d%s",
//                            pane.getIndex(),
//                            pane.getTiming().toString().toLowerCase(),
//                            shardIndex,
//                            numShards,
//                            suffix);
            String fullName = filenamePrefix; //+ filename;
            return fullName + suffix;
        }
    }

    /**
     * The {@link de.tillhub.templates.RealtimeTransactionsETLFlatTable.PubsubMessageToTransactionEvent} transforms an incoming {@link PubsubMessage} to a
     * {@link TransactionEvent} class not before enriching it with the client_account and transaction ID
     */
    static class PubsubMessageToTransactionEvent
            extends DoFn<PubsubMessage, TransactionEvent> {
        @ProcessElement
        public void processElement(ProcessContext context) {
            PubsubMessage message = context.element();
            String jsonAsStr = new String(message.getPayload(), StandardCharsets.UTF_8);
            JSONObject obj = new JSONObject(jsonAsStr);
            String clientAccount = message.getAttribute("client_account");
            String transaction = message.getAttribute("transaction");
            String createdAt = obj.getString("date").split("T")[0];
            obj.put("client_account", clientAccount);
            obj.put("oltp_entity_id", transaction);

            TransactionEvent te = new TransactionEvent(obj.toString(), clientAccount, createdAt, transaction);
            context.output(te);
        }
    }

    /**
     * The {@link TransactionEvent} is a pojo holding the tx event
     */
    static class TransactionEvent implements Serializable {
        private String payload;
        private String clientAccount;
        private String createdAt;
        private String transaction;

        public TransactionEvent(String payload, String clientAccount, String createdAt, String transaction) {
            this.payload = payload;
            this.clientAccount = clientAccount;
            this.createdAt = createdAt;
            this.transaction = transaction;
        }
        public String getPayload() {
            return payload;
        }

        public String getClientAccount() {
            return clientAccount;
        }

        public String getCreatedAt() {
            return createdAt;
        }

        public void setClientAccount(String clientAccount) {
            this.clientAccount = clientAccount;
        }

        public void setPayload(String payload) {
            this.payload = payload;
        }

        public void setCreatedAt(String createdAt) {
            this.createdAt = createdAt;
        }

        public void setTransaction(String transaction) {
            this.transaction = transaction;
        }

        public String getTransaction() {
            return transaction;
        }
    }
}
