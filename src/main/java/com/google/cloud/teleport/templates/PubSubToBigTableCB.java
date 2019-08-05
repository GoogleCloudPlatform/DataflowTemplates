package com.google.cloud.teleport.templates;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.util.ResourceUtils;
import com.google.cloud.teleport.util.ValueProviderUtils;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import com.google.cloud.teleport.templates.common.ErrorConverters;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.values.FailsafeElement;
import com.google.cloud.teleport.coders.FailsafeElementCoder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.apache.hadoop.hbase.client.Mutation;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;

public class PubSubToBigTableCB {
    /** The log to output status messages to. */
    private static final Logger LOG = LoggerFactory.getLogger(PubSubToBigTableCB.class);

    /** Bigtable  Column family */
    private static final byte[] FAMILY = Bytes.toBytes("cf");

    /** The tag for the main output for the UDF. */
    public static final TupleTag<FailsafeElement<PubsubMessage, String>> UDF_OUT =
            new TupleTag<FailsafeElement<PubsubMessage, String>>() {};

    /** The tag for the main output of the json transformation. */
    public static final TupleTag<Mutation> TRANSFORM_OUT = new TupleTag<Mutation>() {};

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

        @Description("Table spec to write the output to")
        ValueProvider<String> getDebugOutputTableSpec();

        void setDebugOutputTableSpec(ValueProvider<String> value);

        @Description("The project that has the BigTable for pipeline output.")
        ValueProvider<String> getBigtableProjectId();

        void setBigtableProjectId(ValueProvider<String> projectId);

        @Description("The Bigtable instance id that containers the table for pipeline ouput.")
        ValueProvider<String> getBigtableInstanceId();

        void setBigtableInstanceId(ValueProvider<String> instanceId);

        @Description("The Bigtable table id for pipeline output.")
        ValueProvider<String> getBigtableTableId();

        void setBigtableTableId(ValueProvider<String> tableId);

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
     * PubSubToBigTableCB#run(Options)} method to start the pipeline and invoke {@code
     * result.waitUntilFinish()} on the {@link PipelineResult}.
     *
     * @param args The command-line args passed by the executor.
     */
    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

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
    public static PipelineResult run(Options options) {

        Pipeline pipeline = Pipeline.create(options);

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForType(CODER.getEncodedTypeDescriptor(), CODER);

        // [END bigtable_dataflow_connector_create_pipeline]

        // [START bigtable_dataflow_connector_config]
        CloudBigtableTableConfiguration bigTableConfig =
                new CloudBigtableTableConfiguration.Builder()
                        .withProjectId(options.getBigtableProjectId().get())
                        .withInstanceId(options.getBigtableInstanceId().get())
                        .withTableId(options.getBigtableTableId().get())
                        .build();

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

        PCollectionTuple convertedBigTableRows =
                messages
                        /*
                         * Step #2: Transform the PubsubMessages into TableRows
                         */
                        .apply("ConvertMessageToBigTableRow", new PubsubMessageToBigTableRow(options));

        /*
         * Step #3: Write successful records to BigTable
         */
        convertedBigTableRows.get(TRANSFORM_OUT).apply(CloudBigtableIO.writeToTable(bigTableConfig));

        /*
         * Step #4: Write records that failed table row transformation
         * or conversion out to BigQuery deadletter table.
         */
        PCollectionList.of(
                ImmutableList.of(
                        convertedBigTableRows.get(UDF_DEADLETTER_OUT),
                        convertedBigTableRows.get(TRANSFORM_DEADLETTER_OUT)))
                .apply("Flatten", Flatten.pCollections())
                .apply(
                        "WriteFailedRecords",
                        ErrorConverters.WritePubsubMessageErrors.newBuilder()
                                .setErrorRecordsTable(
                                        ValueProviderUtils.maybeUseDefaultDeadletterTable(
                                                options.getOutputDeadletterTable(),
                                                options.getDebugOutputTableSpec(),
                                                DEFAULT_DEADLETTER_TABLE_SUFFIX))
                                .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson())
                                .build());

        return pipeline.run();
    }

    static class PubsubMessageToBigTableRow
            extends PTransform<PCollection<PubsubMessage>, PCollectionTuple> {

        private final Options options;

        PubsubMessageToBigTableRow(Options options) {
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
                                    "JsonToBigTableRow",
                                    ParDo.of(new FailsafeElementToBigtableRowFn())
                                            .withOutputTags(TRANSFORM_OUT, TupleTagList.of(TRANSFORM_DEADLETTER_OUT)));

            // Re-wrap the PCollections so we can return a single PCollectionTuple
            return PCollectionTuple.of(UDF_OUT, udfOut.get(UDF_OUT))
                    .and(UDF_DEADLETTER_OUT, udfOut.get(UDF_DEADLETTER_OUT))
                    .and(TRANSFORM_OUT, jsonToTableRowOut.get(TRANSFORM_OUT))
                    .and(TRANSFORM_DEADLETTER_OUT, jsonToTableRowOut.get(TRANSFORM_DEADLETTER_OUT));
        }
    }


    static class FailsafeElementToBigtableRowFn extends DoFn<FailsafeElement<PubsubMessage, String>, Mutation> {
        private static final long serialVersionUID = 1L;

        @ProcessElement
        public void processElement(ProcessContext context) {
            FailsafeElement<PubsubMessage, String> message = context.element();
            try {
                context.output(TRANSFORM_OUT, jsonToBigtablePut(message.getPayload()));
            } catch (Exception e) {
                context.output(
                    TRANSFORM_DEADLETTER_OUT,
                    FailsafeElement.of(message)
                            .setErrorMessage(e.getMessage())
                            .setStacktrace(Throwables.getStackTraceAsString(e)));
            }
        }

        private static Put jsonToBigtablePut(String json) throws Exception {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> obj = mapper.readValue(json, new TypeReference<Map<String, Object>>() {});
            Put p = new Put(obj.get("impression_id").toString().getBytes());
            for (Map.Entry<String, Object> key : obj.entrySet()) {
                p.addColumn(FAMILY, key.getKey().getBytes(), key.getValue().toString().getBytes());
            }
            return p;
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
            context.output(
                    FailsafeElement.of(message, new String(message.getPayload(), StandardCharsets.UTF_8)));
        }
    }
}
