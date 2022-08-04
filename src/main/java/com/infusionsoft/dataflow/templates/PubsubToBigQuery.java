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

import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.templates.common.BigQueryConverters.FailsafeJsonToTableRow;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.values.FailsafeElement;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.StreamSupport;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
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
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubsubToBigQuery {

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
  /** The log to output status messages to. */
  private static final Logger LOG =
      LoggerFactory.getLogger(com.google.cloud.teleport.templates.PubSubToBigQuery.class);

  private static Logger logger = LoggerFactory.getLogger("PubsubToBigQuery");

  /**
   * The main entry-point for pipeline execution. This method will start the pipeline but will not
   * wait for it's execution to finish. If blocking execution is required, use the {@link
   * com.google.cloud.teleport.templates.PubSubToBigQuery#run(com.google.cloud.teleport.templates.PubSubToBigQuery.Options)}
   * method to start the pipeline and invoke {@code result.waitUntilFinish()} on the {@link
   * PipelineResult}.
   *
   * @param args The command-line args passed by the executor.
   */
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    options.setMaxNumWorkers(9);
    options.setWorkerMachineType("n1-standard-1");
    options.setNumWorkers(1);
    runWithBatchJson(options);
  }

  private static <T> Iterable<T> concat(Iterable<? extends Iterable<T>> foo) {
    return () ->
        StreamSupport.stream(foo.spliterator(), false)
            .flatMap(i -> StreamSupport.stream(i.spliterator(), false))
            .iterator();
  }

  public static PipelineResult runWithBatchJson(Options options) {

    Pipeline pipeline = Pipeline.create(options);
    PCollection<String> messages =
        pipeline.apply(
            "ReadPubSubTopic", PubsubIO.readStrings().fromTopic(options.getInputTopic()));
    PCollection<Iterable<TableRow>> tableRows =
        messages.apply(batchJsonToListJson()).apply(jsonToTableRow());
    PCollection<TableRow> tableRowPCollection =
        tableRows.apply("Flatten the TableRow", Flatten.iterables());

    PCollectionTuple pcs = PCollectionTuple.of(TRANSFORM_OUT, tableRowPCollection);

    WriteResult writeResult = writeSuccessfulRecrods(pcs);

    return pipeline.run();
  }

  private static WriteResult writeSuccessfulRecrods(PCollectionTuple pCollectionTuple) {
    return pCollectionTuple
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
                .to(
                    (SerializableFunction<ValueInSingleWindow<TableRow>, TableDestination>)
                        element -> {
                          TableRow row = element.getValue();
                          String event = row.get("event").toString();
                          String projectAndDataset = "is-events-dataflow-intg:crm_prod";
                          String table_name = null;
                          if (event.equals("user_login")) {
                            table_name = "user_login";
                          } else if (event.equals("user_logout")) {
                            table_name = "user_logout";
                          } else if (event.equals("api_call_made")) {
                            table_name = "api_call_made";
                          }
                          String destination =
                              String.format("%s.%s", projectAndDataset, table_name);
                          return new TableDestination(destination, null);
                        }));
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
  public static PipelineResult runWithSingleJson(Options options) {

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

    WriteResult writeResult = writeSuccessfulRecrods(convertedTableRows);

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

  /* From package-private in TextToBigQueryStreaming.java */
  private static final JsonFactory JSON_FACTORY = Transport.getJsonFactory();

  static FailsafeElement<String, String> wrapBigQueryInsertError(BigQueryInsertError insertError) {

    FailsafeElement<String, String> failsafeElement;
    try {

      String rowPayload = JSON_FACTORY.toString(insertError.getRow());
      String errorMessage = JSON_FACTORY.toString(insertError.getError());

      failsafeElement = FailsafeElement.of(rowPayload, rowPayload);
      failsafeElement.setErrorMessage(errorMessage);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return failsafeElement;
  }

  public static PTransform<PCollection<Iterable<String>>, PCollection<Iterable<TableRow>>>
      jsonToTableRow() {
    return new JsonToTableRow();
  }

  public static PTransform<PCollection<String>, PCollection<Iterable<String>>>
      batchJsonToListJson() {
    return new BatchJsonToListJson();
  }

  /**
   * The {@link com.google.cloud.teleport.templates.PubSubToBigQuery.Options} class provides the
   * custom execution options passed by the executor at the command-line.
   */
  public interface Options
      extends PipelineOptions, JavascriptTextTransformerOptions, DataflowPipelineOptions {
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

    void setMaxNumWorkers(int value);

    void setWorkerMachineType(String value);

    void setNumWorkers(int value);
  }

  static class PubsubMessageToTableRow
      extends PTransform<PCollection<PubsubMessage>, PCollectionTuple> {

    private final Options options;

    PubsubMessageToTableRow(Options options) {
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

  private static class JsonToTableRow
      extends PTransform<PCollection<Iterable<String>>, PCollection<Iterable<TableRow>>> {

    @Override
    public PCollection<Iterable<TableRow>> expand(PCollection<Iterable<String>> stringPCollection) {

      return stringPCollection.apply(
          "JsonListToTableRow",
          MapElements
              .<Iterable<String>, Iterable<com.google.api.services.bigquery.model.TableRow>>via(
                  new SimpleFunction<Iterable<String>, Iterable<TableRow>>() {
                    @Override
                    public Iterable<TableRow> apply(Iterable<String> json) {
                      List<TableRow> tableRowList = new ArrayList<>();
                      try {
                        for (String splitJson : json) {
                          InputStream inputStream =
                              new ByteArrayInputStream(
                                  splitJson.getBytes(StandardCharsets.UTF_8.name()));
                          tableRowList.add(
                              TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER));
                        }

                      } catch (IOException e) {
                        logger.error("Unable to parse input ", e);
                      } catch (Exception e) {
                        logger.error("Error in the JsonToTableRow step", e);
                      }

                      return tableRowList;
                    }
                  }));
    }
  }

  private static class BatchJsonToListJson
      extends PTransform<PCollection<String>, PCollection<Iterable<String>>> {

    @Override
    public PCollection<Iterable<String>> expand(PCollection<String> input) {
      return input.apply(
          "BatchJsonToJsonList",
          MapElements.<String, Iterable<String>>via(
              new SimpleFunction<String, Iterable<String>>() {
                @Override
                public Iterable<String> apply(String input) {
                  String[] splitInput = input.split("&");
                  Set<String> inputSet = new HashSet<>();

                  for (String string : splitInput) {
                    try {
                      inputSet.add(string);
                    } catch (Exception e) {
                      logger.error("Unable to parse input in BatchJsonToListJson", e);
                    }
                  }
                  return inputSet;
                }
              }));
    }
  }
}
