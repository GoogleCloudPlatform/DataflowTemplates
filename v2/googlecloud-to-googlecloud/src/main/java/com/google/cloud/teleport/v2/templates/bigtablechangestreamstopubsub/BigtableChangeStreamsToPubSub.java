/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub;

import com.google.cloud.Timestamp;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.SchemaServiceClient;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.bigtable.utils.UnsupportedEntryException;
import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueManager;
import com.google.cloud.teleport.v2.cdc.dlq.StringDeadLetterQueueSanitizer;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.options.BigtableChangeStreamsToPubSubOptions;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.BigtableSource;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.Mod;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.ModType;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.PubSubDestination;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.schemautils.PubSubUtils;
import com.google.cloud.teleport.v2.transforms.DLQWriteTransform;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This pipeline ingests {@link ChangeStreamMutation} from Bigtable change stream. The {@link
 * ChangeStreamMutation} is then broken into {@link Mod}, which converted into PubsubMessage and
 * inserted into Pub/Sub topic.
 */
@Template(
    name = "Bigtable_Change_Streams_to_PubSub",
    category = TemplateCategory.STREAMING,
    displayName = "Cloud Bigtable change streams to PubSub",
    description =
        "Streaming pipeline. Streams Bigtable data change records and writes them into PubSub using Dataflow Runner V2.",
    optionsClass = BigtableChangeStreamsToPubSubOptions.class,
    flexContainerName = "bigtable-changestreams-to-pubsub",
    contactInformation = "https://cloud.google.com/support")
public final class BigtableChangeStreamsToPubSub {

  /** String/String Coder for {@link FailsafeElement}. */
  public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

  private static final Logger LOG = LoggerFactory.getLogger(BigtableChangeStreamsToPubSub.class);

  private static final String USE_RUNNER_V2_EXPERIMENT = "use_runner_v2";

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    LOG.info("Starting to replicate change records from Cloud Bigtable change streams to PubSub");

    BigtableChangeStreamsToPubSubOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(BigtableChangeStreamsToPubSubOptions.class);

    run(options);
  }

  private static void validateOptions(BigtableChangeStreamsToPubSubOptions options) {
    if (options.getDlqRetryMinutes() <= 0) {
      throw new IllegalArgumentException("dlqRetryMinutes must be positive.");
    }
    if (options.getDlqMaxRetries() < 0) {
      throw new IllegalArgumentException("dlqMaxRetries cannot be negative.");
    }

    List <String> acceptableMessageFormats = Arrays.asList("AVRO", "PROTOCOL_BUFFER", "JSON");
    if (!acceptableMessageFormats.contains(options.getMessageFormat())) {
      throw new IllegalArgumentException("Allowed formats are AVRO, PROTOCOL_BUFFER and JSON Text. Default value is JSON.");
    }

    List <String> acceptableMessageEncodings = Arrays.asList("BINARY", "JSON");
    if (!acceptableMessageEncodings.contains(options.getMessageFormat())) {
      throw new IllegalArgumentException("Allowed formats are BINARY and JSON Text. Default value is JSON.");
    }
  }

  private static void setOptions(BigtableChangeStreamsToPubSubOptions options) {
    options.setStreaming(true);
    options.setEnableStreamingEngine(true);

    // Add use_runner_v2 to the experiments option, since change streams connector is only supported
    // on Dataflow runner v2.
    List<String> experiments = options.getExperiments();
    if (experiments == null) {
      experiments = new ArrayList<>();
    }
    boolean hasUseRunnerV2 = false;
    for (String experiment : experiments) {
      if (experiment.toLowerCase().equals(USE_RUNNER_V2_EXPERIMENT)) {
        hasUseRunnerV2 = true;
        break;
      }
    }
    if (!hasUseRunnerV2) {
      experiments.add(USE_RUNNER_V2_EXPERIMENT);
    }
    options.setExperiments(experiments);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(BigtableChangeStreamsToPubSubOptions options) {
    LOG.info("Requested message format is " + options.getMessageFormat());
    setOptions(options);
    validateOptions(options);

    String bigtableProject = getBigtableProjectId(options);

    // Retrieve and parse the startTimestamp
    Instant startTimestamp =
        options.getBigtableChangeStreamStartTimestamp().isEmpty()
            ? Instant.now()
            : toInstant(Timestamp.parseTimestamp(options.getBigtableChangeStreamStartTimestamp()));

    BigtableSource sourceInfo =
        new BigtableSource(
            options.getBigtableReadInstanceId(),
            options.getBigtableReadTableId(),
            getBigtableCharset(options),
            options.getBigtableChangeStreamIgnoreColumnFamilies(),
            options.getBigtableChangeStreamIgnoreColumns(),
            startTimestamp);

    PubSubDestination destinationInfo =
        new PubSubDestination(
            getPubSubProjectId(options),
            options.getPubSubTopic(),
            options.getMessageFormat(),
            options.getMessageEncoding(),
            options.getUseBase64Rowkey(),
            options.getUseBase64ColumnQualifier(),
            options.getUseBase64Value());

    PubSubUtils pubSub = new PubSubUtils(sourceInfo, destinationInfo, options.getPubSubAPI());

    try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
      GetTopicRequest request =
          GetTopicRequest.newBuilder()
              .setTopic(
                  TopicName.ofProjectTopicName(
                          pubSub.getDestination().getPubSubProject(),
                          pubSub.getDestination().getPubSubTopicName())
                      .toString())
              .build();
      Topic topic = topicAdminClient.getTopic(request);

      pubSub.getDestination().setPubSubTopic(topic);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    try {
      if (!validateSchema(options, pubSub)) {
        final String errorMessage =
                "Invalid message format or encoding associated with the topic.";
        throw new IllegalArgumentException(errorMessage);
      }

    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }

    /*
     * Stages: 1) Read {@link ChangeStreamMutation} from change stream. 2) Create {@link
     * FailsafeElement} of {@link Mod} JSON and merge from: - {@link ChangeStreamMutation}. - GCS Dead
     * letter queue. 3) Convert {@link Mod} JSON into PubsubMessage and publish it to PubSub.
     * 4) Write Failures from 2) and 3) to GCS dead letter queue.
     */
    /** Step 1 */
    Pipeline pipeline = Pipeline.create(options);
    DeadLetterQueueManager dlqManager = buildDlqManager(options);

    String dlqDirectory = dlqManager.getRetryDlqDirectoryWithDateTime();
    String tempDlqDirectory = dlqManager.getRetryDlqDirectory() + "tmp/";

    BigtableIO.ReadChangeStream readChangeStream =
        BigtableIO.readChangeStream()
            .withChangeStreamName(options.getBigtableChangeStreamName())
            .withProjectId(bigtableProject)
            .withMetadataTableInstanceId(options.getBigtableChangeStreamMetadataInstanceId())
            .withInstanceId(options.getBigtableReadInstanceId())
            .withTableId(options.getBigtableReadTableId())
            .withAppProfileId(options.getBigtableChangeStreamAppProfile())
            .withStartTime(startTimestamp);

    if (!StringUtils.isBlank(options.getBigtableChangeStreamMetadataTableTableId())) {
      readChangeStream =
          readChangeStream.withMetadataTableTableId(
              options.getBigtableChangeStreamMetadataTableTableId());
    }
    /** Step 2: just return the output for sending to pubSub and DLQ */
    PCollection<ChangeStreamMutation> dataChangeRecord =
        pipeline
            .apply("Read from Cloud Bigtable Change Streams", readChangeStream)
            .apply(Values.create());

    PCollection<FailsafeElement<String, String>> sourceFailsafeModJson =
        dataChangeRecord
            .apply(
                "ChangeStreamMutation To Mod JSON",
                ParDo.of(new ChangeStreamMutationToModJsonFn(sourceInfo)))
            .apply(
                "Wrap Mod JSON In FailsafeElement",
                ParDo.of(
                    new DoFn<String, FailsafeElement<String, String>>() {
                      @ProcessElement
                      public void process(
                          @Element String input,
                          OutputReceiver<FailsafeElement<String, String>> receiver) {
                        receiver.output(FailsafeElement.of(input, input));
                      }
                    }))
            .setCoder(FAILSAFE_ELEMENT_CODER);

    PCollectionTuple dlqModJson =
        dlqManager.getReconsumerDataTransform(
            pipeline.apply(dlqManager.dlqReconsumer(options.getDlqRetryMinutes())));
    PCollection<FailsafeElement<String, String>> retryableDlqFailsafeModJson =
        dlqModJson.get(DeadLetterQueueManager.RETRYABLE_ERRORS).setCoder(FAILSAFE_ELEMENT_CODER);

    PCollection<FailsafeElement<String, String>> failsafeModJson =
        PCollectionList.of(sourceFailsafeModJson)
            .and(retryableDlqFailsafeModJson)
            .apply("Merge Source And DLQ Mod JSON", Flatten.pCollections());

    FailsafeModJsonToPubsubMessageTransformer.FailsafeModJsonToPubsubMessageOptions
        failsafeModJsonToPubsubOptions =
            FailsafeModJsonToPubsubMessageTransformer.FailsafeModJsonToPubsubMessageOptions
                .builder()
                .setCoder(FAILSAFE_ELEMENT_CODER)
                .build();

    FailsafeModJsonToPubsubMessageTransformer.FailsafeModJsonToPubsubMessage
        failsafeModJsonToPubsubMessage =
            new FailsafeModJsonToPubsubMessageTransformer.FailsafeModJsonToPubsubMessage(
                pubSub, failsafeModJsonToPubsubOptions);

    PCollectionTuple pubsubMessage =
        failsafeModJson.apply("Mod JSON To pubsub", failsafeModJsonToPubsubMessage);

    PCollection<String> transformDlqJson =
        pubsubMessage
            .get(failsafeModJsonToPubsubMessage.transformDeadLetterOut)
            .apply(
                "Failed Mod JSON During Table Row Transformation",
                MapElements.via(new StringDeadLetterQueueSanitizer()));

    PCollectionList.of(transformDlqJson)
        .apply("Merge Failed Mod JSON From Transform And PubSub", Flatten.pCollections())
        .apply(
            "Write Failed Mod JSON To DLQ",
            DLQWriteTransform.WriteDLQ.newBuilder()
                .withDlqDirectory(dlqDirectory)
                .withTmpDirectory(tempDlqDirectory)
                .setIncludePaneInfo(true)
                .build());

    PCollection<FailsafeElement<String, String>> nonRetryableDlqModJsonFailsafe =
        dlqModJson.get(DeadLetterQueueManager.PERMANENT_ERRORS).setCoder(FAILSAFE_ELEMENT_CODER);
    LOG.info(
        "DLQ manager severe DLQ directory with date time: {}",
        dlqManager.getSevereDlqDirectoryWithDateTime());
    LOG.info("DLQ manager severe DLQ directory: {}", dlqManager.getSevereDlqDirectory() + "tmp/");
    nonRetryableDlqModJsonFailsafe
        .apply(
            "Write Mod JSON With Non-retryable Error To DLQ",
            MapElements.via(new StringDeadLetterQueueSanitizer()))
        .setCoder(StringUtf8Coder.of())
        .apply(
            DLQWriteTransform.WriteDLQ.newBuilder()
                .withDlqDirectory(dlqManager.getSevereDlqDirectoryWithDateTime())
                .withTmpDirectory(dlqManager.getSevereDlqDirectory() + "tmp/")
                .setIncludePaneInfo(true)
                .build());

    return pipeline.run();
  }

  private static Instant toInstant(Timestamp timestamp) {
    if (timestamp == null) {
      return null;
    } else {
      return Instant.ofEpochMilli(timestamp.getSeconds() * 1000 + timestamp.getNanos() / 1000000);
    }
  }

  private static DeadLetterQueueManager buildDlqManager(
      BigtableChangeStreamsToPubSubOptions options) {
    String tempLocation =
        options.as(DataflowPipelineOptions.class).getTempLocation().endsWith("/")
            ? options.as(DataflowPipelineOptions.class).getTempLocation()
            : options.as(DataflowPipelineOptions.class).getTempLocation() + "/";
    String dlqDirectory =
        options.getDlqDirectory().isEmpty() ? tempLocation + "dlq/" : options.getDlqDirectory();

    LOG.info("DLQ directory: {}", dlqDirectory);
    return DeadLetterQueueManager.create(dlqDirectory, options.getDlqMaxRetries());
  }

  private static String getBigtableCharset(BigtableChangeStreamsToPubSubOptions options) {
    return StringUtils.isEmpty(options.getBigtableChangeStreamCharset())
        ? "UTF-8"
        : options.getBigtableChangeStreamCharset();
  }

  private static String getBigtableProjectId(BigtableChangeStreamsToPubSubOptions options) {
    return StringUtils.isEmpty(options.getBigtableReadProjectId())
        ? options.getProject()
        : options.getBigtableReadProjectId();
  }

  private static String getPubSubProjectId(BigtableChangeStreamsToPubSubOptions options) {
    return StringUtils.isEmpty(options.getPubSubProjectId())
        ? options.getProject()
        : options.getPubSubProjectId();
  }

  private static Boolean validateSchema(BigtableChangeStreamsToPubSubOptions options, PubSubUtils pubSub)
      throws Exception {
      Topic topic = pubSub.getDestination().getPubSubTopic();
      if (options.getMessageEncoding() != topic.getSchemaSettings().getEncoding().toString()) {
        return false;
      }
      String messageFormatPath = topic.getSchemaSettings().getSchema();
      String messageFormat = pubSub.getDestination().getMessageFormat();
      if (topic.getSchemaSettings().getSchema().isEmpty()) {
        return true;
      } else {
        ByteString testChangeJsonMessage = ByteString.copyFromUtf8(readTestChangeJsonMessageInBytes());
          try (SchemaServiceClient schemaServiceClient = SchemaServiceClient.create()){
          ValidateMessageRequest request =
                  ValidateMessageRequest.newBuilder()
                          .setParent(pubSub.getDestination().getPubSubProject())
                          .setMessage(testChangeJsonMessage)
                          .setName(messageFormatPath)
                          .build();
            ValidateMessageResponse response = schemaServiceClient.validateMessage(request);


        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      return true;
  }

  private static String readTestChangeJsonMessageInBytes() {
    String changeJson = "";
    return changeJson;
  }

  /**
   * DoFn that converts a {@link ChangeStreamMutation} to multiple {@link Mod} in serialized JSON
   * format.
   */
  static class ChangeStreamMutationToModJsonFn extends DoFn<ChangeStreamMutation, String> {

    private final BigtableSource sourceInfo;

    ChangeStreamMutationToModJsonFn(BigtableSource source) {
      this.sourceInfo = source;
    }

    @ProcessElement
    public void process(@Element ChangeStreamMutation input, OutputReceiver<String> receiver)
        throws Exception {
      for (Entry entry : input.getEntries()) {
        ModType modType = getModType(entry);

        Mod mod = null;
        switch (modType) {
          case SET_CELL:
            mod = new Mod(sourceInfo, input, (SetCell) entry);
            break;
          case DELETE_CELLS:
            mod = new Mod(sourceInfo, input, (DeleteCells) entry);
            break;
          case DELETE_FAMILY:
            mod = new Mod(sourceInfo, input, (DeleteFamily) entry);
            break;
          default:
          case UNKNOWN:
            throw new UnsupportedEntryException(
                "Cloud Bigtable change stream entry of type "
                    + entry.getClass().getName()
                    + " is not supported. The entry was put into a DLQ directory. "
                    + "Please update your Dataflow template with the latest template version");
        }

        String modJsonString;

        try {
          modJsonString = mod.toJson();
        } catch (IOException e) {
          // Ignore exception and print bad format.
          modJsonString = String.format("\"%s\"", input);
        }
        receiver.output(modJsonString);
      }
    }

    private ModType getModType(Entry entry) {
      if (entry instanceof SetCell) {
        return ModType.SET_CELL;
      } else if (entry instanceof DeleteCells) {
        return ModType.DELETE_CELLS;
      } else if (entry instanceof DeleteFamily) {
        return ModType.DELETE_FAMILY;
      }
      return ModType.UNKNOWN;
    }
  }
}
