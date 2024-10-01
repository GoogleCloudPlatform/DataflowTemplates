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
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation.MutationType;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.cloud.pubsub.v1.SchemaServiceClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.bigtable.options.BigtableCommonOptions;
import com.google.cloud.teleport.v2.bigtable.utils.UnsupportedEntryException;
import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueManager;
import com.google.cloud.teleport.v2.cdc.dlq.StringDeadLetterQueueSanitizer;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.options.BigtableChangeStreamsToPubSubOptions;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.FailsafePublisher.PublishModJsonToTopic;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.BigtableSource;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.MessageEncoding;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.MessageFormat;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.Mod;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.ModType;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.PubSubDestination;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.TestChangeStreamMutation;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.schemautils.PubSubUtils;
import com.google.cloud.teleport.v2.transforms.DLQWriteTransform;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.Encoding;
import com.google.pubsub.v1.GetTopicRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Schema;
import com.google.pubsub.v1.SchemaName;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import com.google.pubsub.v1.ValidateMessageRequest;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
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
    displayName = "Cloud Bigtable Change Streams to PubSub",
    description =
        "Streaming pipeline. Streams Bigtable data change records and writes them into PubSub using Dataflow Runner V2.",
    optionsClass = BigtableChangeStreamsToPubSubOptions.class,
    optionsOrder = {
      BigtableChangeStreamsToPubSubOptions.class,
      BigtableCommonOptions.ReadChangeStreamOptions.class,
      BigtableCommonOptions.ReadOptions.class
    },
    skipOptions = {
      "bigtableReadAppProfile",
      "bigtableAdditionalRetryCodes",
      "bigtableRpcAttemptTimeoutMs",
      "bigtableRpcTimeoutMs"
    },
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-bigtable-change-streams-to-pubsub",
    flexContainerName = "bigtable-changestreams-to-pubsub",
    contactInformation = "https://cloud.google.com/support",
    streaming = true,
    supportsAtLeastOnce = true)
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
      if (experiment.equalsIgnoreCase(USE_RUNNER_V2_EXPERIMENT)) {
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

    Topic topic = null;
    try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
      GetTopicRequest request =
          GetTopicRequest.newBuilder()
              .setTopic(
                  TopicName.ofProjectTopicName(
                          getPubSubProjectId(options), options.getPubSubTopic())
                      .toString())
              .build();
      topic = topicAdminClient.getTopic(request);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    try {
      if (!validateSchema(topic, options, sourceInfo)) {
        final String errorMessage = "Configured topic doesn't accept messages of configured format";
        throw new IllegalArgumentException(errorMessage);
      }

    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }

    PubSubDestination destinationInfo = newPubSubDestination(options, topic);
    PubSubUtils pubSub = new PubSubUtils(sourceInfo, destinationInfo);

    /*
     * Stages: 1) Read {@link ChangeStreamMutation} from change stream. 2) Create {@link
     * FailsafeElement} of {@link Mod} JSON and merge from: - {@link ChangeStreamMutation}. - GCS Dead
     * letter queue. 3) Convert {@link Mod} JSON into PubsubMessage and publish it to PubSub.
     * 4) Write Failures from 2) and 3) to GCS dead letter queue.
     */
    // Step 1
    Pipeline pipeline = Pipeline.create(options);

    // Register the coders for pipeline
    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(
        FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);

    DeadLetterQueueManager dlqManager = buildDlqManager(options);

    String dlqDirectory = dlqManager.getRetryDlqDirectoryWithDateTime();
    String tempDlqDirectory = dlqManager.getRetryDlqDirectory() + "tmp/";

    if (options.getDisableDlqRetries()) {
      LOG.info(
          "Disabling retries for the DLQ, directly writing into severe DLQ: {}",
          dlqManager.getSevereDlqDirectoryWithDateTime());
      dlqDirectory = dlqManager.getSevereDlqDirectoryWithDateTime();
      tempDlqDirectory = dlqManager.getSevereDlqDirectory() + "tmp/";
    }

    BigtableIO.ReadChangeStream readChangeStream =
        BigtableIO.readChangeStream()
            .withChangeStreamName(options.getBigtableChangeStreamName())
            .withExistingPipelineOptions(
                options.getBigtableChangeStreamResume()
                    ? BigtableIO.ExistingPipelineOptions.RESUME_OR_FAIL
                    : BigtableIO.ExistingPipelineOptions.FAIL_IF_EXISTS)
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
    // Step 2: just return the output for sending to pubSub and DLQ
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

    PCollection<FailsafeElement<String, String>> retryableDlqFailsafeModJson = null;
    if (options.getDisableDlqRetries()) {
      retryableDlqFailsafeModJson = pipeline.apply(Create.empty(FAILSAFE_ELEMENT_CODER));
    } else {
      retryableDlqFailsafeModJson =
          dlqModJson.get(DeadLetterQueueManager.RETRYABLE_ERRORS).setCoder(FAILSAFE_ELEMENT_CODER);
    }

    PCollection<FailsafeElement<String, String>> failsafeModJson =
        PCollectionList.of(sourceFailsafeModJson)
            .and(retryableDlqFailsafeModJson)
            .apply("Merge Source And DLQ Mod JSON", Flatten.pCollections());

    FailsafePublisher.FailsafeModJsonToPubsubMessageOptions failsafeModJsonToPubsubOptions =
        FailsafePublisher.FailsafeModJsonToPubsubMessageOptions.builder()
            .setCoder(FAILSAFE_ELEMENT_CODER)
            .build();

    PublishModJsonToTopic publishModJsonToTopic =
        new PublishModJsonToTopic(pubSub, failsafeModJsonToPubsubOptions);

    PCollection<FailsafeElement<String, String>> failedToPublish =
        failsafeModJson.apply("Publish Mod JSON To Pubsub", publishModJsonToTopic);

    PCollection<String> transformDlqJson =
        failedToPublish.apply(
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
            "Write Mod JSON With Non-retriable Error To DLQ",
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

  private static PubSubDestination newPubSubDestination(
      BigtableChangeStreamsToPubSubOptions options, Topic topic) {
    return new PubSubDestination(
        getPubSubProjectId(options),
        options.getPubSubTopic(),
        topic,
        options.getMessageFormat(),
        options.getMessageEncoding(),
        options.getUseBase64Rowkeys(),
        options.getUseBase64ColumnQualifiers(),
        options.getUseBase64Values(),
        options.getStripValues());
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

  private static Boolean validateSchema(
      Topic topic, BigtableChangeStreamsToPubSubOptions options, BigtableSource source)
      throws Exception {
    String messageFormatPath = topic.getSchemaSettings().getSchema();
    if (topic.getSchemaSettings().getSchema().isEmpty()) {
      validateIncompatibleEncoding(options);
      LOG.info(
          "Topic has no schema configured, pipeline will use message format: {}, message encoding: {}",
          options.getMessageFormat(),
          options.getMessageEncoding());
      return true;
    } else {
      SchemaName schemaName = SchemaName.parse(topic.getSchemaSettings().getSchema());
      Schema schema;
      try (SchemaServiceClient schemaServiceClient = SchemaServiceClient.create()) {
        schema = schemaServiceClient.getSchema(schemaName);
      }

      options.setMessageEncoding(toMessageEncoding(topic.getSchemaSettings().getEncoding()));

      Schema.Type schemaType = schema.getType();
      switch (schemaType) {
        case AVRO:
          options.setMessageFormat(MessageFormat.AVRO);
          validateNoUseOfBase64(options);
          break;
        case PROTOCOL_BUFFER:
          if (options.getMessageEncoding() == MessageEncoding.JSON) {
            options.setMessageFormat(MessageFormat.JSON);
          } else {
            options.setMessageFormat(MessageFormat.PROTOCOL_BUFFERS);
            validateNoUseOfBase64(options);
          }
          break;
        case TYPE_UNSPECIFIED:
        case UNRECOGNIZED:
          // Not overriding messageFormat, will try what customer configured or the default if
          // not configured
          break;
        default:
          throw new IllegalArgumentException("Topic schema type is not supported: " + schemaType);
      }

      LOG.info("Topic has schema configured: {}", topic.getSchemaSettings().getSchema());
      LOG.info(
          "Pipeline will use message format: {}, message encoding: {}",
          options.getMessageFormat(),
          options.getMessageEncoding());

      PubSubDestination destination = newPubSubDestination(options, topic);
      PubSubUtils pubSub = new PubSubUtils(source, destination);

      ByteString testChangeMessageData = createTestChangeMessage(pubSub).getData();
      Encoding encoding = toPubSubEncoding(options.getMessageEncoding());
      try (SchemaServiceClient schemaServiceClient = SchemaServiceClient.create()) {
        String testMessageEncoded = toBase64String(testChangeMessageData);
        LOG.info("Validating a test message (Base64 encoded): {}", testMessageEncoded);
        ValidateMessageRequest request =
            ValidateMessageRequest.newBuilder()
                .setParent("projects/" + pubSub.getDestination().getPubSubProject())
                .setEncoding(encoding)
                .setMessage(testChangeMessageData)
                .setName(messageFormatPath)
                .build();
        schemaServiceClient.validateMessage(request);
        LOG.info("Test message successfully validated.");
      } catch (Exception e) {
        throw new IllegalArgumentException("Failed to validate test message", e);
      }
    }
    return true;
  }

  private static void validateNoUseOfBase64(BigtableChangeStreamsToPubSubOptions options) {
    if (options.getUseBase64Values()) {
      throw new IllegalArgumentException(
          "useBase64Values values can only be used with topics accepting JSON messages");
    }
    if (options.getUseBase64Rowkeys()) {
      throw new IllegalArgumentException(
          "useBase64Rowkeys values can only be used with topics accepting JSON messages");
    }
    if (options.getUseBase64ColumnQualifiers()) {
      throw new IllegalArgumentException(
          "useBase64ColumnQualifiers values can only be used with topics accepting JSON messages");
    }
  }

  private static void validateIncompatibleEncoding(BigtableChangeStreamsToPubSubOptions options) {
    if (options.getMessageEncoding() == MessageEncoding.BINARY
        && options.getMessageFormat() == MessageFormat.JSON) {
      throw new IllegalArgumentException(
          "JSON message format is incompatible with BINARY message encoding");
    }
  }

  private static MessageEncoding toMessageEncoding(Encoding encoding) {
    if (encoding == null) {
      return MessageEncoding.JSON;
    }
    switch (encoding) {
      case JSON:
      case ENCODING_UNSPECIFIED:
      case UNRECOGNIZED:
        return MessageEncoding.JSON;
      case BINARY:
        return MessageEncoding.BINARY;
      default:
        throw new IllegalArgumentException("Topic has unsupported message encoding: " + encoding);
    }
  }

  private static String toBase64String(ByteString testChangeMessageData) {
    return Base64.getEncoder().encodeToString(testChangeMessageData.toByteArray());
  }

  private static Encoding toPubSubEncoding(MessageEncoding messageEncoding) {
    switch (messageEncoding) {
      case BINARY:
        return Encoding.BINARY;
      case JSON:
        return Encoding.JSON;
      default:
        throw new IllegalArgumentException("Unexpected message encoding: " + messageEncoding);
    }
  }

  private static PubsubMessage createTestChangeMessage(PubSubUtils pubSub) throws Exception {
    SetCell setCell =
        SetCell.create(
            "test_column_family",
            ByteString.copyFrom("test_column", Charset.defaultCharset()),
            1000L, // timestamp
            ByteString.copyFrom("test_value", Charset.defaultCharset()));

    TestChangeStreamMutation mutation =
        new TestChangeStreamMutation(
            "test_rowkey",
            MutationType.USER,
            "source_cluster",
            org.threeten.bp.Instant.now(), // commit timestamp
            1, // tiebreaker
            "token",
            org.threeten.bp.Instant.now(), // low watermark
            setCell);

    Mod mod = new Mod(pubSub.getSource(), mutation, setCell);

    switch (pubSub.getDestination().getMessageFormat()) {
      case AVRO:
        return pubSub.mapChangeJsonStringToPubSubMessageAsAvro(mod.getChangeJson());
      case PROTOCOL_BUFFERS:
        return pubSub.mapChangeJsonStringToPubSubMessageAsProto(mod.getChangeJson());
      case JSON:
        return pubSub.mapChangeJsonStringToPubSubMessageAsJson(mod.getChangeJson());
      default:
        throw new IllegalArgumentException(
            "Unexpected message format: " + pubSub.getDestination().getMessageFormat());
    }
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
