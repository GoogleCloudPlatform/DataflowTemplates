/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.clickhouse.templates;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.clickhouse.options.PubSubToClickHouseOptions;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.clickhouse.ClickHouseIO;
import org.apache.beam.sdk.io.clickhouse.TableSchema;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link PubSubToClickHouse} pipeline is a streaming pipeline which ingests JSON-encoded
 * messages from a Pub/Sub subscription and writes them into a ClickHouse table.
 *
 * <p>Messages that fail to parse or map to the target schema are routed to a dead-letter
 * destination — a ClickHouse table, a Pub/Sub topic, or both — determined implicitly by which of
 * {@code --clickHouseDeadLetterTable} and {@code --deadLetterTopic} are provided.
 *
 * <p>Check out the README for instructions on how to use or modify this template.
 */
@Template(
    name = "PubSub_to_ClickHouse",
    category = TemplateCategory.STREAMING,
    displayName = "Pub/Sub to ClickHouse",
    description =
        "The Pub/Sub to ClickHouse template is a streaming pipeline that reads JSON-encoded "
            + "messages from a Pub/Sub subscription and writes them to a ClickHouse table. "
            + "Messages that fail due to schema mismatch or malformed JSON are routed to a "
            + "dead-letter destination: a ClickHouse table, a Pub/Sub topic, or both — "
            + "determined implicitly by which of --clickHouseDeadLetterTable and "
            + "--deadLetterTopic are provided. "
            + "The target table and any ClickHouse dead-letter table must exist prior to execution.",
    optionsClass = PubSubToClickHouseOptions.class,
    flexContainerName = "pubsub-to-clickhouse",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-to-clickhouse",
    contactInformation = "https://cloud.google.com/support",
    preview = true,
    requirements = {
      "The Pub/Sub subscription must exist.",
      "Messages must be encoded as valid JSON.",
      "The ClickHouse target table must exist and column names must match the JSON message fields.",
      "When --clickHouseDeadLetterTable is set, the ClickHouse dead-letter table must exist "
          + "with schema: (raw_message String, error_message String, stack_trace String, failed_at String).",
      "When --deadLetterTopic is set, the dead-letter Pub/Sub topic must exist.",
      "At least one of --clickHouseDeadLetterTable or --deadLetterTopic must be provided."
    },
    streaming = true,
    supportsAtLeastOnce = true)
public class PubSubToClickHouse {

  private static final Logger LOG = LoggerFactory.getLogger(PubSubToClickHouse.class);

  /** Tag for successfully parsed rows routed to the main ClickHouse table. */
  public static final TupleTag<Row> TRANSFORM_OUT = new TupleTag<Row>() {};

  /** Tag for rows that failed parsing, routed to the dead-letter destination(s). */
  public static final TupleTag<Row> TRANSFORM_DEADLETTER_OUT = new TupleTag<Row>() {};

  /** Fixed schema for the ClickHouse dead-letter table. */
  public static final Schema DEADLETTER_SCHEMA =
      Schema.builder()
          .addStringField("raw_message")
          .addStringField("error_message")
          .addStringField("stack_trace")
          .addStringField("failed_at")
          .build();

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    PubSubToClickHouseOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubToClickHouseOptions.class);

    validateOptions(options);
    run(options);
  }

  /**
   * Validates that at least one dead-letter destination is provided.
   *
   * @param options pipeline options
   * @throws IllegalArgumentException if neither dead-letter destination is configured
   */
  public static void validateOptions(PubSubToClickHouseOptions options) {
    boolean hasClickHouseDlq =
        options.getClickHouseDeadLetterTable() != null
            && !options.getClickHouseDeadLetterTable().isEmpty();
    boolean hasPubSubDlq =
        options.getDeadLetterTopic() != null && !options.getDeadLetterTopic().isEmpty();

    if (!hasClickHouseDlq && !hasPubSubDlq) {
      throw new IllegalArgumentException(
          "At least one dead-letter destination must be provided: "
              + "--clickHouseDeadLetterTable and/or --deadLetterTopic.");
    }
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(PubSubToClickHouseOptions options) {
    boolean writeToClickHouseDlq =
        options.getClickHouseDeadLetterTable() != null
            && !options.getClickHouseDeadLetterTable().isEmpty();
    boolean writeToPubSubDlq =
        options.getDeadLetterTopic() != null && !options.getDeadLetterTopic().isEmpty();

    LOG.info(
        "Dead-letter destinations — ClickHouse: {}, PubSub: {}",
        writeToClickHouseDlq,
        writeToPubSubDlq);

    Properties props = new Properties();
    props.setProperty("user", options.getClickHouseUsername());
    props.setProperty("password", options.getClickHousePassword());
    props.setProperty("client_name", "PubSubToClickHouseTemplate");

    LOG.info("Fetching ClickHouse schema for table: {}", options.getClickHouseTable());

    TableSchema clickHouseSchema =
        ClickHouseIO.getTableSchema(
            options.getClickHouseUrl(),
            options.getClickHouseDatabase(),
            options.getClickHouseTable(),
            props);
    Schema beamSchema = TableSchema.getEquivalentSchema(clickHouseSchema);

    Pipeline pipeline = Pipeline.create(options);

    LOG.info("Reading from subscription: {}", options.getInputSubscription());

    /*
     * Step 1: Read JSON messages from Pub/Sub.
     */
    PCollection<PubsubMessage> messages =
        pipeline.apply(
            "Read PubSub Subscription",
            PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInputSubscription()));

    /*
     * Step 2: Parse JSON messages into Beam Rows.
     *         Successes  → TRANSFORM_OUT
     *         Failures   → TRANSFORM_DEADLETTER_OUT
     */
    PCollectionTuple parsed =
        messages.apply(
            "Parse PubSub Messages",
            ParDo.of(new PubSubMessageToClickHouseRowFn(beamSchema, clickHouseSchema))
                .withOutputTags(TRANSFORM_OUT, TupleTagList.of(TRANSFORM_DEADLETTER_OUT)));

    PCollection<Row> mainRows = parsed.get(TRANSFORM_OUT).setRowSchema(beamSchema);
    PCollection<Row> dlqRows = parsed.get(TRANSFORM_DEADLETTER_OUT).setRowSchema(DEADLETTER_SCHEMA);

    /*
     * Step 3: Apply windowing strategy.
     *   - windowSeconds only  → FixedWindows (time-based)
     *   - batchRowCount only  → GlobalWindows with count trigger
     *   - both or neither     → GlobalWindows with AfterFirst (combined: time OR count)
     */
    boolean hasTime = options.getWindowSeconds() != null;
    boolean hasCount = options.getBatchRowCount() != null;
    int windowSeconds = hasTime ? options.getWindowSeconds() : 30;
    int rowCount = hasCount ? options.getBatchRowCount() : 1000;

    PCollection<Row> windowedMain;
    PCollection<Row> windowedDlq;

    if (hasTime && !hasCount) {
      LOG.info("Windowing mode: time-based ({}s)", windowSeconds);
      Window<Row> timeWindow =
          Window.into(FixedWindows.of(Duration.standardSeconds(windowSeconds)));
      windowedMain = mainRows.apply("Time Window (main)", timeWindow);
      windowedDlq = dlqRows.apply("Time Window (dlq)", timeWindow);

    } else if (hasCount && !hasTime) {
      LOG.info("Windowing mode: count-based ({} rows)", rowCount);
      Window<Row> countWindow =
          Window.<Row>into(new GlobalWindows())
              .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(rowCount)))
              .discardingFiredPanes()
              .withAllowedLateness(Duration.ZERO);
      windowedMain = mainRows.apply("Count Window (main)", countWindow);
      windowedDlq = dlqRows.apply("Count Window (dlq)", countWindow);

    } else {
      LOG.info("Windowing mode: combined ({}s or {} rows)", windowSeconds, rowCount);
      Window<Row> combinedWindow =
          Window.<Row>into(new GlobalWindows())
              .triggering(
                  Repeatedly.forever(
                      AfterFirst.of(
                          AfterPane.elementCountAtLeast(rowCount),
                          AfterProcessingTime.pastFirstElementInPane()
                              .plusDelayOf(Duration.standardSeconds(windowSeconds)))))
              .discardingFiredPanes()
              .withAllowedLateness(Duration.ZERO);
      windowedMain = mainRows.apply("Combined Window (main)", combinedWindow);
      windowedDlq = dlqRows.apply("Combined Window (dlq)", combinedWindow);
    }

    /*
     * Step 4: Write successful rows to ClickHouse.
     */
    windowedMain.apply(
        "Write to ClickHouse",
        ClickHouseIO.<Row>write(
                options.getClickHouseUrl(),
                options.getClickHouseDatabase(),
                options.getClickHouseTable())
            .withProperties(props)
            .withMaxInsertBlockSize(options.getMaxInsertBlockSize())
            .withMaxRetries(options.getMaxRetries())
            .withInsertDeduplicate(options.getInsertDeduplicate())
            .withInsertDistributedSync(options.getInsertDistributedSync()));

    /*
     * Step 5: Route dead-letter rows to configured destination(s).
     *         ClickHouse DLQ: if --clickHouseDeadLetterTable is set
     *         PubSub DLQ:     if --deadLetterTopic is set
     *         Both:           if both are set
     */
    if (writeToClickHouseDlq) {
      windowedDlq.apply(
          "Write Dead-Letter to ClickHouse",
          ClickHouseIO.<Row>write(
                  options.getClickHouseUrl(),
                  options.getClickHouseDatabase(),
                  options.getClickHouseDeadLetterTable())
              .withProperties(props));
    }

    if (writeToPubSubDlq) {
      windowedDlq
          .apply(
              "Format Dead-Letter for PubSub",
              MapElements.into(TypeDescriptor.of(PubsubMessage.class))
                  .via(
                      row -> {
                        String rawMessage = row.getString("raw_message");
                        byte[] payload =
                            rawMessage != null
                                ? rawMessage.getBytes(StandardCharsets.UTF_8)
                                : new byte[0];
                        Map<String, String> attributes =
                            Map.of(
                                "errorMessage",
                                    row.getString("error_message") != null
                                        ? row.getString("error_message")
                                        : "",
                                "failedAt",
                                    row.getString("failed_at") != null
                                        ? row.getString("failed_at")
                                        : "");
                        return new PubsubMessage(payload, attributes);
                      }))
          .apply(
              "Write Dead-Letter to PubSub",
              PubsubIO.writeMessages().to(options.getDeadLetterTopic()));
    }

    return pipeline.run();
  }

  /**
   * A {@link DoFn} that parses incoming {@link PubsubMessage} payloads as JSON and maps them to
   * Beam {@link Row} objects matching the target ClickHouse table schema.
   *
   * <p>Successfully parsed rows are emitted to {@link PubSubToClickHouse#TRANSFORM_OUT}. Messages
   * that fail to parse or map are emitted to {@link PubSubToClickHouse#TRANSFORM_DEADLETTER_OUT}
   * with full error details for inspection and replay.
   */
  public static class PubSubMessageToClickHouseRowFn extends DoFn<PubsubMessage, Row> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String METRICS_NAMESPACE = "PubSubToClickHouse";

    /** Total PubSub messages that entered this DoFn (parsed + failed). */
    private static final Counter messagesReceived =
        Metrics.counter(METRICS_NAMESPACE, "messages-received");

    /** Messages successfully converted to a Beam Row and emitted to the main output. */
    private static final Counter rowsParsedOk =
        Metrics.counter(METRICS_NAMESPACE, "rows-parsed-ok");

    /** Messages that failed parsing or schema mapping and were routed to the dead-letter output. */
    private static final Counter rowsParseFailed =
        Metrics.counter(METRICS_NAMESPACE, "rows-parse-failed");

    /** Size distribution (bytes) of incoming PubSub message payloads. */
    private static final Distribution payloadBytes =
        Metrics.distribution(METRICS_NAMESPACE, "message-payload-bytes");

    private final Schema beamSchema;
    private final TableSchema clickHouseSchema;
    private Map<String, TableSchema.Column> columnMap;

    public PubSubMessageToClickHouseRowFn(Schema beamSchema, TableSchema clickHouseSchema) {
      this.beamSchema = beamSchema;
      this.clickHouseSchema = clickHouseSchema;
    }

    @Setup
    public void setup() {
      columnMap =
          clickHouseSchema.columns().stream()
              .collect(Collectors.toMap(TableSchema.Column::name, col -> col));
    }

    @ProcessElement
    public void processElement(@Element PubsubMessage message, MultiOutputReceiver out) {
      byte[] rawBytes = message.getPayload();
      String payload = new String(rawBytes, StandardCharsets.UTF_8);

      messagesReceived.inc();
      payloadBytes.update(rawBytes.length);

      try {
        JsonNode json = MAPPER.readTree(payload);
        if (json == null || json.isNull() || json.isMissingNode()) {
          throw new IllegalArgumentException("Message payload is empty or null");
        }
        Row.Builder rowBuilder = Row.withSchema(beamSchema);

        for (Schema.Field field : beamSchema.getFields()) {
          String fieldName = field.getName();
          JsonNode node = json.get(fieldName);
          TableSchema.Column column = columnMap.get(fieldName);

          if (column == null) {
            throw new IllegalArgumentException(
                "No ClickHouse column found for field: " + fieldName);
          }

          TableSchema.ColumnType columnType = column.columnType();

          if (node == null || node.isNull()) {
            rowBuilder.addValue(null);
            continue;
          }

          String rawValue = node.isTextual() ? node.asText() : node.toString();

          if (TableSchema.ColumnType.FLOAT32.typeName().equals(columnType.typeName())) {
            rowBuilder.addValue(Float.valueOf(rawValue));
          } else if (TableSchema.ColumnType.FLOAT64.typeName().equals(columnType.typeName())) {
            rowBuilder.addValue(Double.valueOf(rawValue));
          } else if (TableSchema.ColumnType.DATETIME.typeName().equals(columnType.typeName())
              || TableSchema.ColumnType.DATE.typeName().equals(columnType.typeName())) {
            rowBuilder.addValue(new DateTime(rawValue));
          } else if (Objects.equals(columnType.typeName().toString(), "ARRAY")) {
            if (!node.isArray() || node.isEmpty()) {
              rowBuilder.addValue(new ArrayList<>());
            } else {
              TableSchema.ColumnType elemType = columnType.arrayElementType();
              ArrayList<Object> list = new ArrayList<>();
              node.forEach(
                  elem -> {
                    String elemRaw = elem.isTextual() ? elem.asText() : elem.toString();
                    list.add(TableSchema.ColumnType.parseDefaultExpression(elemType, elemRaw));
                  });
              rowBuilder.addValue(list);
            }
          } else {
            rowBuilder.addValue(
                TableSchema.ColumnType.parseDefaultExpression(columnType, rawValue));
          }
        }

        out.get(TRANSFORM_OUT).output(rowBuilder.build());
        rowsParsedOk.inc();

      } catch (Exception e) {
        Row deadLetterRow =
            Row.withSchema(DEADLETTER_SCHEMA)
                .addValues(
                    payload,
                    e.getMessage() != null ? e.getMessage() : "null",
                    stackTraceToString(e),
                    DateTime.now().toString("yyyy-MM-dd HH:mm:ss"))
                .build();

        out.get(TRANSFORM_DEADLETTER_OUT).output(deadLetterRow);
        rowsParseFailed.inc();
      }
    }

    private static String stackTraceToString(Throwable t) {
      StringBuilder sb = new StringBuilder();
      Throwable current = t;
      while (current != null) {
        sb.append(current.toString()).append("\n");
        for (StackTraceElement el : current.getStackTrace()) {
          sb.append("  at ").append(el.toString()).append("\n");
        }
        current = current.getCause();
        if (current != null) {
          sb.append("Caused by: ");
        }
      }
      return sb.toString();
    }
  }
}
