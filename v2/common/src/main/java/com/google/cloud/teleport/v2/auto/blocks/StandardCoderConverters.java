package com.google.cloud.teleport.v2.auto.blocks;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesAndMessageIdCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StandardCoderConverters {

  private static class PubSubMessageToRowFn extends DoFn<PubsubMessage, Row> {

    private final Schema schema;

    public PubSubMessageToRowFn(Schema schema) {
      super();
      this.schema = schema;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws JsonProcessingException {
      PubsubMessage message = context.element();

      assert message != null;
      String messageId = message.getMessageId();
      String payload = new String(message.getPayload());

      Row row = Row.withSchema(schema).withFieldValues(Map.of("messageId", messageId, "message", payload, "attributes", message.getAttributeMap())).build();

      context.output(row);
    }
  }

  public static PCollection<Row> pubSubMessageToRow(PCollection<PubsubMessage> collection) {
    Schema schema = Schema.builder()
        .addNullableStringField("messageId")
        .addNullableStringField("message")
        .addMapField("attributes", Schema.FieldType.STRING, Schema.FieldType.STRING)
        .build();

    return collection
        .apply("PubSubMessageToRow",
            ParDo.of(new PubSubMessageToRowFn(schema)))
        .setCoder(RowCoder.of(schema));
  }

  private static class RowToTableRowFn extends DoFn<Row, TableRow> {
    @ProcessElement
    public void processElement(ProcessContext context) throws JsonProcessingException {
      Row row = context.element();

      assert row != null;

      ObjectMapper objectMapper = new ObjectMapper();
      TableRow tableRow = objectMapper.readValue(row.getString("row"), TableRow.class);

      context.output(tableRow);
    }
  }

  public static PCollection<TableRow> rowToTableRow(PCollection<Row> tableRow) {
    return tableRow.apply(ParDo.of(new RowToTableRowFn()))
        .setCoder(TableRowJsonCoder.of());
  }

  private static class TableRowToRowFn extends DoFn<TableRow, Row> {

    private final Schema schema;

    private TableRowToRowFn(Schema schema) {
      super();
      this.schema = schema;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws JsonProcessingException {
      TableRow tableRow = context.element();

      assert tableRow != null;

      ObjectMapper objectMapper = new ObjectMapper();
      String rowString = objectMapper.writeValueAsString(tableRow);

      Row row = Row.withSchema(schema).withFieldValue("row", rowString).build();
      context.output(row);
    }
  }

  public static PCollection<Row> tableRowToRow(PCollection<TableRow> tableRow) {
    Schema schema = Schema.builder()
        .addField("row", Schema.FieldType.STRING)
        .build();
    return tableRow.apply(ParDo.of(new TableRowToRowFn(schema)))
        .setCoder(RowCoder.of(schema));
  }

  private static Row constructRow(Map<String, Object> tableRow, Schema.Builder schema) {
    for (String key : tableRow.keySet()) {
      Object value = tableRow.get(key);
      Schema.Builder innerSchema = Schema.builder();
      if (value instanceof Map) {
        tableRow.put(key, constructRow((Map<String, Object>) tableRow.get(key), innerSchema));
      }
      schema.addField(key, getType(tableRow.get(key)));
    }

    return Row.withSchema(schema.build()).withFieldValues(tableRow).build();
  }

  private static Schema.FieldType getType(Object type) {
    if (type instanceof Integer) {
      return Schema.FieldType.INT32;
    } else if (type instanceof Long) {
      return Schema.FieldType.INT64;
    } else if (type instanceof Short) {
      return Schema.FieldType.INT16;
    } else if (type instanceof Float) {
      return Schema.FieldType.FLOAT;
    } else if (type instanceof String) {
      return Schema.FieldType.STRING;
    } else if (type instanceof Boolean) {
      return Schema.FieldType.BOOLEAN;
    } else if (type instanceof List) {
      return Schema.FieldType.array(getType(type.getClass().getComponentType()));
    } else if (type instanceof Map) {
      Schema.Builder schema = Schema.builder();
      for (String key : (((Map<String, ?>) type).keySet())) {
        schema.addField(key, getType(((Map<?, ?>) type).get(key)));
      }
      return Schema.FieldType.row(schema.build());
    } else if (type instanceof Row) {
      return Schema.FieldType.row(((Row) type).getSchema());
    }
    return Schema.FieldType.BYTES;
  }

  private static class RowToPubSubMessageFn extends DoFn<Row, PubsubMessage> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      Row message = context.element();

      assert message != null;

      String messageId = message.getValue("messageId");
      String payload = message.getValue("message");
      Map<String, String> attributeMap = message.getValue("attributes");

      PubsubMessage pubsubMessage = new PubsubMessage(payload.getBytes(), attributeMap, messageId);

      context.output(pubsubMessage);
    }
  }

  public static PCollection<PubsubMessage> RowToPubSubMessage(PCollection<Row> collection) {
    return collection.apply("RowToPubSubMessage",
            ParDo.of(new RowToPubSubMessageFn()))
        .setCoder(PubsubMessageWithAttributesAndMessageIdCoder.of());
  }

  private static class FailsafeElementToRowFn extends DoFn<FailsafeElement<PubsubMessage, String>, Row> {
    Schema schema = Schema.builder()
        .addField("payload", Schema.FieldType.STRING)
        .addField("errorMessage", Schema.FieldType.STRING)
        .addField("stackTrace", Schema.FieldType.STRING)
        .build();
    @ProcessElement
    public void processElement(ProcessContext context) {
      FailsafeElement<PubsubMessage, String> element = context.element();

      Map<String, Object> values = new HashMap<>();
      values.put("payload", element.getPayload());
      values.put("errorMessage", element.getErrorMessage());
      values.put("stackTrace", element.getStacktrace());

      Row row = Row.withSchema(schema).withFieldValues(values).build();

      context.output(row);
    }
  }

  public static PCollection<Row> failsafeElementToRow(PCollection<FailsafeElement<PubsubMessage, String>> collection) {
    return collection.apply("NormalizeFailsafeElement",
        ParDo.of(new FailsafeElementToRowFn()));
  }
}
