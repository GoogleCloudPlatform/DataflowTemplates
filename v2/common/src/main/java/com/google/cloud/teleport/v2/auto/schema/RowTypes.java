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
package com.google.cloud.teleport.v2.auto.schema;

import static com.google.cloud.teleport.v2.auto.schema.RowTypes.PubSubMessageRow.PubSubMessageToRow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesAndMessageIdCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

public interface RowTypes extends Serializable {

  class FailsafeStringRow implements RowTypes {

    public static final Schema SCHEMA =
        Schema.of(
            Schema.Field.of("originalPayload", Schema.FieldType.STRING),
            Schema.Field.of("failed_row", Schema.FieldType.STRING),
            Schema.Field.of("error_message", Schema.FieldType.STRING),
            Schema.Field.of("stacktrace", Schema.FieldType.STRING));

    public static Row FailsafeStringToRow(FailsafeElement<String, String> failsafeElement) {
      Schema schema =
          Schema.of(
              Schema.Field.of("originalPayload", Schema.FieldType.STRING),
              Schema.Field.of("failed_row", Schema.FieldType.STRING),
              Schema.Field.of("error_message", Schema.FieldType.STRING),
              Schema.Field.of("stacktrace", Schema.FieldType.STRING));

      return Row.withSchema(schema)
          .withFieldValues(
              Map.of(
                  "originalPayload",
                  failsafeElement.getOriginalPayload(),
                  "failed_row",
                  failsafeElement.getPayload(),
                  "error_message",
                  failsafeElement.getErrorMessage(),
                  "stacktrace",
                  failsafeElement.getStacktrace()))
          .build();
    }

    public static FailsafeElement<String, String> rowToFailsafeString(Row row) {
      Row message = row.getValue("originalPayload");
      return FailsafeElement.of(row.getString("originalPayload"), row.getString("failed_row"))
          .setErrorMessage(row.getValue("failed_row"))
          .setStacktrace(row.getValue("stacktrace"));
    }
  }

  class FailsafePubSubRow implements RowTypes {

    public static final Schema SCHEMA =
        Schema.of(
            Schema.Field.of("originalPayload", Schema.FieldType.row(PubSubMessageRow.SCHEMA)),
            Schema.Field.of("failed_row", Schema.FieldType.STRING),
            Schema.Field.of("error_message", Schema.FieldType.STRING),
            Schema.Field.of("stacktrace", Schema.FieldType.STRING));

    public static class MapToRow extends PTransform<PCollection<FailsafeElement<PubsubMessage, String>>, PCollection<Row>> {

      @Override
      public PCollection<Row> expand(PCollection<FailsafeElement<PubsubMessage, String>> input) {
        return input.apply(MapElements.into(TypeDescriptor.of(Row.class))
                .via(RowTypes.FailsafePubSubRow::FailsafePubSubRowToRow))
            .setCoder(RowCoder.of(SCHEMA));
      }

      public static FailsafePubSubRow.MapToRow of() {
        return new FailsafePubSubRow.MapToRow();
      }
    }

    public static Row FailsafePubSubRowToRow(
        FailsafeElement<PubsubMessage, String> failsafeElement) {
      Row pubsubRow = PubSubMessageToRow(failsafeElement.getOriginalPayload());

      Schema schema =
          Schema.of(
              Schema.Field.of("originalPayload", Schema.FieldType.row(pubsubRow.getSchema())),
              Schema.Field.of("failed_row", Schema.FieldType.STRING),
              Schema.Field.of("error_message", Schema.FieldType.STRING),
              Schema.Field.of("stacktrace", Schema.FieldType.STRING));

      return Row.withSchema(schema)
          .withFieldValues(
              Map.of(
                  "originalPayload",
                  pubsubRow,
                  "failed_row",
                  failsafeElement.getPayload(),
                  "error_message",
                  failsafeElement.getErrorMessage(),
                  "stacktrace",
                  failsafeElement.getStacktrace()))
          .build();
    }

    public static FailsafeElement<PubsubMessage, String> rowToFailsafePubSub(Row row) {
      Row message = row.getValue("originalPayload");
      return FailsafeElement.of(
              new PubsubMessage(
                  message.getString("message").getBytes(),
                  message.getMap("attributes"),
                  message.getString("messageId")),
              row.getString("failed_row"))
          .setErrorMessage(row.getValue("failed_row"))
          .setStacktrace(row.getValue("stacktrace"));
    }
  }

  class PubSubMessageRow implements RowTypes {

    public static final Schema SCHEMA =
        Schema.builder()
            .addNullableStringField("messageId")
            .addNullableStringField("message")
            .addMapField("attributes", Schema.FieldType.STRING, Schema.FieldType.STRING)
            .build();

    public static class MapToRow extends PTransform<PCollection<PubsubMessage>, PCollection<Row>> {

      @Override
      public PCollection<Row> expand(PCollection<PubsubMessage> input) {
        return input.apply(MapElements.into(TypeDescriptor.of(Row.class))
            .via(RowTypes.PubSubMessageRow::PubSubMessageToRow))
            .setCoder(RowCoder.of(RowTypes.PubSubMessageRow.SCHEMA));
      }

      public static MapToRow of() {
        return new MapToRow();
      }
    }

    public static class MapToPubSubMessage extends PTransform<PCollection<Row>, PCollection<PubsubMessage>> {

      @Override
      public PCollection<PubsubMessage> expand(PCollection<Row> input) {
        return input.apply(MapElements.into(TypeDescriptor.of(PubsubMessage.class))
                .via(RowTypes.PubSubMessageRow::RowToPubSubMessage))
            .setCoder(PubsubMessageWithAttributesAndMessageIdCoder.of());
      }
    }

    public static Row PubSubMessageToRow(PubsubMessage message) {
      String messageId = message.getMessageId();
      String payload = new String(message.getPayload());
      Map<String, String> attributeMap = new HashMap<>();
      if (message.getAttributeMap() != null) {
        attributeMap.putAll(message.getAttributeMap());
      }

      return Row.withSchema(SCHEMA)
          .withFieldValues(
              Map.of(
                  "messageId", messageId,
                  "message", payload,
                  "attributes", attributeMap))
          .build();
    }

    public static PubsubMessage RowToPubSubMessage(Row row) {
      String messageId = row.getValue("messageId");
      String payload = row.getValue("message");
      Map<String, String> attributeMap = row.getValue("attributes");

      assert payload != null;
      return new PubsubMessage(payload.getBytes(), attributeMap, messageId);
    }
  }

  class SchemaTableRow implements RowTypes {
    public static final Schema SCHEMA =
        Schema.builder().addField("row", Schema.FieldType.STRING).build();

    public static Row TableRowToRow(TableRow row) {
      try {
        ObjectMapper objectMapper = new ObjectMapper();
        String rowString = objectMapper.writeValueAsString(row);
        return Row.withSchema(SCHEMA).withFieldValue("row", rowString).build();
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    public static TableRow RowToTableRow(Row row) {
      try {
        return new ObjectMapper().readValue(row.getString("row"), TableRow.class);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
