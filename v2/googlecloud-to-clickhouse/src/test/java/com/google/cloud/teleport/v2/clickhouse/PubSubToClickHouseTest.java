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
package com.google.cloud.teleport.v2.clickhouse;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.clickhouse.options.PubSubToClickHouseOptions;
import com.google.cloud.teleport.v2.clickhouse.templates.PubSubToClickHouse;
import com.google.cloud.teleport.v2.clickhouse.templates.PubSubToClickHouse.PubSubMessageToClickHouseRowFn;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.beam.sdk.io.clickhouse.TableSchema;
import org.apache.beam.sdk.io.clickhouse.TableSchema.Column;
import org.apache.beam.sdk.io.clickhouse.TableSchema.ColumnType;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link PubSubToClickHouse}. */
@RunWith(JUnit4.class)
public class PubSubToClickHouseTest {

  @Rule public final TestPipeline pipeline = TestPipeline.create();

  // ── Shared string schema ─────────────────────────────────────────────────

  private static final TableSchema CH_STRING_SCHEMA =
      TableSchema.of(
          Column.of("id", ColumnType.nullable(ColumnType.STRING.typeName())),
          Column.of("name", ColumnType.nullable(ColumnType.STRING.typeName())));

  private static final Schema STRING_SCHEMA = TableSchema.getEquivalentSchema(CH_STRING_SCHEMA);

  // ── Helpers ──────────────────────────────────────────────────────────────

  private static PubsubMessage message(String json) {
    return new PubsubMessage(json.getBytes(StandardCharsets.UTF_8), ImmutableMap.of());
  }

  private PCollectionTuple applyFn(PubSubMessageToClickHouseRowFn fn, List<PubsubMessage> msgs) {
    return pipeline
        .apply(Create.of(msgs).withCoder(PubsubMessageWithAttributesCoder.of()))
        .apply(
            ParDo.of(fn)
                .withOutputTags(
                    PubSubToClickHouse.TRANSFORM_OUT,
                    TupleTagList.of(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT)));
  }

  private PubSubMessageToClickHouseRowFn stringFn() {
    return new PubSubMessageToClickHouseRowFn(STRING_SCHEMA, CH_STRING_SCHEMA);
  }

  // ── validateOptions ──────────────────────────────────────────────────────

  @Test
  public void testValidateOptionsNoDlqDestinationThrows() {
    PubSubToClickHouseOptions options = PipelineOptionsFactory.as(PubSubToClickHouseOptions.class);

    assertThrows(IllegalArgumentException.class, () -> PubSubToClickHouse.validateOptions(options));
  }

  @Test
  public void testValidateOptionsEmptyStringDlqThrows() {
    PubSubToClickHouseOptions options = PipelineOptionsFactory.as(PubSubToClickHouseOptions.class);
    options.setClickHouseDeadLetterTable("");
    options.setDeadLetterTopic("");

    assertThrows(IllegalArgumentException.class, () -> PubSubToClickHouse.validateOptions(options));
  }

  @Test
  public void testValidateOptionsClickHouseDlqOnlyAccepted() {
    PubSubToClickHouseOptions options = PipelineOptionsFactory.as(PubSubToClickHouseOptions.class);
    options.setClickHouseDeadLetterTable("my_dlq_table");

    PubSubToClickHouse.validateOptions(options); // must not throw
  }

  @Test
  public void testValidateOptionsPubSubDlqOnlyAccepted() {
    PubSubToClickHouseOptions options = PipelineOptionsFactory.as(PubSubToClickHouseOptions.class);
    options.setDeadLetterTopic("projects/my-project/topics/my-dlq");

    PubSubToClickHouse.validateOptions(options); // must not throw
  }

  @Test
  public void testValidateOptionsBothDlqDestinationsAccepted() {
    PubSubToClickHouseOptions options = PipelineOptionsFactory.as(PubSubToClickHouseOptions.class);
    options.setClickHouseDeadLetterTable("my_dlq_table");
    options.setDeadLetterTopic("projects/my-project/topics/my-dlq");

    PubSubToClickHouse.validateOptions(options); // must not throw
  }

  // ── DoFn: happy path ─────────────────────────────────────────────────────

  @Test
  public void testValidJsonParsedToMainOutput() {
    PCollectionTuple result =
        applyFn(stringFn(), List.of(message("{\"id\": \"1\", \"name\": \"Alice\"}")));

    result.get(PubSubToClickHouse.TRANSFORM_OUT).setRowSchema(STRING_SCHEMA);
    result
        .get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT)
        .setRowSchema(PubSubToClickHouse.DEADLETTER_SCHEMA);

    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_OUT))
        .containsInAnyOrder(Row.withSchema(STRING_SCHEMA).addValues("1", "Alice").build());
    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT)).empty();

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testMultipleValidMessagesParsed() {
    PCollectionTuple result =
        applyFn(
            stringFn(),
            List.of(
                message("{\"id\": \"1\", \"name\": \"Alice\"}"),
                message("{\"id\": \"2\", \"name\": \"Bob\"}"),
                message("{\"id\": \"3\", \"name\": \"Carol\"}")));

    result.get(PubSubToClickHouse.TRANSFORM_OUT).setRowSchema(STRING_SCHEMA);
    result
        .get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT)
        .setRowSchema(PubSubToClickHouse.DEADLETTER_SCHEMA);

    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_OUT))
        .containsInAnyOrder(
            Row.withSchema(STRING_SCHEMA).addValues("1", "Alice").build(),
            Row.withSchema(STRING_SCHEMA).addValues("2", "Bob").build(),
            Row.withSchema(STRING_SCHEMA).addValues("3", "Carol").build());
    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT)).empty();

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testNullJsonFieldProducesNullInRow() {
    PCollectionTuple result =
        applyFn(stringFn(), List.of(message("{\"id\": null, \"name\": \"Alice\"}")));

    result.get(PubSubToClickHouse.TRANSFORM_OUT).setRowSchema(STRING_SCHEMA);
    result
        .get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT)
        .setRowSchema(PubSubToClickHouse.DEADLETTER_SCHEMA);

    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_OUT))
        .containsInAnyOrder(Row.withSchema(STRING_SCHEMA).addValues(null, "Alice").build());
    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT)).empty();

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testMissingJsonFieldTreatedAsNull() {
    // "name" is absent — should produce a null value, not an error
    PCollectionTuple result = applyFn(stringFn(), List.of(message("{\"id\": \"42\"}")));

    result.get(PubSubToClickHouse.TRANSFORM_OUT).setRowSchema(STRING_SCHEMA);
    result
        .get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT)
        .setRowSchema(PubSubToClickHouse.DEADLETTER_SCHEMA);

    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_OUT))
        .containsInAnyOrder(Row.withSchema(STRING_SCHEMA).addValues("42", null).build());
    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT)).empty();

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testExtraJsonFieldsAreIgnored() {
    // "unknown" key is not in the schema and should be silently dropped
    PCollectionTuple result =
        applyFn(
            stringFn(),
            List.of(message("{\"id\": \"7\", \"name\": \"Dave\", \"unknown\": \"extra\"}")));

    result.get(PubSubToClickHouse.TRANSFORM_OUT).setRowSchema(STRING_SCHEMA);
    result
        .get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT)
        .setRowSchema(PubSubToClickHouse.DEADLETTER_SCHEMA);

    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_OUT))
        .containsInAnyOrder(Row.withSchema(STRING_SCHEMA).addValues("7", "Dave").build());
    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT)).empty();

    pipeline.run().waitUntilFinish();
  }

  // ── DoFn: dead-letter routing ────────────────────────────────────────────

  @Test
  public void testInvalidJsonRoutedToDeadLetter() {
    String badPayload = "not valid json {{";
    PCollectionTuple result = applyFn(stringFn(), List.of(message(badPayload)));

    result.get(PubSubToClickHouse.TRANSFORM_OUT).setRowSchema(STRING_SCHEMA);
    result
        .get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT)
        .setRowSchema(PubSubToClickHouse.DEADLETTER_SCHEMA);

    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_OUT)).empty();
    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT))
        .satisfies(
            rows -> {
              Row dlq = rows.iterator().next();
              assertThat(dlq.getString("raw_message")).isEqualTo(badPayload);
              assertThat(dlq.getString("error_message")).isNotEmpty();
              assertThat(dlq.getString("stack_trace")).isNotEmpty();
              assertThat(dlq.getDateTime("failed_at")).isNotNull();
              return null;
            });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testEmptyPayloadRoutedToDeadLetter() {
    PCollectionTuple result = applyFn(stringFn(), List.of(message("")));

    result.get(PubSubToClickHouse.TRANSFORM_OUT).setRowSchema(STRING_SCHEMA);
    result
        .get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT)
        .setRowSchema(PubSubToClickHouse.DEADLETTER_SCHEMA);

    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_OUT)).empty();
    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT))
        .satisfies(
            rows -> {
              assertThat(rows).hasSize(1);
              return null;
            });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testMixedMessagesRoutedToCorrectOutputs() {
    PCollectionTuple result =
        applyFn(
            stringFn(),
            List.of(
                message("{\"id\": \"1\", \"name\": \"Alice\"}"),
                message("not json"),
                message("{\"id\": \"2\", \"name\": \"Bob\"}"),
                message("{bad}")));

    result.get(PubSubToClickHouse.TRANSFORM_OUT).setRowSchema(STRING_SCHEMA);
    result
        .get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT)
        .setRowSchema(PubSubToClickHouse.DEADLETTER_SCHEMA);

    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_OUT))
        .containsInAnyOrder(
            Row.withSchema(STRING_SCHEMA).addValues("1", "Alice").build(),
            Row.withSchema(STRING_SCHEMA).addValues("2", "Bob").build());
    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT))
        .satisfies(
            rows -> {
              assertThat(rows).hasSize(2);
              return null;
            });

    pipeline.run().waitUntilFinish();
  }

  // ── DoFn: numeric types ──────────────────────────────────────────────────

  @Test
  public void testFloat32FieldParsedToFloat() {
    TableSchema chSchema = TableSchema.of(Column.of("value", ColumnType.FLOAT32));
    Schema beamSchema = TableSchema.getEquivalentSchema(chSchema);
    PubSubMessageToClickHouseRowFn fn = new PubSubMessageToClickHouseRowFn(beamSchema, chSchema);

    PCollectionTuple result = applyFn(fn, List.of(message("{\"value\": 1.5}")));

    result.get(PubSubToClickHouse.TRANSFORM_OUT).setRowSchema(beamSchema);
    result
        .get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT)
        .setRowSchema(PubSubToClickHouse.DEADLETTER_SCHEMA);

    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_OUT))
        .containsInAnyOrder(Row.withSchema(beamSchema).addValues(1.5f).build());
    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT)).empty();

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testFloat64FieldParsedToDouble() {
    TableSchema chSchema = TableSchema.of(Column.of("value", ColumnType.FLOAT64));
    Schema beamSchema = TableSchema.getEquivalentSchema(chSchema);
    PubSubMessageToClickHouseRowFn fn = new PubSubMessageToClickHouseRowFn(beamSchema, chSchema);

    PCollectionTuple result = applyFn(fn, List.of(message("{\"value\": 2.5}")));

    result.get(PubSubToClickHouse.TRANSFORM_OUT).setRowSchema(beamSchema);
    result
        .get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT)
        .setRowSchema(PubSubToClickHouse.DEADLETTER_SCHEMA);

    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_OUT))
        .containsInAnyOrder(Row.withSchema(beamSchema).addValues(2.5).build());
    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT)).empty();

    pipeline.run().waitUntilFinish();
  }

  // ── DoFn: date/time types ────────────────────────────────────────────────

  @Test
  public void testDateTimeFieldParsedToMainOutput() {
    TableSchema chSchema = TableSchema.of(Column.of("ts", ColumnType.DATETIME));
    Schema beamSchema = TableSchema.getEquivalentSchema(chSchema);
    PubSubMessageToClickHouseRowFn fn = new PubSubMessageToClickHouseRowFn(beamSchema, chSchema);

    PCollectionTuple result = applyFn(fn, List.of(message("{\"ts\": \"2025-01-15T10:30:00Z\"}")));

    result.get(PubSubToClickHouse.TRANSFORM_OUT).setRowSchema(beamSchema);
    result
        .get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT)
        .setRowSchema(PubSubToClickHouse.DEADLETTER_SCHEMA);

    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_OUT))
        .satisfies(
            rows -> {
              assertThat(rows).hasSize(1);
              return null;
            });
    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT)).empty();

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testInvalidDateTimeRoutedToDeadLetter() {
    TableSchema chSchema = TableSchema.of(Column.of("ts", ColumnType.DATETIME));
    Schema beamSchema = TableSchema.getEquivalentSchema(chSchema);
    PubSubMessageToClickHouseRowFn fn = new PubSubMessageToClickHouseRowFn(beamSchema, chSchema);

    PCollectionTuple result = applyFn(fn, List.of(message("{\"ts\": \"not-a-date\"}")));

    result.get(PubSubToClickHouse.TRANSFORM_OUT).setRowSchema(beamSchema);
    result
        .get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT)
        .setRowSchema(PubSubToClickHouse.DEADLETTER_SCHEMA);

    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_OUT)).empty();
    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT))
        .satisfies(
            rows -> {
              assertThat(rows).hasSize(1);
              return null;
            });

    pipeline.run().waitUntilFinish();
  }

  // ── DoFn: array types ────────────────────────────────────────────────────

  @Test
  public void testArrayFieldParsedToMainOutput() {
    TableSchema chSchema = TableSchema.of(Column.of("tags", ColumnType.array(ColumnType.STRING)));
    Schema beamSchema = TableSchema.getEquivalentSchema(chSchema);
    PubSubMessageToClickHouseRowFn fn = new PubSubMessageToClickHouseRowFn(beamSchema, chSchema);

    PCollectionTuple result = applyFn(fn, List.of(message("{\"tags\": [\"a\", \"b\", \"c\"]}")));

    result.get(PubSubToClickHouse.TRANSFORM_OUT).setRowSchema(beamSchema);
    result
        .get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT)
        .setRowSchema(PubSubToClickHouse.DEADLETTER_SCHEMA);

    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_OUT))
        .satisfies(
            rows -> {
              Row row = rows.iterator().next();
              assertThat(row.<List<Object>>getValue("tags")).containsExactly("a", "b", "c");
              return null;
            });
    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT)).empty();

    pipeline.run().waitUntilFinish();
  }

  // ── stackTraceToString ───────────────────────────────────────────────────

  @Test
  public void testDeadLetterStackTraceContainsStackFrames() {
    PCollectionTuple result = applyFn(stringFn(), List.of(message("not valid json {{")));

    result.get(PubSubToClickHouse.TRANSFORM_OUT).setRowSchema(STRING_SCHEMA);
    result
        .get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT)
        .setRowSchema(PubSubToClickHouse.DEADLETTER_SCHEMA);

    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT))
        .satisfies(
            rows -> {
              String stackTrace = rows.iterator().next().getString("stack_trace");
              assertThat(stackTrace).contains("at ");
              return null;
            });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testStackTraceToStringWithCyclicCauseDoesNotHang() throws Exception {
    // Create a cyclic cause chain by overriding getCause() — avoids JDK field reflection
    // restrictions and still exercises printStackTrace's cycle detection.
    class CyclicException extends RuntimeException {
      private Throwable customCause;

      CyclicException(String message) {
        super(message);
      }

      void setCustomCause(Throwable cause) {
        this.customCause = cause;
      }

      @Override
      public Throwable getCause() {
        return customCause;
      }
    }

    CyclicException e1 = new CyclicException("first");
    CyclicException e2 = new CyclicException("second");
    e1.setCustomCause(e2);
    e2.setCustomCause(e1); // e1 → e2 → e1 (cycle)

    Method method =
        PubSubMessageToClickHouseRowFn.class.getDeclaredMethod(
            "stackTraceToString", Throwable.class);
    method.setAccessible(true);

    String result = (String) method.invoke(null, e1);

    assertThat(result).contains("first");
    assertThat(result).contains("CIRCULAR REFERENCE");
  }

  @Test
  public void testEmptyArrayFieldParsedToEmptyList() {
    TableSchema chSchema = TableSchema.of(Column.of("tags", ColumnType.array(ColumnType.STRING)));
    Schema beamSchema = TableSchema.getEquivalentSchema(chSchema);
    PubSubMessageToClickHouseRowFn fn = new PubSubMessageToClickHouseRowFn(beamSchema, chSchema);

    PCollectionTuple result = applyFn(fn, List.of(message("{\"tags\": []}")));

    result.get(PubSubToClickHouse.TRANSFORM_OUT).setRowSchema(beamSchema);
    result
        .get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT)
        .setRowSchema(PubSubToClickHouse.DEADLETTER_SCHEMA);

    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_OUT))
        .satisfies(
            rows -> {
              Row row = rows.iterator().next();
              assertThat(row.<List<Object>>getValue("tags")).isEmpty();
              return null;
            });
    PAssert.that(result.get(PubSubToClickHouse.TRANSFORM_DEADLETTER_OUT)).empty();

    pipeline.run().waitUntilFinish();
  }
}
