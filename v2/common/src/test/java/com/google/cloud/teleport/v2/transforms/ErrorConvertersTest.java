/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.v2.transforms;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse.InsertErrors;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.transforms.ErrorConverters.FailedStringToTableRowFn;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertErrorCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Ascii;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit Tests {@link ErrorConverters}. */
@RunWith(JUnit4.class)
public class ErrorConvertersTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testErrorLogs() throws IOException {
    TupleTag<String> errorTag = new TupleTag<String>("errors") {};
    TupleTag<String> goodTag = new TupleTag<String>("good") {};

    TemporaryFolder tmpFolder = new TemporaryFolder();
    tmpFolder.create();

    pipeline
        .apply(Create.of("Hello", "World", "Colin"))
        .apply(
            ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        if (c.element().equals("Hello")) {
                          c.output(c.element());
                        } else {
                          c.output(errorTag, c.element());
                        }
                      }
                    })
                .withOutputTags(goodTag, TupleTagList.of(errorTag)))
        .apply(
            ErrorConverters.LogErrors.newBuilder()
                .setErrorWritePath(tmpFolder.getRoot().getAbsolutePath() + "errors.txt")
                .setErrorTag(errorTag)
                .build());

    pipeline.run();

    // Read in tempfile data
    File file = new File(tmpFolder.getRoot().getAbsolutePath() + "errors.txt-00000-of-00001");
    String fileContents = Files.toString(file, Charsets.UTF_8);
    tmpFolder.delete();

    // Get the unique expected & received lines of text
    HashSet<String> expected = new HashSet<>();
    Collections.addAll(expected, "World", "Colin");

    HashSet<String> result = new HashSet<>();
    Collections.addAll(result, fileContents.split("\n"));

    assertThat(result).isEqualTo(expected);
  }

  /**
   * Tests that {@link ErrorConverters.FailedStringToTableRowFn} properly formats failed String
   * objects into {@link TableRow} objects to save to BigQuery.
   */
  @Test
  public void testFailedStringMessageToTableRowFn() {
    // Test input
    final String message = "Super secret";
    final String errorMessage = "Failed to parse input JSON";
    final String stacktrace = "Error at com.google.cloud.teleport.TextToBigQueryStreaming";

    final FailsafeElement<String, String> input =
        FailsafeElement.of(message, message)
            .setErrorMessage(errorMessage)
            .setStacktrace(stacktrace);

    final Instant timestamp =
        new DateTime(2022, 2, 22, 22, 22, 22, 222, DateTimeZone.UTC).toInstant();

    // Register the coder for the pipeline. This prevents having to invoke .setCoder() on
    // many transforms.
    FailsafeElementCoder<String, String> coder =
        FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    // Build pipeline
    PCollection<TableRow> output =
        pipeline
            .apply(
                "CreateInput",
                Create.timestamped(TimestampedValue.of(input, timestamp)).withCoder(coder))
            .apply("FailedRecordToTableRow", ParDo.of(new FailedStringToTableRowFn()));

    // Assert
    PAssert.that(output)
        .satisfies(
            collection -> {
              final TableRow result = collection.iterator().next();
              assertThat(result.get("timestamp")).isEqualTo("2022-02-22 22:22:22.222000");
              assertThat(result.get("attributes")).isNull();
              assertThat(result.get("payloadString")).isEqualTo(message);
              assertThat(result.get("payloadBytes")).isNotNull();
              assertThat(result.get("errorMessage")).isEqualTo(errorMessage);
              assertThat(result.get("stacktrace")).isEqualTo(stacktrace);
              return null;
            });

    // Execute pipeline
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void transformConvertsBigQueryInsertErrorToPubsubMessage() throws IOException {

    GenericRecord expectedRecord = BigQueryConvertersTest.generateNestedAvroRecord();
    String errorMessage = "small-test-message";
    BigQueryInsertError bigQueryInsertError = getBigQueryInsertError(expectedRecord, errorMessage);
    ErrorConverters.BigQueryInsertErrorToPubsubMessage<GenericRecord> converter =
        getConverter(expectedRecord.getSchema(), AvroCoder.of(expectedRecord.getSchema()));

    PCollection<PubsubMessage> output =
        pipeline
            .apply(Create.of(bigQueryInsertError).withCoder(BigQueryInsertErrorCoder.of()))
            .apply(converter);

    PubsubMessage expectedMessage =
        getPubsubMessage(expectedRecord, bigQueryInsertError.getError().toString());
    byte[] expectedPayload = expectedMessage.getPayload();
    Map<String, String> expectedAttributes = expectedMessage.getAttributeMap();
    PAssert.thatSingleton(output)
        .satisfies(
            input -> {
              assertThat(input.getPayload()).isEqualTo(expectedPayload);
              assertThat(input.getAttributeMap()).isEqualTo(expectedAttributes);
              return null;
            });
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void transformConvertsBigQueryInsertErrorToPubsubMessageWithTruncatedMessage()
      throws IOException {

    GenericRecord expectedRecord = BigQueryConvertersTest.generateNestedAvroRecord();
    String errorMessage = Strings.repeat("a", 1000);
    BigQueryInsertError bigQueryInsertError = getBigQueryInsertError(expectedRecord, errorMessage);
    ErrorConverters.BigQueryInsertErrorToPubsubMessage<GenericRecord> converter =
        getConverter(expectedRecord.getSchema(), AvroCoder.of(expectedRecord.getSchema()));

    PCollection<PubsubMessage> output =
        pipeline
            .apply(Create.of(bigQueryInsertError).withCoder(BigQueryInsertErrorCoder.of()))
            .apply(converter);

    // Expecting a truncated message with a truncation indicator suffix.
    String expectedErrorMessage =
        Ascii.truncate(
            bigQueryInsertError.getError().toString(),
            /* maxLength= */ 512,
            /* truncationIndicator= */ "...");
    PubsubMessage expectedMessage = getPubsubMessage(expectedRecord, expectedErrorMessage);
    byte[] expectedPayload = expectedMessage.getPayload();
    Map<String, String> expectedAttributes = expectedMessage.getAttributeMap();
    PAssert.thatSingleton(output)
        .satisfies(
            input -> {
              assertThat(input.getPayload()).isEqualTo(expectedPayload);
              assertThat(input.getAttributeMap()).isEqualTo(expectedAttributes);
              return null;
            });
    pipeline.run();
  }

  /**
   * Generates a {@link BigQueryInsertError} with the {@link GenericRecord} and error message.
   *
   * @param record payload to be used for the test
   * @param errorMessage error message for the test
   */
  private static BigQueryInsertError getBigQueryInsertError(
      GenericRecord record, String errorMessage) {

    Row beamRow = AvroUtils.toBeamRowStrict(record, AvroUtils.toBeamSchema(record.getSchema()));
    TableRow tableRow = BigQueryUtils.toTableRow(beamRow);

    TableReference tableReference = new TableReference();

    return new BigQueryInsertError(tableRow.clone(), getInsertErrors(errorMessage), tableReference);
  }

  /**
   * Generates a {@link InsertErrors} used by {@link BigQueryInsertError}.
   *
   * @param error string to be added to {@link BigQueryInsertError}
   */
  private static InsertErrors getInsertErrors(String error) {
    InsertErrors insertErrors = new TableDataInsertAllResponse.InsertErrors();
    ErrorProto errorProto = new ErrorProto().setMessage(error);
    insertErrors.setErrors(Lists.newArrayList(errorProto));
    return insertErrors;
  }

  /**
   * Generates a {@link PubsubMessage} with the {@link GenericRecord} payload and the user provided
   * error message as an attribute.
   *
   * @param record payload for the message
   * @param errorValue errors to be added as an attribute
   */
  private static PubsubMessage getPubsubMessage(GenericRecord record, String errorValue)
      throws IOException {
    AvroCoder<GenericRecord> coder = AvroCoder.of(record.getSchema());
    String errorKey = "error";
    Map<String, String> attributeMap =
        ImmutableMap.<String, String>builder().put(errorKey, errorValue).build();
    return new PubsubMessage(CoderUtils.encodeToByteArray(coder, record), attributeMap);
  }

  /**
   * Returns a {@link ErrorConverters.BigQueryInsertErrorToPubsubMessage} used for the test.
   *
   * @param schema avro schema for the payload
   * @param coder coder to be used for encoding the payload
   */
  private static ErrorConverters.BigQueryInsertErrorToPubsubMessage<GenericRecord> getConverter(
      Schema schema, AvroCoder<GenericRecord> coder) {

    SerializableFunction<TableRow, GenericRecord> translateFunction =
        BigQueryConverters.TableRowToGenericRecordFn.of(schema);

    return ErrorConverters.BigQueryInsertErrorToPubsubMessage.<GenericRecord>newBuilder()
        .setPayloadCoder(coder)
        .setTranslateFunction(translateFunction)
        .build();
  }
}
