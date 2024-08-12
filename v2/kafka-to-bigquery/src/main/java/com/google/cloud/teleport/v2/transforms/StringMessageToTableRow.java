/*
 * Copyright (C) 2024 Google LLC
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

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.templates.KafkaToBigQueryFlex;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.FailsafeJsonToTableRow;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

/**
 * The {@link StringMessageToTableRow} class is a {@link PTransform} which transforms incoming Kafka
 * Message objects into {@link TableRow} objects for insertion into BigQuery while applying a UDF to
 * the input. The executions of the UDF and transformation to {@link TableRow} objects is done in a
 * fail-safe way by wrapping the element with it's original payload inside the {@link
 * FailsafeElement} class. The {@link StringMessageToTableRow} transform will output a {@link
 * PCollectionTuple} which contains all output and dead-letter {@link PCollection}.
 *
 * <p>The {@link PCollectionTuple} output will contain the following {@link PCollection}:
 *
 * <ul>
 *   <li>{@link KafkaToBigQuery#TRANSFORM_OUT} - Contains all records successfully converted from
 *       JSON to {@link TableRow} objects.
 *   <li>{@link KafkaToBigQuery#TRANSFORM_DEADLETTER_OUT} - Contains all {@link FailsafeElement}
 *       records which couldn't be converted to table rows.
 * </ul>
 */
public class StringMessageToTableRow
    extends PTransform<PCollection<KafkaRecord<String, String>>, PCollectionTuple> {

  @Override
  public PCollectionTuple expand(PCollection<KafkaRecord<String, String>> input) {

    PCollectionTuple jsonToTableRowOut =
        input
            // Map the incoming messages into FailsafeElements so we can recover from failures
            // across multiple transforms.
            .apply("MapToRecord", ParDo.of(new StringMessageToFailsafeElementFn()))
            .setCoder(
                FailsafeElementCoder.of(
                    NullableCoder.of(
                        KafkaRecordCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())),
                    NullableCoder.of(StringUtf8Coder.of())))
            .apply(
                "JsonToTableRow",
                FailsafeJsonToTableRow.<KafkaRecord<String, String>>newBuilder()
                    .setSuccessTag(KafkaToBigQueryFlex.TRANSFORM_OUT)
                    .setFailureTag(KafkaToBigQueryFlex.TRANSFORM_DEADLETTER_OUT)
                    .build());

    PCollection<FailsafeElement<KafkaRecord<String, String>, String>> badRecords =
        jsonToTableRowOut
            .get(KafkaToBigQueryFlex.TRANSFORM_DEADLETTER_OUT)
            .setCoder(
                FailsafeElementCoder.of(
                    NullableCoder.of(
                        KafkaRecordCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())),
                    NullableCoder.of(StringUtf8Coder.of())));

    // Re-wrap the PCollections so we can return a single PCollectionTuple
    return PCollectionTuple.of(
            KafkaToBigQueryFlex.TRANSFORM_OUT,
            jsonToTableRowOut.get(KafkaToBigQueryFlex.TRANSFORM_OUT))
        .and(KafkaToBigQueryFlex.TRANSFORM_DEADLETTER_OUT, badRecords);
  }

  /**
   * The {@link StringMessageToFailsafeElementFn} wraps an Kafka Message with the {@link
   * FailsafeElement} class so errors can be recovered from and the original message can be output
   * to an error records table.
   */
  static class StringMessageToFailsafeElementFn
      extends DoFn<
          KafkaRecord<String, String>, FailsafeElement<KafkaRecord<String, String>, String>> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      KafkaRecord<String, String> message = context.element();
      assert message != null;
      context.output(FailsafeElement.of(message, message.getKV().getValue()));
    }
  }
}
