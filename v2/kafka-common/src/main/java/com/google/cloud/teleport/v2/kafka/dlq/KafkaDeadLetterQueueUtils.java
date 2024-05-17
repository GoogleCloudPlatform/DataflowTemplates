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
package com.google.cloud.teleport.v2.kafka.dlq;

import java.io.IOException;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;

/**
 * Utility methods for working with dead letter queues and {@link BadRecord} objects. These methods
 * are designed to help retrieve the original element or a JSON representation of the original
 * element that caused an error during processing.
 *
 * <p>To successfully extract the original element, a coder must be defined. This class is primarily
 * designed to operate on {@link BadRecord} objects and expects the encoded object is a {@link
 * KafkaRecord}.
 */
public class KafkaDeadLetterQueueUtils {
  /* KafkaRecord coder to decode the bytes from the BadRecord */
  private static final KafkaRecordCoder<byte[], byte[]> kafkaRecordCoder =
      KafkaRecordCoder.of(
          NullableCoder.of(ByteArrayCoder.of()), NullableCoder.of(ByteArrayCoder.of()));

  public static class GetPayLoadStringFromBadRecord extends DoFn<BadRecord, KV<String, String>> {
    @ProcessElement
    public void processElement(
        @Element BadRecord badRecord, OutputReceiver<KV<String, String>> receiver) {
      String record = badRecord.getRecord().getHumanReadableJsonRecord();
      receiver.output(KV.of(badRecord.getFailure().getException(), record));
    }
  }

  public static class GetPayLoadFromBadRecord extends DoFn<BadRecord, KV<byte[], byte[]>> {
    @ProcessElement
    public void processElement(
        @Element BadRecord badRecord, OutputReceiver<KV<byte[], byte[]>> receiver)
        throws IOException {
      byte[] encodedRecord = badRecord.getRecord().getEncodedRecord();
      KafkaRecord<byte[], byte[]> record =
          CoderUtils.decodeFromByteArray(kafkaRecordCoder, encodedRecord);
      receiver.output(record.getKV());
    }
  }
}
