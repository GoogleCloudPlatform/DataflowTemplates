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
package com.google.cloud.teleport.v2.kafka.transforms;

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.coders.GenericRecordCoder;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * The {@link AvroTransform} class is a {@link PTransform} which transforms incoming Kafka Message
 * objects into {@link GenericRecord} using an Avro schema file (.avsc).
 */
public class AvroTransform
    extends PTransform<
        PCollection<KafkaRecord<byte[], byte[]>>,
        PCollection<FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>>> {
  private String messageFormat;
  private String schemaPath;

  // DLQ related parameters.
  private ErrorHandler<BadRecord, ?> errorHandler;
  private BadRecordRouter badRecordRouter;
  private static final TupleTag<FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>>
      SUCCESS_GENERIC_RECORDS = new TupleTag<>();

  private AvroTransform(String messageFormat, String schemaPath) {
    this.messageFormat = messageFormat;
    this.schemaPath = schemaPath;
    this.errorHandler = new ErrorHandler.DefaultErrorHandler<>();
    this.badRecordRouter = BadRecordRouter.THROWING_ROUTER;
  }

  private AvroTransform(
      String messageFormat,
      String schemaPath,
      ErrorHandler<BadRecord, ?> errorHandler,
      BadRecordRouter badRecordRouter) {
    this.messageFormat = messageFormat;
    this.schemaPath = schemaPath;
    this.errorHandler = errorHandler;
    this.badRecordRouter = badRecordRouter;
  }

  public static AvroTransform of(String messageFormat, String schemaPath) {
    return new AvroTransform(messageFormat, schemaPath);
  }

  public static AvroTransform of(
      String messageFormat,
      String schemaPath,
      ErrorHandler<BadRecord, ?> errorHandler,
      BadRecordRouter badRecordRouter) {
    return new AvroTransform(messageFormat, schemaPath, errorHandler, badRecordRouter);
  }

  public PCollection<FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>> expand(
      PCollection<KafkaRecord<byte[], byte[]>> kafkaRecords) {
    Schema schema = SchemaUtils.getAvroSchema(schemaPath);
    PCollection<FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>> successGenericRecords;
    PCollectionTuple genericRecords;
    genericRecords =
        kafkaRecords.apply(
            "ConvertKafkaRecordsToGenericRecordsWrappedinFailsafeElement",
            ParDo.of(
                    new KafkaRecordToGenericRecordFailsafeElementFn(
                        schema, this.messageFormat, this.badRecordRouter, SUCCESS_GENERIC_RECORDS))
                .withOutputTags(
                    SUCCESS_GENERIC_RECORDS, TupleTagList.of(BadRecordRouter.BAD_RECORD_TAG)));
    successGenericRecords =
        genericRecords
            .get(SUCCESS_GENERIC_RECORDS)
            .setCoder(
                FailsafeElementCoder.of(
                    KafkaRecordCoder.of(NullableCoder.of(ByteArrayCoder.of()), ByteArrayCoder.of()),
                    GenericRecordCoder.of()));

    // Get the failed elements and add them to the errorHandler collection.
    PCollection<BadRecord> failedGenericRecords =
        genericRecords.get(BadRecordRouter.BAD_RECORD_TAG);
    errorHandler.addErrorCollection(
        failedGenericRecords.setCoder(BadRecord.getCoder(genericRecords.getPipeline())));
    return successGenericRecords;
  }
}
