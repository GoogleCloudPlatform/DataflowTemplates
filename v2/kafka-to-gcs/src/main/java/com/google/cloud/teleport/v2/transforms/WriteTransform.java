/*
 * Copyright (C) 2021 Google LLC
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

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.kafka.values.KafkaTemplateParameters.MessageFormatConstants;
import com.google.cloud.teleport.v2.templates.KafkaToGcsFlex;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;

@AutoValue
public abstract class WriteTransform
    extends PTransform<PCollection<KafkaRecord<byte[], byte[]>>, POutput> {

  public static WriteTransformBuilder newBuilder() {
    return new AutoValue_WriteTransform.Builder();
  }

  public abstract KafkaToGcsFlex.KafkaToGcsOptions options();

  @Override
  public POutput expand(PCollection<KafkaRecord<byte[], byte[]>> kafkaRecord) {
    POutput pOutput = null;
    String outputFileFormat = options().getMessageFormat();

    if (outputFileFormat.equals(MessageFormatConstants.JSON)) {
      pOutput =
          kafkaRecord.apply(
              JsonWriteTransform.newBuilder()
                  .setOutputFilenamePrefix(options().getOutputFilenamePrefix())
                  .setNumShards(options().getNumShards())
                  .setOutputDirectory(options().getOutputDirectory())
                  .setWindowDuration(options().getWindowDuration())
                  .setTempDirectory(options().getTempLocation())
                  .build());
    } else if (outputFileFormat.equals(MessageFormatConstants.AVRO_CONFLUENT_WIRE_FORMAT)) {
      pOutput =
          kafkaRecord.apply(
              AvroWriteTransform.newBuilder()
                  .setOutputDirectory(options().getOutputDirectory())
                  .setOutputFilenamePrefix(options().getOutputFilenamePrefix())
                  .setNumShards(options().getNumShards())
                  .setMessageFormat(options().getMessageFormat())
                  .setSchemaRegistryURL(options().getSchemaRegistryConnectionUrl())
                  .setSchemaPath(options().getConfluentAvroSchemaPath())
                  .setWindowDuration(options().getWindowDuration())
                  .build());
    } else {
      throw new UnsupportedOperationException(
          String.format("Message format: %s is not supported", options().getMessageFormat()));
    }
    return pOutput;
  }

  @AutoValue.Builder
  public abstract static class WriteTransformBuilder {
    public abstract WriteTransformBuilder setOptions(KafkaToGcsFlex.KafkaToGcsOptions options);

    abstract KafkaToGcsFlex.KafkaToGcsOptions options();

    abstract WriteTransform autoBuild();

    public WriteTransform build() {
      return autoBuild();
    }
  }
}
