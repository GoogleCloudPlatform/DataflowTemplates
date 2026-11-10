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

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.io.WindowedFilenamePolicy;
import com.google.cloud.teleport.v2.utils.DurationUtils;
import com.google.cloud.teleport.v2.utils.WriteToGCSUtility;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.kafka.common.serialization.StringDeserializer;

@AutoValue
public abstract class JsonWriteTransform
    extends PTransform<PCollection<KafkaRecord<byte[], byte[]>>, POutput> {

  public abstract String outputDirectory();

  public abstract Integer numShards();

  public abstract String outputFilenamePrefix();

  public abstract String windowDuration();

  public abstract String tempDirectory();

  public static JsonWriteTransformBuilder newBuilder() {

    return new AutoValue_JsonWriteTransform.Builder();
  }

  @AutoValue.Builder
  public abstract static class JsonWriteTransformBuilder {
    abstract JsonWriteTransform autoBuild();

    public abstract JsonWriteTransformBuilder setNumShards(Integer numShards);

    public abstract JsonWriteTransformBuilder setOutputDirectory(String outputDirectory);

    public abstract JsonWriteTransformBuilder setOutputFilenamePrefix(String outputFilenamePrefix);

    public abstract JsonWriteTransformBuilder setWindowDuration(String windowDuration);

    public abstract JsonWriteTransformBuilder setTempDirectory(String tempDirectory);

    public JsonWriteTransform build() {
      return autoBuild();
    }
  }

  public static class ConvertBytesToString extends DoFn<KafkaRecord<byte[], byte[]>, String> {
    @ProcessElement
    public void processElement(
        @Element KafkaRecord<byte[], byte[]> kafkaRecord, OutputReceiver<String> o) {
      // TODO: Add DLQ support.
      StringDeserializer deserializer = new StringDeserializer();
      o.output(deserializer.deserialize(kafkaRecord.getTopic(), kafkaRecord.getKV().getValue()));
    }
  }

  public POutput expand(PCollection<KafkaRecord<byte[], byte[]>> records) {
    return records
        .apply(ParDo.of(new ConvertBytesToString()))
        // TextIO needs windowing for unbounded PCollections.
        .apply(Window.into(FixedWindows.of(DurationUtils.parseDuration(windowDuration()))))
        .apply(
            "Write using TextIO",
            TextIO.write()
                .withWindowedWrites()
                .withNumShards(numShards())
                .to(
                    WindowedFilenamePolicy.writeWindowedFiles()
                        .withOutputDirectory(outputDirectory())
                        .withOutputFilenamePrefix(outputFilenamePrefix())
                        .withSuffix(".json")
                        .withShardTemplate(WriteToGCSUtility.SHARD_TEMPLATE))
                .withTempDirectory(FileBasedSink.convertToFileResourceIfPossible(tempDirectory())));
  }
}
