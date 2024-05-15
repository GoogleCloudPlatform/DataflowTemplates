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
package com.google.cloud.teleport.v2.dlq;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.io.WindowedFilenamePolicy;
import com.google.cloud.teleport.v2.utils.DurationUtils;
import com.google.cloud.teleport.v2.utils.WriteToGCSUtility;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;

@AutoValue
public abstract class GcsDeadLetterQueue extends PTransform<PCollection<BadRecord>, POutput> {
  private final int NUM_SHARDS = 20;
  private final String OUTPUT_FILENAME_PREFIX = "error";
  private final String OUTPUT_FILE_EXTENSION = ".json";

  public abstract String dlqOutputDirectory();

  public abstract String windowDuration();

  @AutoValue.Builder
  public abstract static class GcsDeadLetterQueueBuilder {
    public abstract GcsDeadLetterQueueBuilder setDlqOutputDirectory(String value);

    public abstract GcsDeadLetterQueueBuilder setWindowDuration(String value);

    abstract GcsDeadLetterQueue autoBuild();

    public GcsDeadLetterQueue build() {
      return autoBuild();
    }
  }

  @Override
  public POutput expand(PCollection<BadRecord> input) {
    return input
        .apply(Window.into(FixedWindows.of(DurationUtils.parseDuration(windowDuration()))))
        .apply(ParDo.of(new DlqUtils.getPayLoadStringFromBadRecord()))
        .apply(
            ParDo.of(
                new DoFn<KV<String, String>, String>() {
                  @ProcessElement
                  public void processElement(
                      @Element KV<String, String> kv, OutputReceiver<String> o) {
                    o.output(kv.getValue());
                  }
                }))
        .apply(
            TextIO.write()
                .withWindowedWrites()
                .withNumShards(NUM_SHARDS)
                .to(
                    WindowedFilenamePolicy.writeWindowedFiles()
                        .withOutputFilenamePrefix(OUTPUT_FILENAME_PREFIX)
                        .withSuffix(OUTPUT_FILE_EXTENSION)
                        .withShardTemplate(WriteToGCSUtility.SHARD_TEMPLATE)
                        .withOutputDirectory(dlqOutputDirectory()))
                .withTempDirectory(
                    FileBasedSink.convertToFileResourceIfPossible(dlqOutputDirectory() + "/temp")));
  }

  public static GcsDeadLetterQueueBuilder newBuilder() {
    return new AutoValue_GcsDeadLetterQueue.Builder();
  }
}
