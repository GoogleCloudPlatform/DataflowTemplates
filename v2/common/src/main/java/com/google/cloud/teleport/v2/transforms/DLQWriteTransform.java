/*
 * Copyright (C) 2019 Google LLC
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
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** Transforms to write error messages in to DLQ. */
public class DLQWriteTransform {

  /**
   * The {@link DLQWriteTransform} class is a {@link PTransform} that takes a PCollection of Strings
   * as input and writes them to DLQ directory.
   */
  @AutoValue
  public abstract static class WriteDLQ extends PTransform<PCollection<String>, PDone> {

    public static Builder newBuilder() {
      return new AutoValue_DLQWriteTransform_WriteDLQ.Builder().setIncludePaneInfo(false);
    }

    public abstract String dlqDirectory();

    public abstract String tmpDirectory();

    public abstract boolean includePaneInfo();

    @Override
    public PDone expand(PCollection<String> input) {
      return input
          .apply(
              ParDo.of(
                  new DoFn<String, String>() {
                    @ProcessElement
                    public void process(ProcessContext context) {
                      Instant now = Instant.now();
                      context.outputWithTimestamp(context.element(), now);
                    }
                  }))
          .apply(
              "Creating 1m Window",
              Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))
                  .triggering(
                      Repeatedly.forever(
                          AfterProcessingTime.pastFirstElementInPane()
                              .plusDelayOf(Duration.standardMinutes(1))))
                  .withAllowedLateness(Duration.ZERO)
                  .discardingFiredPanes())
          .apply(
              "DLQ: Write File(s)",
              TextIO.write()
                  .withWindowedWrites()
                  .withNumShards(20)
                  .to(
                      new WindowedFilenamePolicy(
                          dlqDirectory(), "error", getShardTemplate(), ".json"))
                  .withTempDirectory(
                      FileBasedSink.convertToFileResourceIfPossible(tmpDirectory())));
    }

    private String getShardTemplate() {
      String paneStr = includePaneInfo() ? "-P" : "";
      return paneStr + "-SSSSS-of-NNNNN";
    }

    /** Builder for {@link WriteDLQ}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setDlqDirectory(String dlqDirectory);

      public abstract String dlqDirectory();

      public Builder withDlqDirectory(String dlqDirectory) {
        return setDlqDirectory(dlqDirectory);
      }

      public abstract Builder setTmpDirectory(String tmpDirectory);

      public abstract String tmpDirectory();

      public Builder withTmpDirectory(String tmpDirectory) {
        return setTmpDirectory(tmpDirectory);
      }

      public abstract Builder setIncludePaneInfo(boolean value);

      public abstract boolean includePaneInfo();

      public abstract WriteDLQ build();
    }
  }
}
