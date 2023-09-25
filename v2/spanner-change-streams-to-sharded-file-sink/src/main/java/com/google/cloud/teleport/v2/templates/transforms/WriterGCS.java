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
package com.google.cloud.teleport.v2.templates.transforms;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.templates.common.TrimmedShardedDataChangeRecord;
import com.google.gson.Gson;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Writes the files to GCS per shard. */
@AutoValue
public abstract class WriterGCS
    extends PTransform<
        PCollection<TrimmedShardedDataChangeRecord>, PCollection<KV<String, String>>> {
  private static final Logger LOG = LoggerFactory.getLogger(WriterGCS.class);

  public static WriteToGcsBuilder newBuilder() {
    return new AutoValue_WriterGCS.Builder();
  }

  public abstract String gcsOutputDirectory();

  public abstract String tempLocation();

  /*
  Takes the change records and writes to GCS
  The destination folders are created per shard
  Each interval will only have one file created
  for that shard (when there is data for that interval).
  Hence the numShards is set to 1.
  */
  @Override
  public PCollection<KV<String, String>> expand(
      PCollection<TrimmedShardedDataChangeRecord> dataChangeRecords) {
    return dataChangeRecords
        .apply(
            "Write rows to output writeDynamic",
            FileIO.<String, TrimmedShardedDataChangeRecord>writeDynamic()
                .by((row) -> row.getShard())
                .withDestinationCoder(StringUtf8Coder.of())
                .via(Contextful.fn(new DataChangeRecordToJsonTextFn()), TextIO.sink())
                .withNaming(PartitionedFileNaming::new)
                .to(gcsOutputDirectory())
                .withTempDirectory(
                    FileBasedSink.convertToFileResourceIfPossible(tempLocation())
                        .getCurrentDirectory()
                        .toString())
                .withNumShards(1))
        .getPerDestinationOutputFilenames();
  }

  @AutoValue.Builder
  public abstract static class WriteToGcsBuilder {
    abstract WriteToGcsBuilder setGcsOutputDirectory(String gcsOutputDirectory);

    abstract String gcsOutputDirectory();

    abstract WriteToGcsBuilder setTempLocation(String tempLocation);

    abstract String tempLocation();

    abstract WriterGCS autoBuild();

    public WriteToGcsBuilder withGcsOutputDirectory(String gcsOutputDirectory) {
      checkArgument(
          gcsOutputDirectory != null,
          "withGcsOutputDirectory(gcsOutputDirectory) called with null input.");
      return setGcsOutputDirectory(gcsOutputDirectory);
    }

    public WriteToGcsBuilder withTempLocation(String tempLocation) {
      checkArgument(tempLocation != null, "withTempLocation(tempLocation) called with null input.");
      return setTempLocation(tempLocation);
    }

    public WriterGCS build() {
      checkNotNull(gcsOutputDirectory(), "Provide output directory to write to.");
      checkNotNull(tempLocation(), "Temporary directory needs to be provided.");
      return autoBuild();
    }
  }

  class PartitionedFileNaming implements FileIO.Write.FileNaming {
    String partitionValue;

    public PartitionedFileNaming(String partitionValue) {
      this.partitionValue = partitionValue;
    }

    @Override
    public String getFilename(
        BoundedWindow window,
        PaneInfo pane,
        int numShards,
        int shardIndex,
        Compression compression) {
      String windowStr = "";
      if (window instanceof IntervalWindow) {
        IntervalWindow iw = (IntervalWindow) window;
        windowStr = String.format("%s-%s", iw.start().toString(), iw.end().toString());
      } else {
        windowStr = window.maxTimestamp().toString();
      }
      return String.format(
          "%s/%s-pane-%s-last-%s-of-%s.txt",
          this.partitionValue, windowStr, pane.getIndex(), shardIndex, numShards);
    }
  }

  static class DataChangeRecordToJsonTextFn
      extends SimpleFunction<TrimmedShardedDataChangeRecord, String> {
    private static Gson gson = new Gson();

    @Override
    public String apply(TrimmedShardedDataChangeRecord record) {
      return gson.toJson(record, TrimmedShardedDataChangeRecord.class);
    }
  }
}
