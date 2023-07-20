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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.teleport.v2.io.WindowedFilenamePolicy;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.model.BigtableSchemaFormat;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.model.DestinationInfo;
import com.google.cloud.teleport.v2.utils.WriteToGCSUtility;
import com.google.gson.Gson;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link WriteChangeStreamMutationsToGcsText} class is a {@link PTransform} that takes in
 * {@link PCollection} of Bigtable change stream mutations. The transform converts and writes these
 * records to GCS in JSON text file format.
 */
@AutoValue
public abstract class WriteChangeStreamMutationsToGcsText
    extends PTransform<PCollection<ChangeStreamMutation>, PDone> {
  private static final Gson gson = new Gson();

  private static final AtomicLong counter = new AtomicLong(0);

  private static final String workerId = UUID.randomUUID().toString();

  @VisibleForTesting protected static final String DEFAULT_OUTPUT_FILE_PREFIX = "output";

  /* Logger for class. */
  private static final Logger LOG =
      LoggerFactory.getLogger(WriteChangeStreamMutationsToGcsText.class);

  public static WriteToGcsBuilder newBuilder() {
    return new com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs
        .AutoValue_WriteChangeStreamMutationsToGcsText.Builder();
  }

  public abstract String gcsOutputDirectory();

  public abstract String outputFilenamePrefix();

  public abstract String tempLocation();

  public abstract Integer numShards();

  public abstract BigtableUtils bigtableUtils();

  public abstract BigtableSchemaFormat schemaOutputFormat();

  public abstract DestinationInfo destinationInfo();

  @Override
  public PDone expand(PCollection<ChangeStreamMutation> mutations) {
    PCollection<com.google.cloud.teleport.bigtable.ChangelogEntry> changelogEntry =
        mutations.apply(
            "ChangeStreamMutation to ChangelogEntry",
            FlatMapElements.via(
                new BigtableChangeStreamMutationToChangelogEntryFn(bigtableUtils())));

    return changelogEntry
        .apply(MapElements.via(new WriteChangelogEntryAsJson(destinationInfo())))
        .apply(
            "Writing as Text",
            TextIO.write()
                .to(
                    WindowedFilenamePolicy.writeWindowedFiles()
                        .withOutputDirectory(gcsOutputDirectory())
                        .withOutputFilenamePrefix(outputFilenamePrefix())
                        .withShardTemplate(WriteToGCSUtility.SHARD_TEMPLATE)
                        .withSuffix(
                            WriteToGCSUtility.FILE_SUFFIX_MAP.get(
                                WriteToGCSUtility.FileFormat.TEXT)))
                .withTempDirectory(
                    FileBasedSink.convertToFileResourceIfPossible(tempLocation())
                        .getCurrentDirectory())
                .withWindowedWrites()
                .withNumShards(numShards()));
  }

  /** Builder for {@link WriteChangeStreamMutationsToGcsText}. */
  @AutoValue.Builder
  public abstract static class WriteToGcsBuilder {

    abstract WriteToGcsBuilder setGcsOutputDirectory(String gcsOutputDirectory);

    abstract String gcsOutputDirectory();

    abstract WriteToGcsBuilder setTempLocation(String tempLocation);

    abstract String tempLocation();

    abstract WriteToGcsBuilder setOutputFilenamePrefix(String outputFilenamePrefix);

    abstract WriteToGcsBuilder setNumShards(Integer numShards);

    abstract WriteToGcsBuilder setSchemaOutputFormat(BigtableSchemaFormat schema);

    abstract WriteToGcsBuilder setDestinationInfo(DestinationInfo destinationInfo);

    abstract WriteChangeStreamMutationsToGcsText autoBuild();

    abstract WriteToGcsBuilder setBigtableUtils(BigtableUtils bigtableUtils);

    public WriteToGcsBuilder withBigtableUtils(BigtableUtils bigtableUtils) {
      return setBigtableUtils(bigtableUtils);
    }

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

    public WriteToGcsBuilder withOutputFilenamePrefix(String outputFilenamePrefix) {
      if (outputFilenamePrefix == null) {
        LOG.info("Defaulting output filename prefix to: {}", DEFAULT_OUTPUT_FILE_PREFIX);
        outputFilenamePrefix = DEFAULT_OUTPUT_FILE_PREFIX;
      }
      return setOutputFilenamePrefix(outputFilenamePrefix);
    }

    public WriteToGcsBuilder withSchemaOutputFormat(BigtableSchemaFormat schema) {
      return setSchemaOutputFormat(schema);
    }

    public WriteToGcsBuilder withDestinationInfo(DestinationInfo destinationInfo) {
      return setDestinationInfo(destinationInfo);
    }

    public WriteChangeStreamMutationsToGcsText build() {
      checkNotNull(gcsOutputDirectory(), "Provide output directory to write to.");
      checkNotNull(tempLocation(), "Temporary directory needs to be provided.");
      return autoBuild();
    }
  }
}
