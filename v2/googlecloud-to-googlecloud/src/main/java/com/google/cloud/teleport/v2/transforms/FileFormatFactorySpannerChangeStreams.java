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
import com.google.cloud.teleport.v2.options.SpannerChangeStreamsToGcsOptions;
import com.google.cloud.teleport.v2.utils.WriteToGCSUtility.FileFormat;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link FileFormatFactorySpannerChangeStreams} class is a {@link PTransform} that takes in
 * {@link PCollection} of DataChangeRecords. The transform writes these records to GCS file(s) in
 * user specified format.
 */
@AutoValue
public abstract class FileFormatFactorySpannerChangeStreams
    extends PTransform<PCollection<DataChangeRecord>, POutput> {

  /** Logger for class. */
  private static final Logger LOG =
      LoggerFactory.getLogger(FileFormatFactorySpannerChangeStreams.class);

  public static WriteToGcsBuilder newBuilder() {
    return new AutoValue_FileFormatFactorySpannerChangeStreams.Builder();
  }

  public abstract SpannerChangeStreamsToGcsOptions options();

  public abstract FileFormat outputFileFormat();

  @Override
  public POutput expand(PCollection<DataChangeRecord> records) {
    POutput output = null;

    final String errorMessage =
        "Invalid output format:" + outputFileFormat() + ". Supported output formats: TEXT, AVRO";

    /*
     * Calls appropriate class Builder to perform PTransform based on user provided File Format.
     */
    switch (outputFileFormat()) {
      case AVRO:
        output =
            records.apply(
                "Write Avro File(s)",
                WriteDataChangeRecordsToGcsAvro.newBuilder()
                    .withGcsOutputDirectory(options().getGcsOutputDirectory())
                    .withOutputFilenamePrefix(options().getOutputFilenamePrefix())
                    .setNumShards(options().getNumShards())
                    .withTempLocation(options().getTempLocation())
                    .build());
        break;
      case TEXT:
        output =
            records.apply(
                "Write Text File(s)",
                WriteDataChangeRecordsToGcsText.newBuilder()
                    .withGcsOutputDirectory(options().getGcsOutputDirectory())
                    .withOutputFilenamePrefix(options().getOutputFilenamePrefix())
                    .setNumShards(options().getNumShards())
                    .withTempLocation(options().getTempLocation())
                    .build());
        break;

      default:
        LOG.info(errorMessage);
        throw new IllegalArgumentException(errorMessage);
    }
    return output;
  }

  /** Builder for {@link FileFormatFactorySpannerChangeStreams}. */
  @AutoValue.Builder
  public abstract static class WriteToGcsBuilder {

    public abstract WriteToGcsBuilder setOptions(SpannerChangeStreamsToGcsOptions options);

    public abstract WriteToGcsBuilder setOutputFileFormat(FileFormat outputFileFormat);

    abstract SpannerChangeStreamsToGcsOptions options();

    abstract FileFormatFactorySpannerChangeStreams autoBuild();

    public FileFormatFactorySpannerChangeStreams build() {
      setOutputFileFormat(options().getOutputFileFormat());
      return autoBuild();
    }
  }
}
