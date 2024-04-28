/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.templates.common;

import com.google.cloud.teleport.metadata.TemplateParameter;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

/** Options for beam TextIO. */
public class TextConverters {

  /** Options for reading data from a Filesystem. */
  public interface FilesystemReadOptions extends PipelineOptions {

    @TemplateParameter.Text(
        order = 1,
        regexes = {"^gs:\\/\\/[^\\n\\r]+$"},
        description = "Input file(s) in Cloud Storage",
        helpText = "The input file pattern Dataflow reads from. Ex: gs://your-bucket/.../*.json")
    ValueProvider<String> getTextReadPattern();

    void setTextReadPattern(ValueProvider<String> textReadPattern);
  }

  /** Options for writing data to a Filesystem. */
  public interface FilesystemWriteOptions extends PipelineOptions {
    @TemplateParameter.GcsWriteFolder(
        order = 2,
        groupName = "Target",
        description = "Output file directory in Cloud Storage",
        helpText = "The path and filename prefix for writing output files.",
        example = "gs://your-bucket/your-path")
    ValueProvider<String> getTextWritePrefix();

    void setTextWritePrefix(ValueProvider<String> textWritePrefix);
  }

  /** Options for writing into a filesystem using sharded files in windows. */
  public interface FilesystemWindowedWriteOptions extends PipelineOptions {

    @TemplateParameter.GcsWriteFolder(
        order = 1,
        groupName = "Target",
        description = "Output file directory in Cloud Storage",
        helpText =
            "The path and filename prefix for writing output files. Must end with a slash. DateTime formatting is used to parse directory path for date & time formatters.",
        example = "gs://your-bucket/your-path")
    @Validation.Required
    ValueProvider<String> getOutputDirectory();

    void setOutputDirectory(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 2,
        groupName = "Target",
        description = "Output filename prefix of the files to write",
        helpText = "The prefix to place on each windowed file.",
        example = "output-")
    @Default.String("output")
    ValueProvider<String> getOutputFilenamePrefix();

    void setOutputFilenamePrefix(ValueProvider<String> value);

    @Description("The suffix of the files to write.")
    @Default.String("")
    ValueProvider<String> getOutputFilenameSuffix();

    void setOutputFilenameSuffix(ValueProvider<String> value);

    @Description(
        "The shard template of the output file. Specified as repeating sequences "
            + "of the letters 'S' or 'N' (example: SSS-NNN). These are replaced with the "
            + "shard number, or number of shards respectively")
    @Default.String("W-P-SS-of-NN")
    ValueProvider<String> getOutputShardTemplate();

    void setOutputShardTemplate(ValueProvider<String> value);

    @TemplateParameter.Integer(
        order = 4,
        groupName = "Target",
        optional = true,
        description = "Maximum output shards",
        helpText =
            "The maximum number of output shards produced when writing. A higher number of "
                + "shards means higher throughput for writing to Cloud Storage, but potentially higher "
                + "data aggregation cost across shards when processing output Cloud Storage files.")
    @Default.Integer(1)
    Integer getNumShards();

    void setNumShards(Integer value);

    @TemplateParameter.Duration(
        order = 5,
        groupName = "Target",
        optional = true,
        description = "Window duration",
        helpText =
            "The window duration/size in which data will be written to Cloud Storage. Allowed formats are: Ns (for "
                + "seconds, example: 5s), Nm (for minutes, example: 12m), Nh (for hours, example: 2h).",
        example = "5m")
    @Default.String("1m")
    String getWindowDuration();

    void setWindowDuration(String value);
  }
}
