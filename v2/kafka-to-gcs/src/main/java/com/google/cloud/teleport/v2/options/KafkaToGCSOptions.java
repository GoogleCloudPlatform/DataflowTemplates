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
package com.google.cloud.teleport.v2.options;

import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.transforms.WriteToGCSAvro;
import com.google.cloud.teleport.v2.transforms.WriteToGCSParquet;
import com.google.cloud.teleport.v2.transforms.WriteToGCSText;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link KafkaToGCSOptions} interface provides the custom execution options passed by the
 * executor at the command-line.
 */
public interface KafkaToGCSOptions
    extends PipelineOptions,
        WriteToGCSText.WriteToGCSTextOptions,
        WriteToGCSParquet.WriteToGCSParquetOptions,
        WriteToGCSAvro.WriteToGCSAvroOptions {

  @TemplateParameter.Text(
      order = 1,
      optional = false,
      regexes = {"[,:a-zA-Z0-9._-]+"},
      description = "Kafka Bootstrap Server list",
      helpText = "Kafka Bootstrap Server list, separated by commas.",
      example = "localhost:9092,127.0.0.1:9093")
  @Validation.Required
  String getBootstrapServers();

  void setBootstrapServers(String bootstrapServers);

  @TemplateParameter.Text(
      order = 2,
      optional = false,
      regexes = {"[a-zA-Z0-9._-,]+"},
      description = "Kafka topic(s) to read the input from",
      helpText = "Kafka topic(s) to read the input from.",
      example = "topic1,topic2")
  @Validation.Required
  String getInputTopics();

  void setInputTopics(String inputTopics);

  @TemplateParameter.Enum(
      order = 3,
      enumOptions = {"TEXT, AVRO, PARQUET"},
      optional = false,
      description = "File format of the desired output files. (TEXT, AVRO or PARQUET)",
      helpText =
          "The file format of the desired output files. Can be TEXT, AVRO or PARQUET. Defaults to TEXT")
  @Default.String("TEXT")
  String getOutputFileFormat();

  void setOutputFileFormat(String outputFileFormat);

  @TemplateParameter.Duration(
      order = 4,
      optional = true,
      description = "Window duration",
      helpText =
          "The window duration/size in which data will be written to Cloud Storage. Allowed formats are: Ns (for "
              + "seconds, example: 5s), Nm (for minutes, example: 12m), Nh (for hours, example: 2h).",
      example = "5m")
  @Default.String("5m")
  String getWindowDuration();

  void setWindowDuration(String windowDuration);
}
