/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.spanner;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * Text files to Cloud Spanner Import pipeline. INTERNAL USE ONLY! This pipeline is for internal
 * usage for the time being. TODO: Please remove this comment when this is ready for public launch.
 */
public class TextImportPipeline {

  /** Options for {@link TextImportPipeline}. */
  public interface Options extends PipelineOptions {

    @Description("Instance ID to write to Spanner")
    ValueProvider<String> getInstanceId();

    void setInstanceId(ValueProvider<String> value);

    @Description("Database ID to write to Spanner")
    ValueProvider<String> getDatabaseId();

    void setDatabaseId(ValueProvider<String> value);

    @Description("Spanner host")
    @Default.String("https://batch-spanner.googleapis.com")
    ValueProvider<String> getSpannerHost();

    void setSpannerHost(ValueProvider<String> value);

    @Description("Text Import Manifest file, storing a json-encoded {@link importManifest} object")
    ValueProvider<String> getImportManifest();

    void setImportManifest(ValueProvider<String> value);

    @Description("Column delimiter of the data files")
    @Default.Character(',')
    ValueProvider<Character> getColumnDelimiter();

    void setColumnDelimiter(ValueProvider<Character> value);

    @Description(
        "Field qualifier used by the source file. Field qualifier should be used when character"
            + " needs to be escaped.")
    ValueProvider<Character> getFieldQualifier();

    void setFieldQualifier(ValueProvider<Character> value);

    @Description("If true, the lines has trailing delimiters.")
    @Default.Boolean(true)
    ValueProvider<Boolean> getTrailingDelimiter();

    void setTrailingDelimiter(ValueProvider<Boolean> value);

    @Description("The escape character.")
    @Default.Character('\\')
    ValueProvider<Character> getEscape();

    void setEscape(ValueProvider<Character> value);

    @Description("The string that represents the NULL value.")
    @Default.String("\\N")
    ValueProvider<String> getNullString();

    void setNullString(ValueProvider<String> value);

    @Description("If true, wait for job finish")
    @Default.Boolean(true)
    boolean getWaitUntilFinish();

    void setWaitUntilFinish(boolean value);
  }

  public static void main(String[] args) {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    Pipeline p = Pipeline.create(options);

    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withHost(options.getSpannerHost())
            .withInstanceId(options.getInstanceId())
            .withDatabaseId(options.getDatabaseId());

    p.apply(new TextImportTransform(spannerConfig, options.getImportManifest()));

    PipelineResult result = p.run();
    if (options.getWaitUntilFinish()
        &&
        /* Only if template location is null, there is a dataflow job to wait for. Otherwise it's
         * template generation, which doesn't start a dataflow job.
         */
        options.as(DataflowPipelineOptions.class).getTemplateLocation() == null) {
      result.waitUntilFinish();
    }
  }
}
