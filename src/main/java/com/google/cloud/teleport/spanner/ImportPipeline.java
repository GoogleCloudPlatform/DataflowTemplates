/*
 * Copyright (C) 2018 Google Inc.
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

import org.apache.beam.sdk.Pipeline;
import com.google.cloud.teleport.spanner.connector.spanner.SpannerConfig;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;

/** Avro to Cloud Spanner Import pipeline. */
public class ImportPipeline {

  /** Options for {@link ImportPipeline}. */
  public interface Options extends PipelineOptions {

    @Description("Instance ID to write to Spanner")
    ValueProvider<String> getInstanceId();

    void setInstanceId(ValueProvider<String> value);

    @Description("Database ID to write to Spanner")
    ValueProvider<String> getDatabaseId();

    void setDatabaseId(ValueProvider<String> value);

    @Description("Input directory")
    ValueProvider<String> getInputDir();

    void setInputDir(ValueProvider<String> value);

    @Description("Spanner host")
    @Default.String("https://batch-spanner.googleapis.com")
    ValueProvider<String> getSpannerHost();

    void setSpannerHost(ValueProvider<String> value);

    @Description("If true wait until indexes are finished. Useful for testing.")
    @Default.Boolean(false)
    ValueProvider<Boolean> getWaitForIndexes();

    void setWaitForIndexes(ValueProvider<Boolean> value);
  }

  public static void main(String[] args) {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    Pipeline p = Pipeline.create(options);

    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withHost(options.getSpannerHost())
            .withInstanceId(options.getInstanceId())
            .withDatabaseId(options.getDatabaseId());

    p.apply(new ImportTransform(spannerConfig, options.getInputDir(), options.getWaitForIndexes()));

    p.run().waitUntilFinish();
  }
}
