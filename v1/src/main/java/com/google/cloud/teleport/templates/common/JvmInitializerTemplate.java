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
package com.google.cloud.teleport.templates.common;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.options.CommonTemplateOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;

@Template(
    name = "Jvm_Initializer_Template",
    displayName = "Jvm Initializer Template",
    description =
        "This template is for testing purposes only. "
            + "Ensures that templates run with CommonTemplateOptions invoke the CommonTemplateJvmInitializer "
            + "class on the worker before pipeline execution.",
    category = TemplateCategory.UTILITIES,
    optionsClass = JvmInitializerTemplate.Options.class,
    testOnly = true)
class JvmInitializerTemplate {

  public interface Options extends CommonTemplateOptions {
    @TemplateParameter.Text(
        order = 1,
        description = "Input file(s)",
        helpText = "The input file pattern Dataflow reads from local filesystem on worker.")
    ValueProvider<String> getInputFile();

    void setInputFile(ValueProvider<String> value);

    @TemplateParameter.GcsWriteFolder(
        order = 2,
        description = "Output Cloud Storage file prefix",
        helpText = "Path and filename prefix for writing output files. Ex: gs://your-bucket/counts")
    ValueProvider<String> getOutput();

    void setOutput(ValueProvider<String> value);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    Pipeline p = Pipeline.create(options);
    p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
        .apply("WriteLines", TextIO.write().to(options.getOutput()));

    p.run();
  }
}
