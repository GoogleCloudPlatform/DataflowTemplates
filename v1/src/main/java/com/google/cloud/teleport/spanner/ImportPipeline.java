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
package com.google.cloud.teleport.spanner;

import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateCreationParameter;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.spanner.ImportPipeline.Options;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;

/** Avro to Cloud Spanner Import pipeline. */
@Template(
    name = "GCS_Avro_to_Cloud_Spanner",
    category = TemplateCategory.BATCH,
    displayName = "Avro Files on Cloud Storage to Cloud Spanner",
    description =
        "A pipeline to import a Cloud Spanner database from a set of Avro files in Cloud Storage.",
    optionsClass = Options.class,
    contactInformation = "https://cloud.google.com/support")
public class ImportPipeline {

  /** Options for {@link ImportPipeline}. */
  public interface Options extends PipelineOptions {

    @TemplateParameter.Text(
        order = 1,
        regexes = {"^[a-z0-9\\-]+$"},
        description = "Cloud Spanner instance id",
        helpText = "The instance id of the Cloud Spanner database that you want to import to.")
    ValueProvider<String> getInstanceId();

    void setInstanceId(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 2,
        regexes = {"^[a-z_0-9\\-]+$"},
        description = "Cloud Spanner database id",
        helpText =
            "The database id of the Cloud Spanner database that you want to import into (must already exist).")
    ValueProvider<String> getDatabaseId();

    void setDatabaseId(ValueProvider<String> value);

    @TemplateParameter.GcsReadFolder(
        order = 3,
        description = "Cloud storage input directory",
        helpText = "The Cloud Storage path where the Avro files should be imported from.")
    ValueProvider<String> getInputDir();

    void setInputDir(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 4,
        optional = true,
        description = "Cloud Spanner Endpoint to call",
        helpText = "The Cloud Spanner endpoint to call in the template. Only used for testing.",
        example = "https://batch-spanner.googleapis.com")
    @Default.String("https://batch-spanner.googleapis.com")
    ValueProvider<String> getSpannerHost();

    void setSpannerHost(ValueProvider<String> value);

    @TemplateParameter.Boolean(
        order = 5,
        optional = true,
        description = "Wait for Indexes",
        helpText =
            "By default the import pipeline is not blocked on index creation, and it "
                + "may complete with indexes still being created in the background. In testing, it may "
                + "be useful to set this option to false so that the pipeline waits until indexes are "
                + "finished.")
    @Default.Boolean(false)
    ValueProvider<Boolean> getWaitForIndexes();

    void setWaitForIndexes(ValueProvider<Boolean> value);

    @TemplateParameter.Boolean(
        order = 6,
        optional = true,
        description = "Wait for Foreign Keys",
        helpText =
            "By default the import pipeline is not blocked on foreign key creation, and it may complete"
                + " with foreign keys still being created in the background. In testing, it may be"
                + " useful to set this option to false so that the pipeline waits until foreign keys"
                + " are finished.")
    @Default.Boolean(false)
    ValueProvider<Boolean> getWaitForForeignKeys();

    void setWaitForForeignKeys(ValueProvider<Boolean> value);

    @TemplateParameter.Boolean(
        order = 7,
        optional = true,
        description = "Wait for Foreign Keys",
        helpText =
            "By default the import pipeline is blocked on change stream creation. If false, it may"
                + " complete with change streams still being created in the background.")
    @Default.Boolean(true)
    ValueProvider<Boolean> getWaitForChangeStreams();

    void setWaitForChangeStreams(ValueProvider<Boolean> value);

    @TemplateParameter.Boolean(
        order = 8,
        optional = true,
        description = "Create Indexes early",
        helpText =
            "Flag to turn off early index creation if there are many indexes. Indexes and Foreign keys are created after dataload. If there are more than "
                + "40 DDL statements to be executed after dataload, it is preferable to create the "
                + "indexes before datalod. This is the flag to turn the feature off.")
    @Default.Boolean(true)
    ValueProvider<Boolean> getEarlyIndexCreateFlag();

    void setEarlyIndexCreateFlag(ValueProvider<Boolean> value);

    @TemplateCreationParameter(value = "false")
    @Description("If true, wait for job finish")
    @Default.Boolean(true)
    boolean getWaitUntilFinish();

    @TemplateParameter.ProjectId(
        order = 9,
        optional = true,
        description = "Cloud Spanner Project Id",
        helpText = "The project id of the Cloud Spanner instance.")
    ValueProvider<String> getSpannerProjectId();

    void setSpannerProjectId(ValueProvider<String> value);

    void setWaitUntilFinish(boolean value);

    @TemplateParameter.Text(
        order = 10,
        optional = true,
        regexes = {"[0-9]+"},
        description = "DDL Creation timeout in minutes",
        helpText = "DDL Creation timeout in minutes.")
    @Default.Integer(30)
    ValueProvider<Integer> getDDLCreationTimeoutInMinutes();

    void setDDLCreationTimeoutInMinutes(ValueProvider<Integer> value);

    @TemplateParameter.Enum(
        order = 11,
        enumOptions = {"LOW", "MEDIUM", "HIGH"},
        optional = true,
        description = "Priority for Spanner RPC invocations",
        helpText =
            "The request priority for Cloud Spanner calls. The value must be one of: [HIGH,MEDIUM,LOW].")
    ValueProvider<RpcPriority> getSpannerPriority();

    void setSpannerPriority(ValueProvider<RpcPriority> value);
  }

  public static void main(String[] args) {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    Pipeline p = Pipeline.create(options);

    SpannerConfig spannerConfig =
        SpannerConfig.create()
            // Temporary fix explicitly setting SpannerConfig.projectId to the default project
            // if spannerProjectId is not provided as a parameter. Required as of Beam 2.38,
            // which no longer accepts null label values on metrics, and SpannerIO#setup() has
            // a bug resulting in the label value being set to the original parameter value,
            // with no fallback to the default project.
            // TODO: remove NestedValueProvider when this is fixed in Beam.
            .withProjectId(
                NestedValueProvider.of(
                    options.getSpannerProjectId(),
                    (SerializableFunction<String, String>)
                        input -> input != null ? input : SpannerOptions.getDefaultProjectId()))
            .withHost(options.getSpannerHost())
            .withInstanceId(options.getInstanceId())
            .withDatabaseId(options.getDatabaseId())
            .withRpcPriority(options.getSpannerPriority());

    p.apply(
        new ImportTransform(
            spannerConfig,
            options.getInputDir(),
            options.getWaitForIndexes(),
            options.getWaitForForeignKeys(),
            options.getWaitForChangeStreams(),
            options.getEarlyIndexCreateFlag(),
            options.getDDLCreationTimeoutInMinutes()));

    PipelineResult result = p.run();

    if (options.getWaitUntilFinish()
        &&
        /* Only if template location is null, there is a dataflow job to wait for. Else it's
         * template generation which doesn't start a dataflow job.
         */
        options.as(DataflowPipelineOptions.class).getTemplateLocation() == null) {
      result.waitUntilFinish();
    }
  }
}
