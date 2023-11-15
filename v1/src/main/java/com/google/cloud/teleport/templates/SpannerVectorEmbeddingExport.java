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
package com.google.cloud.teleport.templates;

import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import com.google.cloud.teleport.templates.SpannerVectorEmbeddingExport.SpannerToVectorEmbeddingJsonOptions;
import com.google.cloud.teleport.templates.common.SpannerConverters;
import com.google.cloud.teleport.templates.common.SpannerConverters.CreateTransactionFnWithTimestamp;
import com.google.cloud.teleport.templates.common.SpannerConverters.VectorSearchStructValidator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.LocalSpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.Transaction;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dataflow template which export vector embeddings from Spanner to GCS in json format. It exports a
 * Spanner table using <a
 * href="https://cloud.google.com/spanner/docs/reads#read_data_in_parallel">Batch API</a>, which
 * creates multiple workers in parallel for better performance. The result is written to a JSON file
 * in Google Cloud Storage.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_Cloud_Spanner_to_Vector_Embedding.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Cloud_Spanner_vectors_to_Cloud_Storage",
    category = TemplateCategory.BATCH,
    displayName = "Cloud Spanner vectors to Cloud Storage for Vertex Vector Search",
    optionsClass = SpannerToVectorEmbeddingJsonOptions.class,
    description = {
      "The Cloud Spanner to Vector Embeddings on Cloud Storage template is a batch pipeline that exports vector embeddings data from Cloud Spanner's table to Cloud Storage in JSON format. "
          + "Vector embeddings are exported to a Cloud Storage folder specified by the user in the template parameters."
          + " The Cloud Storage folder will contain the list of exported `.json` files representing vector embeddings in a format supported by Vertex AI Vector Search Index.\n",
      "Check <a href=\"https://cloud.google.com/vertex-ai/docs/vector-search/setup/format-structure#json\">Vector Search Format Structure</a> for additional details."
    },
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-spanner-to-vertex-vector-search",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The Cloud Spanner database must exist.",
      "The output Cloud Storage bucket must exist.",
      "In addition to the Identity and Access Management (IAM) roles necessary to run Dataflow jobs, you must also have the <a href=\"https://cloud.google.com/spanner/docs/export#iam\">appropriate IAM roles</a> for reading your Cloud Spanner data and writing to your Cloud Storage bucket."
    })
@SuppressWarnings("unused")
public class SpannerVectorEmbeddingExport {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerVectorEmbeddingExport.class);

  /** Custom PipelineOptions. */
  public interface SpannerToVectorEmbeddingJsonOptions extends PipelineOptions {
    @TemplateParameter.ProjectId(
        order = 10,
        description = "Cloud Spanner Project Id",
        helpText = "The project ID of the Cloud Spanner instance.")
    ValueProvider<String> getSpannerProjectId();

    void setSpannerProjectId(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 20,
        regexes = {"[a-z][a-z0-9\\-]*[a-z0-9]"},
        description = "Cloud Spanner instance ID",
        helpText =
            "The instance ID of the Cloud Spanner from which you want to export the vector embeddings.")
    ValueProvider<String> getSpannerInstanceId();

    void setSpannerInstanceId(ValueProvider<String> spannerInstanceId);

    @TemplateParameter.Text(
        order = 30,
        regexes = {"[a-z][a-z0-9_\\-]*[a-z0-9]"},
        description = "Cloud Spanner database ID",
        helpText =
            "The database ID of the Cloud Spanner from which you want to export the vector embeddings.")
    ValueProvider<String> getSpannerDatabaseId();

    void setSpannerDatabaseId(ValueProvider<String> spannerDatabaseId);

    @TemplateParameter.Text(
        order = 40,
        regexes = {"^.+$"},
        description = "Spanner Table",
        helpText = "Spanner Table to read from")
    ValueProvider<String> getSpannerTable();

    void setSpannerTable(ValueProvider<String> table);

    @TemplateParameter.Text(
        order = 50,
        description = "Columns to Export from Spanner Table",
        helpText =
            "Comma separated list of columns which are required for Vertex AI Vector Search Index."
                + " The `id` & `embedding` are required columns for Vertex Vector Search."
                + " If the column names don't precisely align with the Vertex AI Vector Search Index input structure,"
                + " you can establish column mappings using aliases. If you have the columns that don't match the"
                + " format expected by Vertex, you can use the notation `from:to`. For example, if the columns are"
                + " `id` and `my_embedding`, in which `id` matches what Vertex expects but the embedding column is named differently,"
                + " `id, my_embedding:embedding` should be specified.")
    ValueProvider<String> getSpannerColumnsToExport();

    void setSpannerColumnsToExport(ValueProvider<String> value);

    @TemplateParameter.GcsWriteFolder(
        order = 60,
        description = "Output files folder in Cloud Storage",
        helpText = "The Cloud Storage folder for writing output files. Must end with a slash.",
        example = "gs://your-bucket/folder1/")
    ValueProvider<String> getGcsOutputFolder();

    void setGcsOutputFolder(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 70,
        description = "Output files prefix in Cloud Storage",
        helpText = "The filename prefix for writing output files.",
        example = "vector-embeddings")
    ValueProvider<String> getGcsOutputFilePrefix();

    void setGcsOutputFilePrefix(ValueProvider<String> textWritePrefix);

    @TemplateParameter.Text(
        order = 80,
        optional = true,
        description = "Cloud Spanner Endpoint to call",
        helpText =
            "The Cloud Spanner endpoint to call in the template. The default is set to https://batch-spanner.googleapis.com.",
        example = "https://batch-spanner.googleapis.com")
    @Default.String("https://batch-spanner.googleapis.com")
    ValueProvider<String> getSpannerHost();

    void setSpannerHost(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 90,
        optional = true,
        regexes = {
          "^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):(([0-9]{2})(\\.[0-9]+)?)Z$"
        },
        description = "Timestamp to read stale data from a version in the past.",
        helpText =
            "If set, specifies the time when the database version must be taken."
                + " String is in the RFC 3339 format in UTC time. "
                + " Timestamp must be in the past and maximum timestamp staleness applies; see "
                + "<a href=\"https://cloud.google.com/spanner/docs/timestamp-bounds#maximum_timestamp_staleness\">Maximum Timestamp Staleness</a>."
                + " If not set, strong bound is used to read the latest data; see "
                + "<a href=\"https://cloud.google.com/spanner/docs/timestamp-bounds#strong\">Timestamp Strong Bounds</a>.",
        example = "1990-12-31T23:59:60Z")
    @Default.String(value = "")
    ValueProvider<String> getSpannerVersionTime();

    void setSpannerVersionTime(ValueProvider<String> value);

    @TemplateParameter.Boolean(
        order = 100,
        optional = true,
        description = "Use independent compute resource (Spanner DataBoost).",
        helpText =
            "Use Spanner on-demand compute so the export job will run on independent compute"
                + " resources and have no impact to current Spanner workloads. This will incur"
                + " additional charges in Spanner."
                + " Refer <a href=\" https://cloud.google.com/spanner/docs/databoost/databoost-overview\">Data Boost Overview</a>.")
    @Default.Boolean(false)
    ValueProvider<Boolean> getSpannerDataBoostEnabled();

    void setSpannerDataBoostEnabled(ValueProvider<Boolean> value);

    @TemplateParameter.Enum(
        order = 110,
        enumOptions = {
          @TemplateEnumOption("LOW"),
          @TemplateEnumOption("MEDIUM"),
          @TemplateEnumOption("HIGH")
        },
        optional = true,
        description = "Priority for Spanner RPC invocations",
        helpText =
            "The request priority for Cloud Spanner calls. The value must be one of:"
                + " [HIGH,MEDIUM,LOW]. Defaults to: MEDIUM.")
    ValueProvider<RpcPriority> getSpannerPriority();

    void setSpannerPriority(ValueProvider<RpcPriority> value);
  }

  /**
   * Runs a pipeline which reads in vector embeddings records from Spanner, and writes the JSON to
   * TextIO sink.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    LOG.info("Starting pipeline setup");
    PipelineOptionsFactory.register(SpannerToVectorEmbeddingJsonOptions.class);

    SpannerToVectorEmbeddingJsonOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(SpannerToVectorEmbeddingJsonOptions.class);

    FileSystems.setDefaultPipelineOptions(options);
    Pipeline pipeline = Pipeline.create(options);

    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withHost(options.getSpannerHost())
            .withProjectId(options.getSpannerProjectId())
            .withInstanceId(options.getSpannerInstanceId())
            .withDatabaseId(options.getSpannerDatabaseId())
            .withRpcPriority(options.getSpannerPriority())
            .withDataBoostEnabled(options.getSpannerDataBoostEnabled());

    ValueProvider<String> gcsOutputFilePrefix = options.getGcsOutputFilePrefix();

    // Concatenating cloud storage folder with file prefix to get complete path
    ValueProvider<String> gcsOutputFilePathWithPrefix =
        ValueProvider.NestedValueProvider.of(
            options.getGcsOutputFolder(),
            (SerializableFunction<String, String>)
                folder -> {
                  if (!folder.endsWith("/")) {
                    // Appending the slash if not provided by user
                    folder = folder + "/";
                  }
                  return folder + gcsOutputFilePrefix.get();
                });

    PTransform<PBegin, PCollection<ReadOperation>> spannerExport =
        SpannerConverters.ExportTransformFactory.create(
            options.getSpannerTable(),
            spannerConfig,
            gcsOutputFilePathWithPrefix,
            options.getSpannerVersionTime(),
            options.getSpannerColumnsToExport(),
            ValueProvider.StaticValueProvider.of(/* disable_schema_export= */ false));

    /* CreateTransaction and CreateTransactionFn classes in LocalSpannerIO
     * only take a timestamp object for exact staleness which works when
     * parameters are provided during template compile time. They do not work with
     * a Timestamp valueProvider which can take parameters at runtime. Hence a new
     * ParDo class CreateTransactionFnWithTimestamp had to be created for this
     * purpose.
     */
    PCollectionView<Transaction> tx =
        pipeline
            .apply("Setup for Transaction", Create.of(1))
            .apply(
                "Create transaction",
                ParDo.of(
                    new CreateTransactionFnWithTimestamp(
                        spannerConfig, options.getSpannerVersionTime())))
            .apply("As PCollectionView", View.asSingleton());

    PCollection<String> json =
        pipeline
            .apply("Create export", spannerExport)
            // We need to use LocalSpannerIO.readAll() instead of LocalSpannerIO.read()
            // because ValueProvider parameters such as table name required for
            // LocalSpannerIO.read() can be read only inside DoFn but LocalSpannerIO.read() is of
            // type PTransform<PBegin, Struct>, which prevents prepending it with DoFn that reads
            // these parameters at the pipeline execution time.
            .apply(
                "Read all records",
                LocalSpannerIO.readAll().withTransaction(tx).withSpannerConfig(spannerConfig))
            .apply(
                "Struct To JSON",
                MapElements.into(TypeDescriptors.strings())
                    .via(
                        struct ->
                            (new SpannerConverters.StructJSONPrinter(
                                    new VectorSearchStructValidator()))
                                .print(struct)));

    json.apply(
        "Write to storage", TextIO.write().to(gcsOutputFilePathWithPrefix).withSuffix(".json"));

    pipeline.run();
    LOG.info("Completed pipeline setup");
  }
}
