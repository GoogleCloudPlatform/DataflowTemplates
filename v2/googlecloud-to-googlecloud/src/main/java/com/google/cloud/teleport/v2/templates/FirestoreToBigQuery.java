/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.utils.GCSUtils.getGcsFileAsString;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.metadata.MultiTemplate;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.options.BigQueryCommonOptions;
import com.google.cloud.teleport.v2.options.BigQueryStorageApiBatchOptions;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.TransformTextViaJavascript;
import com.google.cloud.teleport.v2.transforms.PythonExternalTextTransformer;
import com.google.cloud.teleport.v2.utils.FirestoreConverters.FirestoreReadOptions;
import com.google.cloud.teleport.v2.utils.FirestoreConverters.ReadJsonEntities;
import com.google.common.base.Strings;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;

/**
 * Dataflow template which copies Firestore Entities to a BigQuery table.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/googlecloud-to-googlecloud/README_Firestore_to_BigQuery_Flex.md">README</a>
 * for instructions on how to use or modify this template.
 */
@MultiTemplate({
  @Template(
      name = "Firestore_to_BigQuery_Flex",
      category = TemplateCategory.BATCH,
      displayName = "Firestore (Datastore mode) to BigQuery",
      description = "Batch pipeline. Reads Firestore entities and writes them to BigQuery.",
      optionsClass = FirestoreToBigQuery.FirestoreToBigQueryOptions.class,
      skipOptions = {
        "datastoreReadNamespace",
        "datastoreReadGqlQuery",
        "datastoreReadProjectId",
        "javascriptTextTransformReloadIntervalMinutes",
        "pythonExternalTextTransformGcsPath",
        "pythonExternalTextTransformFunctionName"
      },
      flexContainerName = "firestore-to-bigquery",
      contactInformation = "https://cloud.google.com/support",
      hidden = true),
  @Template(
      name = "Firestore_to_BigQuery_Flex",
      category = TemplateCategory.BATCH,
      displayName = "Firestore (Datastore mode) to BigQuery with Python UDF",
      type = Template.TemplateType.XLANG,
      description = "Batch pipeline. Reads Firestore entities and writes them to BigQuery.",
      optionsClass = FirestoreToBigQuery.FirestoreToBigQueryOptions.class,
      skipOptions = {
        "datastoreReadNamespace",
        "datastoreReadGqlQuery",
        "datastoreReadProjectId",
        "javascriptTextTransformGcsPath",
        "javascriptTextTransformFunctionName",
        "javascriptTextTransformReloadIntervalMinutes"
      },
      flexContainerName = "firestore-to-bigquery",
      contactInformation = "https://cloud.google.com/support",
      hidden = true)
})
public class FirestoreToBigQuery {
  public interface FirestoreToBigQueryOptions
      extends PipelineOptions,
          FirestoreReadOptions,
          PythonExternalTextTransformer.PythonExternalTextTransformerOptions,
          BigQueryStorageApiBatchOptions,
          BigQueryCommonOptions.WriteOptions {
    @TemplateParameter.BigQueryTable(
        order = 1,
        description = "BigQuery output table",
        helpText =
            "BigQuery table location to write the output to. The name should be in the format "
                + "`<project>:<dataset>.<table_name>`. The table's schema must match input objects.")
    String getOutputTableSpec();

    void setOutputTableSpec(String value);

    @TemplateParameter.GcsWriteFolder(
        order = 2,
        description = "Temporary directory for BigQuery loading process",
        helpText = "Temporary directory for BigQuery loading process",
        example = "gs://your-bucket/your-files/temp_dir")
    @Validation.Required
    String getBigQueryLoadingTemporaryDirectory();

    void setBigQueryLoadingTemporaryDirectory(String directory);

    @TemplateParameter.GcsReadFile(
        order = 3,
        optional = true,
        description = "Cloud Storage path to BigQuery JSON schema",
        helpText =
            "The Cloud Storage path for the BigQuery JSON schema. If `createDisposition` is not set, or set to CREATE_IF_NEEDED, this parameter must be specified.",
        example = "gs://your-bucket/your-schema.json")
    String getBigQuerySchemaPath();

    void setBigQuerySchemaPath(String path);
  }

  private static BigQueryIO.Write<TableRow> writeToBigQuery(FirestoreToBigQueryOptions options) {
    BigQueryIO.Write<TableRow> write =
        BigQueryIO.writeTableRows()
            .withoutValidation()
            .to(options.getOutputTableSpec())
            .withWriteDisposition(WriteDisposition.valueOf(options.getWriteDisposition()))
            .withCustomGcsTempLocation(
                StaticValueProvider.of(options.getBigQueryLoadingTemporaryDirectory()));

    if (CreateDisposition.valueOf(options.getCreateDisposition())
        == CreateDisposition.CREATE_NEVER) {
      write = write.withCreateDisposition(CreateDisposition.CREATE_NEVER);
    } else {
      write =
          write
              .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
              .withJsonSchema(getGcsFileAsString(options.getBigQuerySchemaPath()));
    }
    if (options.getUseStorageWriteApi()) {
      write = write.withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API);
    }

    return write;
  }

  /**
   * Runs a pipeline which reads in Entities from Firestore, passes in the JSON encoded Entities to
   * a Javascript UDF that returns JSON that conforms to the BigQuery TableRow spec and writes the
   * TableRows to BigQuery.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    FirestoreToBigQueryOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FirestoreToBigQueryOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    boolean useJavascriptUdf = !Strings.isNullOrEmpty(options.getJavascriptTextTransformGcsPath());
    boolean usePythonUdf = !Strings.isNullOrEmpty(options.getPythonExternalTextTransformGcsPath());
    if (useJavascriptUdf == usePythonUdf) {
      throw new IllegalArgumentException(
          "Either javascript or Python gcs path must be provided, but not both.");
    }
    if (usePythonUdf) {
      pipeline
          .apply(
              ReadJsonEntities.newBuilder()
                  .setGqlQuery(options.getFirestoreReadGqlQuery())
                  .setProjectId(options.getFirestoreReadProjectId())
                  .setNamespace(options.getFirestoreReadNamespace())
                  .build())
          .apply(
              "MapToRecord",
              PythonExternalTextTransformer.FailsafeRowPythonExternalUdf.stringMappingFunction())
          .setRowSchema(PythonExternalTextTransformer.FailsafeRowPythonExternalUdf.ROW_SCHEMA)
          .setCoder(
              RowCoder.of(PythonExternalTextTransformer.FailsafeRowPythonExternalUdf.ROW_SCHEMA))
          .apply(
              "InvokeUDF",
              PythonExternalTextTransformer.FailsafePythonExternalUdf.newBuilder()
                  .setFileSystemPath(options.getPythonExternalTextTransformGcsPath())
                  .setFunctionName(options.getPythonExternalTextTransformFunctionName())
                  .build())
          .setRowSchema(PythonExternalTextTransformer.FailsafeRowPythonExternalUdf.FAILSAFE_SCHEMA)
          .apply(
              MapElements.via(
                  new SimpleFunction<Row, TableRow>() {
                    @Override
                    public TableRow apply(Row row) {
                      Row transformedRow = row.getValue("transformed");
                      return BigQueryConverters.convertJsonToTableRow(
                          transformedRow.getValue("message"));
                    }
                  }))
          .apply("WriteBigQuery", writeToBigQuery(options));
    } else {
      pipeline
          .apply(
              ReadJsonEntities.newBuilder()
                  .setGqlQuery(options.getFirestoreReadGqlQuery())
                  .setProjectId(options.getFirestoreReadProjectId())
                  .setNamespace(options.getFirestoreReadNamespace())
                  .build())
          .apply(
              TransformTextViaJavascript.newBuilder()
                  .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                  .setFunctionName(options.getJavascriptTextTransformFunctionName())
                  .build())
          .apply(
              MapElements.via(
                  new SimpleFunction<String, TableRow>() {
                    @Override
                    public TableRow apply(String json) {
                      return BigQueryConverters.convertJsonToTableRow(json);
                    }
                  }))
          .apply("WriteBigQuery", writeToBigQuery(options));
    }

    pipeline.run();
  }
}
