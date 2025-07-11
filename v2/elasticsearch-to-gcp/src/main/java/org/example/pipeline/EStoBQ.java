/*
 * Copyright (C) 2024 Google Inc.
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
package org.example.pipeline;

import java.util.Optional;
import org.example.pipeline.transforms.ParseRecordFromJSON;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.values.Row;

import static org.example.pipeline.ElasticsearchConnectionOptions.connectionConfigurationFromOptions;

/** */
public class EStoBQ {

  public interface EsToBQOptions
      extends ElasticsearchConnectionOptions, ParseRecordFromJSON.ParseJSONsOptions {

    @Description("Output BigQuery dataset (in the format <project.dataset>).")
    @Validation.Required
    String getOutputBQDataset();

    void setOutputBQDataset(String value);

    @Description(
        "Output BQ table name. If empty, a sanitized version of the index name will be used"
            + " as table name in BigQuery.")
    @Default.String("")
    String getOutputBQTableName();

    void setOutputBQTableName(String value);

    static String getOutputTable(EsToBQOptions options) {
      return options.getOutputBQDataset()
          + "."
          + Optional.of(options.getOutputBQTableName())
              .filter(table -> !table.isEmpty())
              .orElseGet(
                  () -> {
                    var indexName = options.getEsIndex();
                    var noDotName =
                        (indexName.startsWith(".") ? indexName.substring(1) : indexName);
                    return noDotName.replace(".", "_");
                  });
    }
  }

  public static void main(String[] args) {
    var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(EsToBQOptions.class);
    var outputTable = EsToBQOptions.getOutputTable(options);

    // Create the pipeline
    var pipeline = Pipeline.create(options);

    var maybeParsed =
        pipeline
            .apply(
                "ReadFromES",
                ElasticsearchIO.read()
                    .withConnectionConfiguration(connectionConfigurationFromOptions(options))
                    .withPointInTimeSearch())
            .apply("TransformToAvro", ParseRecordFromJSON.create());

    var dataSchemaAsMap = maybeParsed.getBigQuerySchemaView(outputTable);

    maybeParsed
        .getRecords()
        .apply(
            "WriteToBigQuery",
            BigQueryIO.<byte[]>write()
                .withoutValidation()
                .to(BigQueryHelpers.parseTableSpec(outputTable))
                .withAvroFormatFunction(
                    avroRequest ->
                        Types.toGenericRecord(
                            avroRequest.getSchema().toString(), avroRequest.getElement()))
                .withSchemaFromView(dataSchemaAsMap)
                .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withSchemaUpdateOptions(
                    Set.of(
                        BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                        BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_RELAXATION)));

    maybeParsed
        .getNotParsed()
        .apply(
            "WriteNotParsedToBigQuery",
            BigQueryIO.<Row>write()
                .withoutValidation()
                .useBeamSchema()
                .to(BigQueryHelpers.parseTableSpec(outputTable + "_not_parsed"))
                .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withSchemaUpdateOptions(
                    Set.of(
                        BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                        BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_RELAXATION)));

    pipeline.run();
  }
}
