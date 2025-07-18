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

import org.example.pipeline.transforms.ParseRecordFromJSON;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Requirements;

import static org.apache.beam.sdk.transforms.Contextful.fn;
import org.apache.beam.sdk.values.Row;
import static org.example.pipeline.ElasticsearchConnectionOptions.connectionConfigurationFromOptions;

/** */
public class EStoGCS {

  public interface EsToGCSOptions
      extends ElasticsearchConnectionOptions, ParseRecordFromJSON.ParseJSONsOptions {

    @Description("Output GCS location")
    @Validation.Required
    String getOutputLocation();

    void setOutputLocation(String value);
  }

  public static void main(String[] args) {
    var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(EsToGCSOptions.class);

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

    var schemaView = maybeParsed.getAvroSchemaView();

    maybeParsed
        .getRecords()
        .apply(
            "WriteToGCS",
            FileIO.<byte[]>write()
                .to(options.getOutputLocation())
                .via(
                    fn(
                        (record, context) ->
                            Types.toGenericRecord(context.sideInput(schemaView), record),
                        Requirements.requiresSideInputs(schemaView)),
                    fn(
                        (voidValue, context) -> AvroIO.sink(context.sideInput(schemaView)),
                        Requirements.requiresSideInputs(schemaView)))
                .withNaming(
                    FileIO.Write.defaultNaming(
                        retrieveOutputFilePrefix(options, "/data"), ".avsc")));

    maybeParsed
        .getNotParsed()
        .apply(
            "WriteNotParsedToGCS",
            FileIO.<Row>write()
                .to(options.getOutputLocation())
                .via(
                    fn(row -> AvroUtils.toGenericRecord(row)),
                    fn(
                        voidValue ->
                            AvroIO.sink(AvroUtils.toAvroSchema(Types.NON_PARSED_RECORD_SCHEMA))))
                .withNaming(
                    FileIO.Write.defaultNaming(
                        retrieveOutputFilePrefix(options, "-not-parsed/data"), ".avsc")));

    pipeline.run();
  }

  /** Avoid returning a dot prefix, resulting in a hidden file. */
  static String retrieveOutputFilePrefix(EsToGCSOptions options, String suffix) {
    var indexName = options.getEsIndex();
    return (indexName.startsWith(".") ? indexName.substring(1) : indexName) + suffix;
  }
}
