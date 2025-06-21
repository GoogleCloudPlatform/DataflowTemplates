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
package org.example.pipeline.transforms;

import autovalue.shaded.com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

/** */
public class RecordParseResult implements POutput {
  private final Pipeline pipeline;
  private final TupleTag<Row> failedTag;
  private final PCollection<Row> failed;
  private final TupleTag<byte[]> successTag;
  private final PCollection<byte[]> success;
  private final TupleTag<String> schemaTag;
  private final PCollection<String> detectedSchema;

  public static RecordParseResult in(
      Pipeline pipeline,
      TupleTag<Row> failedTag,
      PCollection<Row> failed,
      TupleTag<byte[]> successTag,
      PCollection<byte[]> success,
      TupleTag<String> schemaTag,
      PCollection<String> detectedSchema) {
    return new RecordParseResult(
        pipeline, failedTag, failed, successTag, success, schemaTag, detectedSchema);
  }

  private RecordParseResult(
      Pipeline pipeline,
      TupleTag<Row> failedTag,
      PCollection<Row> failed,
      TupleTag<byte[]> successTag,
      PCollection<byte[]> success,
      TupleTag<String> schemaTag,
      PCollection<String> detectedSchema) {
    this.pipeline = pipeline;
    this.failedTag = failedTag;
    this.failed = failed;
    this.successTag = successTag;
    this.success = success;
    this.schemaTag = schemaTag;
    this.detectedSchema = detectedSchema;
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    var output = ImmutableMap.<TupleTag<?>, PValue>builder();

    if (failedTag != null) {
      output.put(failedTag, Preconditions.checkArgumentNotNull(failed));
    }
    if (successTag != null) {
      output.put(successTag, Preconditions.checkArgumentNotNull(success));
    }
    if (schemaTag != null) {
      output.put(schemaTag, Preconditions.checkArgumentNotNull(detectedSchema));
    }

    return output.build();
  }

  public PCollection<byte[]> getRecords() {
    return success;
  }

  public PCollection<Row> getNotParsed() {
    return failed;
  }

  public PCollection<String> getDetectedAvroSchema() {
    return detectedSchema;
  }

  public PCollectionView<Map<String, String>> getBigQuerySchemaView(String bqOutputTable) {
    return getDetectedAvroSchema()
        .apply("DynamicTableSchema", new AvroSchemaToBigQuerySchemaView(bqOutputTable));
  }

  public PCollectionView<String> getAvroSchemaView() {
    return getDetectedAvroSchema().apply("ToAvroSchemaView", new AvroSchemaToSchemaView());
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public void finishSpecifyingOutput(
      String transformName, PInput input, PTransform<?, ?> transform) {}

  static class AvroSchemaToBigQuerySchemaView
      extends PTransform<PCollection<String>, PCollectionView<Map<String, String>>> {

    private final String bqOutputTable;

    public AvroSchemaToBigQuerySchemaView(String bqOutputTable) {
      this.bqOutputTable = bqOutputTable;
    }

    @Override
    public PCollectionView<Map<String, String>> expand(PCollection<String> input) {
      return input
          .apply(
              "AvroSchemaToMap",
              MapElements.into(
                      TypeDescriptors.maps(TypeDescriptors.strings(), TypeDescriptors.strings()))
                  .via(
                      avroSchemaStr ->
                          Map.of(
                              BigQueryHelpers.toTableSpec(
                                  BigQueryHelpers.parseTableSpec(bqOutputTable)),
                              BigQueryHelpers.toJsonString(
                                  BigQueryUtils.toTableSchema(
                                      AvroUtils.toBeamSchema(
                                          new Schema.Parser().parse(avroSchemaStr)))))))
          .apply("CombineSingle", Combine.globally((v, d) -> v))
          .apply("ToView", View.asSingleton());
    }
  }

  static class AvroSchemaToSchemaView
      extends PTransform<PCollection<String>, PCollectionView<String>> {

    @Override
    public PCollectionView<String> expand(PCollection<String> input) {
      return input
          .apply("CombineSingle", Combine.globally((v, d) -> v))
          .apply("ToView", View.asSingleton());
    }
  }
}