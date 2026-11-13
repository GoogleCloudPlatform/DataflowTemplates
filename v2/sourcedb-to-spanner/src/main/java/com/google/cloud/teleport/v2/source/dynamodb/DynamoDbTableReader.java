/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.source.dynamodb;

import com.google.cloud.teleport.v2.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.reader.io.schema.SourceTableSchema;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.aws2.common.ClientConfiguration;
import org.apache.beam.sdk.io.aws2.dynamodb.DynamoDBIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

/**
 * Beam {@link PTransform} that reads items from a DynamoDB table using {@link DynamoDBIO} and maps
 * each item to a {@link SourceRow}.
 */
public class DynamoDbTableReader extends PTransform<PBegin, PCollection<SourceRow>> {

  private final ClientConfiguration clientConfiguration;
  private final SourceSchemaReference sourceSchemaReference;
  private final SourceTableSchema sourceTableSchema;
  private final int totalSegments;

  public DynamoDbTableReader(
      ClientConfiguration clientConfiguration,
      SourceSchemaReference sourceSchemaReference,
      SourceTableSchema sourceTableSchema,
      int totalSegments) {
    this.clientConfiguration = clientConfiguration;
    this.sourceSchemaReference = sourceSchemaReference;
    this.sourceTableSchema = sourceTableSchema;
    this.totalSegments = Math.max(1, totalSegments);
  }

  @Override
  public PCollection<SourceRow> expand(PBegin input) {
    final String tableName = sourceTableSchema.tableName();
    final int segments = this.totalSegments;
    return input
        .apply(
            "ReadDynamoDbItems_" + tableName,
            DynamoDBIO.<List<Map<String, AttributeValue>>>read()
                .withClientConfiguration(clientConfiguration)
                .withScanRequestFn(
                    in ->
                        ScanRequest.builder().tableName(tableName).totalSegments(segments).build())
                .items())
        .apply(
            "MapDynamoDbItemsToSourceRows_" + tableName,
            ParDo.of(new MapItemsToSourceRowFn(sourceSchemaReference, sourceTableSchema)))
        .setCoder(SerializableCoder.of(SourceRow.class));
  }

  static class MapItemsToSourceRowFn extends DoFn<List<Map<String, AttributeValue>>, SourceRow> {

    private final DynamoDbRowMapper rowMapper;

    MapItemsToSourceRowFn(
        SourceSchemaReference sourceSchemaReference, SourceTableSchema sourceTableSchema) {
      this.rowMapper = new DynamoDbRowMapper(sourceSchemaReference, sourceTableSchema);
    }

    @ProcessElement
    public void processElement(
        @Element List<Map<String, AttributeValue>> items, OutputReceiver<SourceRow> out) {
      if (items == null) {
        return;
      }
      for (Map<String, AttributeValue> item : items) {
        out.output(rowMapper.mapRow(item));
      }
    }
  }
}
