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
package com.google.cloud.syndeo.transforms.bigtable;

import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableWriteResult;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.utils.RowSelector;
import org.apache.beam.sdk.schemas.utils.SelectHelpers;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;

public class BigTableIOWriteSchemaBasedTransform
    extends PTransform<PCollectionRowTuple, PCollectionRowTuple> implements SchemaTransform {

  private static final String API = "bigtable";
  private static final String INPUT_TAG = "INPUT";

  private final String projectId;
  private final String instanceId;
  private final String tableId;
  private final List<String> keyColumns;

  BigTableIOWriteSchemaBasedTransform(
      String projectId, String instanceId, String tableId, List<String> keyColumns) {
    this.projectId = projectId;
    this.instanceId = instanceId;
    this.tableId = tableId;
    this.keyColumns = keyColumns;
  }

  @Override
  public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
    return this;
  }

  @Override
  public PCollectionRowTuple expand(PCollectionRowTuple input) {
    PCollection<Row> inputData = input.get(INPUT_TAG);

    // STEP 1: Select the key columns from the input Rows
    final Schema keySchema =
        Schema.builder()
            .addFields(
                keyColumns.stream()
                    .map(colName -> inputData.getSchema().getField(colName))
                    .collect(Collectors.toList()))
            .build();

    RowSelector keySelector =
        new SelectHelpers.RowSelectorContainer(
            inputData.getSchema(),
            FieldAccessDescriptor.withFieldNames(keyColumns).resolve(inputData.getSchema()),
            false);

    PCollection<KV<Row, Row>> keyedRows =
        inputData
            .apply(WithKeys.of(row -> keySelector.select(row)))
            .setCoder(KvCoder.of(SchemaCoder.of(keySchema), SchemaCoder.of(inputData.getSchema())));

    // STEP 2: Convert all data types to ByteString data
    final Schema recordBytesSchema =
        Schema.builder()
            .addFields(
                inputData.getSchema().getFields().stream()
                    .map(field -> Schema.Field.of(field.getName(), Schema.FieldType.BYTES))
                    .collect(Collectors.toList()))
            .build();

    PCollection<KV<byte[], Row>> byteEncodedKeyedRows =
        keyedRows
            .apply(
                ParDo.of(
                    new DoFn<KV<Row, Row>, KV<byte[], Row>>() {
                      @ProcessElement
                      public void process(
                          @DoFn.Element KV<Row, Row> elm,
                          OutputReceiver<KV<byte[], Row>> receiver) {
                        List<byte[]> byteEncodedColumns =
                            elm.getValue().getSchema().getFields().stream()
                                .map(
                                    field -> {
                                      if (BeamSchemaToBytesTransformers
                                          .TYPE_TO_BYTES_TRANSFORMATIONS
                                          .containsKey(field.getType().getTypeName())) {
                                        return BeamSchemaToBytesTransformers
                                            .TYPE_TO_BYTES_TRANSFORMATIONS
                                            .get(field.getType().getTypeName())
                                            .apply(elm.getValue(), field);
                                      } else {
                                        throw new IllegalArgumentException(
                                            "Unsupported column type: "
                                                + field.getType().getTypeName().toString());
                                      }
                                    })
                                .collect(Collectors.toList());

                        byte[] byteEncodedKeyCols =
                            StandardCharsets.UTF_8
                                .encode(
                                    String.join(
                                        "",
                                        elm.getKey().getValues().stream()
                                            .map(columnValue -> columnValue.toString())
                                            .collect(Collectors.toList())))
                                .array();

                        Row.Builder valueRow = Row.withSchema(recordBytesSchema);
                        // TODO(pabloem): This is more inefficient than valueRow.addValues(bEC), but
                        // that was giving
                        //   me trouble so I didn't use it.
                        byteEncodedColumns.forEach(bytes -> valueRow.addValue(bytes));
                        receiver.output(KV.of(byteEncodedKeyCols, valueRow.build()));
                      }
                    }))
            .setCoder(KvCoder.of(ByteArrayCoder.of(), SchemaCoder.of(recordBytesSchema)));

    // STEP 3: Convert KV<bytes, Row> into KV<ByteString, List<SetCell<...>>>
    PCollection<KV<ByteString, Iterable<Mutation>>> bigtableMutations =
        byteEncodedKeyedRows.apply(
            ParDo.of(
                new DoFn<KV<byte[], Row>, KV<ByteString, Iterable<Mutation>>>() {
                  @ProcessElement
                  public void process(
                      @DoFn.Element KV<byte[], Row> elm,
                      OutputReceiver<KV<ByteString, Iterable<Mutation>>> receiver) {
                    receiver.output(
                        KV.of(
                            ByteString.copyFrom(elm.getKey()),
                            elm.getValue().getSchema().getFields().stream()
                                .map(
                                    field -> // TODO(pabloem): Pass timestamp for all SetCells
                                    Mutation.newBuilder()
                                            .setSetCell(
                                                Mutation.SetCell.newBuilder()
                                                    .setFamilyName(field.getName())
                                                    .setValue(
                                                        ByteString.copyFrom(
                                                            elm.getValue()
                                                                .getBytes(field.getName())))
                                                    .build())
                                            .build())
                                .collect(Collectors.toList())));
                  }
                }));

    // STEP 4: Write all mutations to BigTable
    PCollection<BigtableWriteResult> btWriteResult =
        bigtableMutations.apply(
            BigtableIO.write()
                .withProjectId(projectId)
                .withInstanceId(instanceId)
                .withTableId(tableId)
                .withWriteResults());

    return PCollectionRowTuple.empty(input.getPipeline());
  }
}
