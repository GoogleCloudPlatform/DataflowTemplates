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
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
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
import org.joda.time.Instant;

public class BigTableIOWriteSchemaBasedTransform
    extends PTransform<PCollectionRowTuple, PCollectionRowTuple> implements SchemaTransform {

  private static final String INPUT_TAG = "INPUT";

  private final String projectId;
  private final String instanceId;
  private final String tableId;
  private final String bigTableEndpoint;
  private final List<String> keyColumns;
  private final Instant timestampForRows;
  private final String appProfileId;

  BigTableIOWriteSchemaBasedTransform(
      String projectId,
      String instanceId,
      String tableId,
      List<String> keyColumns,
      String bigTableEndpoint,
      String appProfileId) {
    this.projectId = projectId;
    this.instanceId = instanceId;
    this.tableId = tableId;
    this.keyColumns = keyColumns;
    this.bigTableEndpoint = bigTableEndpoint;
    this.timestampForRows = Instant.now();
    this.appProfileId = appProfileId;
  }

  @Override
  public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
    return this;
  }

  public BigtableTableAdminClient bigtableTableAdminClient() throws IOException {
    BigtableTableAdminSettings.Builder settingsBuilder;
    if (this.bigTableEndpoint != null && !this.bigTableEndpoint.isEmpty()) {
      settingsBuilder =
          BigtableTableAdminSettings.newBuilderForEmulator(
                  Integer.parseInt(bigTableEndpoint.split(":")[1]))
              .setInstanceId(instanceId)
              .setProjectId(projectId);
      settingsBuilder.stubSettings().setEndpoint(bigTableEndpoint);
    } else {
      settingsBuilder =
          BigtableTableAdminSettings.newBuilder().setInstanceId(instanceId).setProjectId(projectId);
    }
    return BigtableTableAdminClient.create(settingsBuilder.build());
  }

  private void createTableIfNeeded(Schema inputSchema) {
    // TODO(pabloem): What happens if we don't have privileges to create the table?
    try (BigtableTableAdminClient client = bigtableTableAdminClient()) {
      CreateTableRequest createTableRequest = CreateTableRequest.of(tableId);
      inputSchema.getFields().forEach(field -> createTableRequest.addFamily(field.getName()));
      client.createTable(createTableRequest);
    } catch (IOException e) {
      // TODO(pabloem): HANDLE THIS POSSIBILITY
    }
  }

  private void verifyTableSchemaMatches(Schema inputSchema) {
    // TODO(pabloem): What happens if we don't have privileges to create the table?
    try (BigtableTableAdminClient client = bigtableTableAdminClient()) {
      Table table = client.getTable(tableId);
      Set<String> columnFamilies =
          table.getColumnFamilies().stream().map(cf -> cf.getId()).collect(Collectors.toSet());
      Set<String> inputColumns =
          inputSchema.getFields().stream()
              .map(field -> field.getName())
              .collect(Collectors.toSet());

      // All columns in the input must exist in BigTable, and they must be the same size
      // TODO(pabloem): Do we support cases where BigTable column families is a SUPERSET of BQ
      // columns?
      // TODO(pabloem): Add a test case for this.
      if (!(columnFamilies.containsAll(inputColumns)
          && columnFamilies.size() == inputSchema.getFields().size())) {
        throw new IllegalArgumentException(
            String.format(
                "Unable to match input schema with the columns of the destination "
                    + "table in Bigtable. Fields missing in BigTable: %s.",
                inputColumns.removeAll(columnFamilies)));
      }
    } catch (IOException e) {
      // TODO(pabloem): HANDLE THIS POSSIBILITY
    }
  }

  @Override
  public PCollectionRowTuple expand(PCollectionRowTuple input) {
    PCollection<Row> inputData = input.get(INPUT_TAG);

    createTableIfNeeded(inputData.getSchema());
    verifyTableSchemaMatches(inputData.getSchema());

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
                                    field ->
                                        Mutation.newBuilder()
                                            .setSetCell(
                                                Mutation.SetCell.newBuilder()
                                                    .setFamilyName(field.getName())
                                                    .setTimestampMicros(
                                                        timestampForRows.getMillis() * 1000)
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
    BigtableIO.Write btWrite =
        BigtableIO.write().withProjectId(projectId).withInstanceId(instanceId).withTableId(tableId);

    if (appProfileId != null && !appProfileId.isEmpty()) {
      btWrite =
          btWrite.withBigtableOptions(
              BigtableOptions.builder().setAppProfileId(appProfileId).build());
    }
    PCollection<BigtableWriteResult> btWriteResult =
        bigtableMutations.apply(
            bigTableEndpoint == null || bigTableEndpoint.isEmpty()
                ? btWrite.withWriteResults()
                : btWrite.withEmulator(bigTableEndpoint).withWriteResults());

    return PCollectionRowTuple.empty(input.getPipeline());
  }
}
