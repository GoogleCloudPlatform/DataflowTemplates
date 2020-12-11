/*
 * Copyright (C) 2020 Google Inc.
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
package com.google.cloud.teleport.v2.transforms;

import static org.apache.beam.sdk.util.RowJsonUtils.rowToJson;
import static org.apache.beam.vendor.grpc.v1p26p0.com.google.common.base.MoreObjects.firstNonNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.transforms.io.BigQueryIO;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.RowJson;
import org.apache.beam.sdk.util.RowJsonUtils;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class for data tokenization using DSG.
 */
public class DSGTokenization extends DoFn<Row, Row> {

  /**
   * Logger for class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryIO.class);

  private static Schema schemaToDsg;
  private static CloseableHttpClient httpclient;
  private static ObjectMapper objectMapperForDSG;

  private final Schema schema;
  private final int batchSize;
  private final Iterable<String> fieldsToTokenize;
  private final String dsgURI;

  @StateId("buffer")
  private final StateSpec<BagState<Row>> bufferedEvents = StateSpecs.bag();

  @StateId("count")
  private final StateSpec<ValueState<Integer>> countState = StateSpecs.value();

  private DSGTokenization(Schema schema, int batchSize, Iterable<String> fieldsToTokenize,
      String dsgURI) {
    this.schema = schema;
    this.batchSize = batchSize;
    this.fieldsToTokenize = fieldsToTokenize;
    this.dsgURI = dsgURI;
  }

  @Setup
  public void setup() {

    ArrayList<Schema.Field> fields = new ArrayList<>();
    for (String field : fieldsToTokenize) {
      fields.add(schema.getField(field));
    }
    schemaToDsg = new Schema(fields);
    objectMapperForDSG = RowJsonUtils
        .newObjectMapperWith(RowJson.RowJsonSerializer.forSchema(schemaToDsg));

    httpclient = HttpClients.createDefault();
  }

  @Teardown
  public void close() {
    try {
      httpclient.close();
    } catch (IOException exception) {
      LOG.warn("Can't close connection: {}", exception.getMessage());
    }

  }

  @ProcessElement
  public void process(
      ProcessContext context,
      @StateId("buffer") BagState<Row> bufferState,
      @StateId("count") ValueState<Integer> countState) throws IOException {

    int count = firstNonNull(countState.read(), 0);
    count = count + 1;
    countState.write(count);
    bufferState.add(context.element());

    if (count >= batchSize) {
      for (Row outputRow : getTokenizedRow(bufferState.read())) {
        context.output(outputRow);
      }
      bufferState.clear();
      countState.clear();
    }
  }

  private ArrayList<Row> getTokenizedRow(Iterable<Row> inputRows)
      throws IOException { //TODO catch that
    ArrayList<Row> outputRows = new ArrayList<>();

    for (Row inputRow : inputRows) {

      Row.Builder builder = Row.withSchema(schemaToDsg);
      for (Schema.Field field : schemaToDsg.getFields()) {
        builder.withFieldValue(field.getName(), inputRow.getValue(field.getName()));
      }
      CloseableHttpResponse response = sendToDsg(
          rowToJson(objectMapperForDSG, builder.build()).getBytes());

      String tokenizedData = IOUtils
          .toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
      Row tokenizedRow = RowJsonUtils.jsonToRow(objectMapperForDSG, tokenizedData);
      Row.FieldValueBuilder rowBuilder = Row.fromRow(inputRow);
      for (Schema.Field field : schemaToDsg.getFields()) {
        rowBuilder.withFieldValue(field.getName(), tokenizedRow.getValue(field.getName()));
      }
      outputRows.add(rowBuilder.build());
    }

    return outputRows;

  }

  private CloseableHttpResponse sendToDsg(byte[] data) throws IOException {
    HttpPost httpPost = new HttpPost(dsgURI);
    HttpEntity stringEntity = new ByteArrayEntity(data, ContentType.APPLICATION_JSON);
    httpPost.setEntity(stringEntity);
    return httpclient.execute(httpPost);
  }
}

