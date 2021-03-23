package com.google.cloud.teleport.v2.transforms;

import static org.apache.beam.sdk.util.RowJsonUtils.rowToJson;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.transforms.io.BigQueryIO;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.RowJson;
import org.apache.beam.sdk.util.RowJsonUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.Gson;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.JsonArray;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.JsonObject;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link ProtegrityDataProtectors} Using passing parameters transform will buffer input rows in
 * batch and will send it when the count of buffered rows will equal specified batch size. When it
 * takes the last one batch, it will send it when the last row will come to doFn even count of
 * buffered rows will less than the batch size.
 */
public class ProtegrityDataProtectors {

  /**
   * The {@link ProtegrityDataProtectors.RowToTokenizedRow} transform converts {@link Row} to {@link
   * TableRow} objects. The transform accepts a {@link FailsafeElement} object so the original
   * payload of the incoming record can be maintained across multiple series of transforms.
   *
   * Environment variables:
   * MAX_BUFFERING_DURATION_MS - Max duration (in milliseconds) of buffering rows in {@link GroupIntoBatches}
   */
  @AutoValue
  public abstract static class RowToTokenizedRow<T>
      extends PTransform<PCollection<KV<Integer, Row>>, PCollectionTuple> {

    private static final Long MAX_BUFFERING_DURATION_MS =
        Long.valueOf(System.getenv().getOrDefault("MAX_BUFFERING_DURATION_MS", "100"));

    public static <T> RowToTokenizedRow.Builder<T> newBuilder() {
      return new AutoValue_ProtegrityDataProtectors_RowToTokenizedRow.Builder<>();
    }

    public abstract TupleTag<Row> successTag();

    public abstract TupleTag<FailsafeElement<Row, Row>> failureTag();

    public abstract Schema schema();

    public abstract int batchSize();

    public abstract Map<String, String> dataElements();

    public abstract String serviceAccount();

    public abstract String dsgURI();

    @Override
    public PCollectionTuple expand(PCollection<KV<Integer, Row>> inputRows) {
      FailsafeElementCoder<Row, Row> coder = FailsafeElementCoder.of(
          RowCoder.of(schema()),
          RowCoder.of(schema())
      );

      Duration maxBuffering = Duration.millis(MAX_BUFFERING_DURATION_MS);
      PCollectionTuple pCollectionTuple = inputRows
          .apply("GroupRowsIntoBatches",
              GroupIntoBatches.<Integer, Row>ofSize(batchSize())
                  .withMaxBufferingDuration(maxBuffering))
          .apply("TokenizeUsingDsg",
              ParDo.of(new DSGTokenizationFn(schema(), dataElements(), serviceAccount(),
                  dsgURI(), failureTag()))
                  .withOutputTags(successTag(), TupleTagList.of(failureTag())));

      return PCollectionTuple
          .of(successTag(), pCollectionTuple.get(successTag()).setRowSchema(schema()))
          .and(failureTag(), pCollectionTuple.get(failureTag()).setCoder(coder));
    }

    /**
     * Builder for {@link ProtegrityDataProtectors.RowToTokenizedRow}.
     */
    @AutoValue.Builder
    public abstract static class Builder<T> {

      public abstract RowToTokenizedRow.Builder<T> setSuccessTag(TupleTag<Row> successTag);

      public abstract RowToTokenizedRow.Builder<T> setFailureTag(
          TupleTag<FailsafeElement<Row, Row>> failureTag);

      public abstract RowToTokenizedRow.Builder<T> setSchema(Schema schema);

      public abstract RowToTokenizedRow.Builder<T> setBatchSize(int batchSize);

      public abstract RowToTokenizedRow.Builder<T> setDataElements(
          Map<String, String> fieldsDataElements);

      public abstract RowToTokenizedRow.Builder<T> setServiceAccount(String serviceAccount);

      public abstract RowToTokenizedRow.Builder<T> setDsgURI(String dsgURI);

      public abstract RowToTokenizedRow<T> build();
    }


  }

  /**
   * Class implements stateful doFn for data tokenization using remote DSG.
   */
  public static class DSGTokenizationFn extends DoFn<KV<Integer, Iterable<Row>>, Row> {

    /**
     * Logger for class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryIO.class);
    public static final String ID_TOKEN_NAME = "ID";

    private static Schema schemaToDsg;
    private static CloseableHttpClient httpclient;
    private static ObjectMapper objectMapperSerializerForDSG;
    private static ObjectMapper objectMapperDeserializerForDSG;

    private final Schema schema;
    private final Map<String, String> dataElements;
    private final String serviceAccount;
    private final String dsgURI;
    private final TupleTag<FailsafeElement<Row, Row>> failureTag;

    private boolean hasIdInInputs = true;
    private String idFieldName;
    private Map<String, Row> inputRowsWithIds;


    public DSGTokenizationFn(Schema schema, Map<String, String> dataElements,
        String serviceAccount,
        String dsgURI,
        TupleTag<FailsafeElement<Row, Row>> failureTag) {
      this.schema = schema;
      this.dataElements = dataElements;
      this.serviceAccount = serviceAccount;
      this.dsgURI = dsgURI;
      this.failureTag = failureTag;
    }

    @Setup
    public void setup() {

      List<String> idFieldList = dataElements.entrySet().stream()
          .filter(map -> ID_TOKEN_NAME.equals(map.getValue()))
          .map(Entry::getKey)
          .collect(Collectors.toList());

      // If we have more than 1 ID fields, we will choose the first.
      if (idFieldList.size() > 0) {
        idFieldName = idFieldList.get(0);
      }

      if (idFieldName == null || !schema.hasField(idFieldName)) {
        this.hasIdInInputs = false;
      }

      ArrayList<Schema.Field> fields = new ArrayList<>();
      for (String field : dataElements.keySet()) {
        if (schema.hasField(field)) {
          fields.add(schema.getField(field));
        }
      }
      if (!hasIdInInputs) {
        idFieldName = ID_TOKEN_NAME;
        fields.add(Field.of(ID_TOKEN_NAME, FieldType.STRING));
        dataElements.put(ID_TOKEN_NAME, ID_TOKEN_NAME);
      }
      schemaToDsg = new Schema(fields);
      objectMapperSerializerForDSG = RowJsonUtils
          .newObjectMapperWith(RowJson.RowJsonSerializer.forSchema(schemaToDsg));

      objectMapperDeserializerForDSG = RowJsonUtils
          .newObjectMapperWith(RowJson.RowJsonDeserializer.forSchema(schemaToDsg));

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
    public void process(@Element KV<Integer, Iterable<Row>> element,
        ProcessContext context) {
      Iterable<Row> rows = element.getValue();

      try {
        for (Row outputRow : getTokenizedRow(rows)) {
          context.output(outputRow);
        }
      } catch (Exception e) {
        for (Row outputRow : rows) {
          context.output(
              failureTag,
              FailsafeElement.of(outputRow, outputRow)
                  .setErrorMessage(e.getMessage())
                  .setStacktrace(Throwables.getStackTraceAsString(e)));
        }
      }
    }

    private ArrayList<String> rowsToJsons(Iterable<Row> inputRows) {
      ArrayList<String> jsons = new ArrayList<>();
      Map<String, Row> inputRowsWithIds = new HashMap<>();
      for (Row inputRow : inputRows) {

        Row.Builder builder = Row.withSchema(schemaToDsg);
        for (Schema.Field field : schemaToDsg.getFields()) {
          if (inputRow.getSchema().hasField(field.getName())) {
            builder = builder.addValue(inputRow.getValue(field.getName()));
          }
        }
        String id;
        if (!hasIdInInputs) {
          id = UUID.randomUUID().toString();
          builder = builder.addValue(id);
        } else {
          id = inputRow.getValue(idFieldName);
        }
        inputRowsWithIds.put(id, inputRow);

        Row row = builder
            .build();

        jsons.add(rowToJson(objectMapperSerializerForDSG, row));
      }
      this.inputRowsWithIds = inputRowsWithIds;
      return jsons;
    }

    private String formatJsonsToDsgBatch(Iterable<String> jsons) {
      StringBuilder stringBuilder = new StringBuilder(String.join(",", jsons));
      Gson gson = new Gson();
      Type gsonType = new TypeToken<HashMap<String, String>>() {
      }.getType();
      String dataElementsJson = gson.toJson(dataElements, gsonType);
      String serviceAccountJson = gson.toJson(serviceAccount);
      stringBuilder.append("]").insert(0, "{\"data\": [")
          .append(",\"data_elements\":").append(dataElementsJson)
          .append(",\"service_account\":").append(serviceAccountJson)
          .append("}");
      return stringBuilder.toString();
    }

    private ArrayList<Row> getTokenizedRow(Iterable<Row> inputRows)
        throws IOException {
      ArrayList<Row> outputRows = new ArrayList<>();

      CloseableHttpResponse response = sendToDsg(
          formatJsonsToDsgBatch(rowsToJsons(inputRows)).getBytes());

      String tokenizedData = IOUtils
          .toString(response.getEntity().getContent(), StandardCharsets.UTF_8);

      Gson gson = new Gson();
      JsonArray jsonTokenizedRows = gson
          .fromJson(tokenizedData, JsonObject.class)
          .getAsJsonArray("data");

      for (int i = 0; i < jsonTokenizedRows.size(); i++) {
        Row tokenizedRow = RowJsonUtils
            .jsonToRow(objectMapperDeserializerForDSG, jsonTokenizedRows.get(i).toString());
        Row.FieldValueBuilder rowBuilder = Row
            .fromRow(this.inputRowsWithIds.get(tokenizedRow.getString(idFieldName)));
        for (Schema.Field field : schemaToDsg.getFields()) {
          if (!hasIdInInputs && field.getName().equals(idFieldName)) {
            continue;
          }
          rowBuilder = rowBuilder
              .withFieldValue(field.getName(), tokenizedRow.getValue(field.getName()));

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
}


