/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.transforms;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Common transforms for Json files. The implementation is inspired by {@link CsvConverters}. */
public final class JsonConverters {

  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(JsonConverters.class);

  /**
   * The {@link JsonConverters.ReadJson} class is a {@link PTransform} that reads JSON records from
   * text files. The transform returns {@link PCollection} of json records.
   */
  @AutoValue
  public abstract static class ReadJson extends PTransform<PBegin, PCollection<String>> {

    public static JsonConverters.ReadJson.Builder newBuilder() {
      return new AutoValue_JsonConverters_ReadJson.Builder();
    }

    public abstract String inputFileSpec();

    @Override
    public PCollection<String> expand(PBegin input) {
      return input.apply("ReadJSONRecords", TextIO.read().from(inputFileSpec()));
    }

    /** Builder for {@link JsonConverters.ReadJson}. */
    @AutoValue.Builder
    public abstract static class Builder {

      public abstract JsonConverters.ReadJson.Builder setInputFileSpec(String inputFileSpec);

      abstract JsonConverters.ReadJson autoBuild();

      public JsonConverters.ReadJson build() {
        JsonConverters.ReadJson readJson = autoBuild();
        Objects.requireNonNull(readJson.inputFileSpec(), "Input file spec must be provided.");
        return readJson;
      }
    }
  }

  /**
   * The {@link JsonConverters.StringToGenericRecordFn} class takes in a Json record string as input
   * and outputs a {@link GenericRecord}.
   */
  public static class StringToGenericRecordFn extends DoFn<String, GenericRecord> {

    private final String serializedSchema;
    private Schema schema;

    public StringToGenericRecordFn(String serializedSchema) {
      this.serializedSchema = serializedSchema;
    }

    @Setup
    public void setup() {
      schema = SchemaUtils.parseAvroSchema(serializedSchema);
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IllegalArgumentException {
      String jsonRecord =
          Objects.requireNonNull(context.element(), "The Json record can't be null");
      Object result;
      try {
        result = jsonElementToAvro(JsonParser.parseString(jsonRecord), schema);
      } catch (JsonSchemaMismatchException e) {
        String message =
            String.format("Json record `%s` doesn't match Avro schema `%s`.", jsonRecord, schema);
        LOG.error(message);
        throw new IllegalArgumentException(message, e);
      }
      if (!(result instanceof GenericRecord)) {
        throw new IllegalArgumentException(
            String.format(
                "The Json record `%s` must convert to GenericRecord instance with Avro schema"
                    + " `%s`.",
                jsonRecord, schema));
      }
      context.output((GenericRecord) result);
    }
  }

  @Nullable
  private static Object jsonElementToAvro(@Nullable JsonElement element, Schema schema)
      throws JsonSchemaMismatchException {
    boolean schemaIsNullable = isNullable(schema);
    if (schemaIsNullable) {
      schema = typeFromNullable(schema);
    }
    if (element == null || element.isJsonNull()) {
      if (!schemaIsNullable) {
        throw new JsonSchemaMismatchException("The element is not nullable in Avro schema.");
      }
      return null;
    } else if (element.isJsonObject()) {
      if (schema.getType() != Schema.Type.RECORD) {
        throw new JsonSchemaMismatchException(
            String.format("The element `%s` doesn't match Avro type RECORD", element));
      }
      return jsonObjectToAvro(element.getAsJsonObject(), schema);
    } else if (element.isJsonArray()) {
      if (schema.getType() != Schema.Type.ARRAY) {
        throw new JsonSchemaMismatchException(
            String.format("The element `%s` doesn't match Avro type ARRAY", element));
      }
      JsonArray jsonArray = element.getAsJsonArray();
      List<Object> avroArray = new ArrayList<>(jsonArray.size());
      for (JsonElement e : element.getAsJsonArray()) {
        avroArray.add(jsonElementToAvro(e, schema.getElementType()));
      }
      return avroArray;
    } else if (element.isJsonPrimitive()) {
      return jsonPrimitiveToAvro(element.getAsJsonPrimitive(), schema);
    } else {
      throw new JsonSchemaMismatchException(
          String.format(
              "The Json element `%s` is of an unknown class %s", element, element.getClass()));
    }
  }

  public static GenericRecord jsonObjectToAvro(JsonObject jsonObject, Schema schema)
      throws JsonSchemaMismatchException {
    GenericRecord avroRecord = new GenericData.Record(schema);
    for (Schema.Field field : schema.getFields()) {
      avroRecord.put(field.name(), jsonElementToAvro(jsonObject.get(field.name()), field.schema()));
    }
    return avroRecord;
  }

  private static Object jsonPrimitiveToAvro(JsonPrimitive primitive, Schema schema)
      throws JsonSchemaMismatchException {
    switch (schema.getType()) {
      case NULL:
        return null;
      case STRING:
        return primitive.getAsString();
      case BOOLEAN:
        return primitive.getAsBoolean();
      case INT:
        return primitive.getAsInt();
      case LONG:
        return primitive.getAsLong();
      case FLOAT:
        return primitive.getAsFloat();
      case DOUBLE:
        return primitive.getAsDouble();
      default:
        throw new JsonSchemaMismatchException(
            String.format("The Avro type %s is not supported", schema.getType()));
    }
  }

  /**
   * This method is equivalent to {@code Schema.isNullable()} of Avro 1.9.
   *
   * <p>Normally nullables in Avro are represented as a `union`: `"type": ["null", "some type"]}`.
   * But the implementation of {@code Schema.isNullable()} in Avro 1.9 suggests that the unions
   * could potentially be nested, so this implementation also handles that.
   */
  private static boolean isNullable(Schema type) {
    return type.getType() == Schema.Type.NULL
        || type.getType() == Schema.Type.UNION
            && type.getTypes().stream().anyMatch(JsonConverters::isNullable);
  }

  private static Schema typeFromNullable(Schema type) {
    if (type.getType() == Schema.Type.UNION) {
      return typeFromNullable(
          type.getTypes().stream()
              .filter(t -> t.getType() != Schema.Type.NULL)
              .findFirst()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          String.format("Type `%s` should have a non null subtype", type))));
    }
    return type;
  }

  private static class JsonSchemaMismatchException extends Exception {
    JsonSchemaMismatchException(String message) {
      super(message);
    }
  }
}
