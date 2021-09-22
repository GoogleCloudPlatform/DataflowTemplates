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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
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
        checkArgument(readJson.inputFileSpec() != null, "Input file spec must be provided.");
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
      GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
      String json = context.element();
      try {
        context.output(reader.read(null, DecoderFactory.get().jsonDecoder(schema, json)));
      } catch (IOException e) {
        String message =
            "Error while converting json record `"
                + json
                + "` according to schema `"
                + schema
                + "`.";
        LOG.error(message, e);
        throw new RuntimeException(message, e);
      }
    }
  }
}
