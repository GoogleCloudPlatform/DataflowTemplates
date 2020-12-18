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
package com.google.cloud.teleport.v2.utils;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.fromTableSchema;

import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.common.io.ByteStreams;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.Gson;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.JsonArray;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.JsonElement;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@link SchemasUtils} Class to read JSON based schema. Is there available to read from file or
 * from string. Currently supported local File System and GCS.
 */
public class SchemasUtils {

  /* Logger for class.*/
  private static final Logger LOG = LoggerFactory.getLogger(SchemasUtils.class);

  private TableSchema bigQuerySchema;
  private Schema beamSchema;
  private String jsonBeamSchema;

  public SchemasUtils(String schema) {
    parseJson(schema);
  }

  public SchemasUtils(String path, Charset encoding) throws IOException {
    if (path.startsWith("gs://")) {
      parseJson(new String(readGcsFile(path), encoding));
    } else {
      byte[] encoded = Files.readAllBytes(Paths.get(path));
      parseJson(new String(encoded, encoding));
    }
    LOG.info("Extracted schema: " + bigQuerySchema.toPrettyString());
  }

  public TableSchema getBigQuerySchema() {
    return bigQuerySchema;
  }

  private void parseJson(String jsonSchema) throws UnsupportedOperationException {
    TableSchema schema = BigQueryHelpers.fromJsonString(jsonSchema, TableSchema.class);
    validateSchemaTypes(schema);
    bigQuerySchema = schema;
    jsonBeamSchema = BigQueryHelpers.toJsonString(schema.getFields());
  }

  private void validateSchemaTypes(TableSchema bigQuerySchema) {
    try {
      beamSchema = fromTableSchema(bigQuerySchema);
    } catch (UnsupportedOperationException exception) {
      LOG.error("Check json schema, {}", exception.getMessage());
    } catch (NullPointerException npe) {
      LOG.error("Missing schema keywords, please check what all required fields presented");
    }
  }

  /**
   * Method to read a schema file from GCS and return the file contents as a string.
   *
   * @param gcsFilePath path to file in GCS in format "gs://your-bucket/path/to/file"
   * @return byte array with file contents
   * @throws IOException thrown if not able to read file
   */
  public static byte[] readGcsFile(String gcsFilePath)
      throws IOException {
    LOG.info("Reading contents from GCS file: {}", gcsFilePath);
    // Read the GCS file into byte array and will throw an I/O exception in case file not found.
    try (ReadableByteChannel readerChannel =
        FileSystems.open(FileSystems.matchSingleFileSpec(gcsFilePath).resourceId())) {
      try (InputStream stream = Channels.newInputStream(readerChannel)) {
        return ByteStreams.toByteArray(stream);
      }
    }
  }

  public Set<String> getFieldsToTokenize(String payloadConfigGcsPath) {
    Set<String> fieldsToTokenize = new HashSet<>();
    try {
      String rawJsonWithFieldsToTokenize = new String(
          readGcsFile(payloadConfigGcsPath), Charset.defaultCharset());
      Gson gson = new Gson();
      JsonArray jsonTokenizedRows = gson
          .fromJson(rawJsonWithFieldsToTokenize, JsonObject.class)
          .getAsJsonArray("fields");
      for (JsonElement element : jsonTokenizedRows) {
        fieldsToTokenize.add(element.getAsString());
      }
    } catch (IOException | NullPointerException exception) {
      LOG.error(
          "Cant parse fields to tokenize, or input parameter payloadConfigGcsPath was not specified."
              + " All fields will be sent to the protectors");
      fieldsToTokenize = this.getBeamSchema().getFields().stream().map(Field::getName).collect(
          Collectors.toSet());
    }
    return fieldsToTokenize;
  }

  public Schema getBeamSchema() {
    return beamSchema;
  }

  public String getJsonBeamSchema() {
    return jsonBeamSchema;
  }
}
