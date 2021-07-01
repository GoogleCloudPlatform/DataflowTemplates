/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.teleport.v2.utils;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.beam.sdk.extensions.protobuf.ProtoDomain;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.vendor.guava.v20_0.com.google.common.io.CharStreams;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities for working with different schemas. */
public class SchemaUtils {

  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(SchemaUtils.class);

  public static final String DEADLETTER_SCHEMA =
      "{\n"
          + "  \"fields\": [\n"
          + "    {\n"
          + "      \"name\": \"timestamp\",\n"
          + "      \"type\": \"TIMESTAMP\",\n"
          + "      \"mode\": \"REQUIRED\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"payloadString\",\n"
          + "      \"type\": \"STRING\",\n"
          + "      \"mode\": \"REQUIRED\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"payloadBytes\",\n"
          + "      \"type\": \"BYTES\",\n"
          + "      \"mode\": \"REQUIRED\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"attributes\",\n"
          + "      \"type\": \"RECORD\",\n"
          + "      \"mode\": \"REPEATED\",\n"
          + "      \"fields\": [\n"
          + "        {\n"
          + "          \"name\": \"key\",\n"
          + "          \"type\": \"STRING\",\n"
          + "          \"mode\": \"NULLABLE\"\n"
          + "        },\n"
          + "        {\n"
          + "          \"name\": \"value\",\n"
          + "          \"type\": \"STRING\",\n"
          + "          \"mode\": \"NULLABLE\"\n"
          + "        }\n"
          + "      ]\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"errorMessage\",\n"
          + "      \"type\": \"STRING\",\n"
          + "      \"mode\": \"NULLABLE\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"stacktrace\",\n"
          + "      \"type\": \"STRING\",\n"
          + "      \"mode\": \"NULLABLE\"\n"
          + "    }\n"
          + "  ]\n"
          + "}";

  // TODO(zhoufek): Move GCS-reading methods to a separate class, since they aren't schema-related

  /**
   * The {@link SchemaUtils#getGcsFileAsString(String)} reads a file from GCS and returns it as a
   * string.
   *
   * @param filePath path to file in GCS
   * @return contents of the file as a string
   * @throws RuntimeException thrown if not able to read or parse file
   */
  public static String getGcsFileAsString(String filePath) {
    ReadableByteChannel channel = getGcsFileByteChannel(filePath);
    try (Reader reader = Channels.newReader(channel, UTF_8.name())) {
      return CharStreams.toString(reader);
    } catch (IOException e) {
      LOG.error("Error parsing file contents into string: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  /**
   * Similar to {@link SchemaUtils#getGcsFileAsString(String)}, but it gets the raw bytes rather
   * than encoding them into a string.
   *
   * @param filePath path to file in GCS
   * @return raw contents of file
   * @throws RuntimeException thrown if not able to read or parse file
   */
  public static byte[] getGcsFileAsBytes(String filePath) {
    ReadableByteChannel channel = getGcsFileByteChannel(filePath);
    try (InputStream inputStream = Channels.newInputStream(channel)) {
      return IOUtils.toByteArray(inputStream);
    } catch (IOException e) {
      LOG.error("Error parsing file into bytes: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  /** Handles getting the {@link ReadableByteChannel} for {@code filePath}. */
  private static ReadableByteChannel getGcsFileByteChannel(String filePath) {
    try {
      MatchResult result = FileSystems.match(filePath);
      checkArgument(
          result.status() == MatchResult.Status.OK && !result.metadata().isEmpty(),
          "Failed to match any files with the pattern: " + filePath);

      List<ResourceId> rId =
          result.metadata().stream()
              .map(MatchResult.Metadata::resourceId)
              .collect(Collectors.toList());

      checkArgument(rId.size() == 1, "Expected exactly 1 file, but got " + rId.size() + " files.");

      return FileSystems.open(rId.get(0));
    } catch (IOException ioe) {
      LOG.error("File system i/o error: " + ioe.getMessage());
      throw new RuntimeException(ioe);
    }
  }

  /**
   * The {@link SchemaUtils#getAvroSchema(String)} reads an Avro schema file from GCS, parses it and
   * returns a new {@link Schema} object.
   *
   * @param schemaLocation Path to schema (e.g. gs://mybucket/path/).
   * @return {@link Schema}
   */
  public static Schema getAvroSchema(String schemaLocation) {
    String schemaFile = getGcsFileAsString(schemaLocation);
    return new Schema.Parser().parse(schemaFile);
  }

  /**
   * The {@link SchemaUtils#parseAvroSchema(String)} parses schema text and returns a new {@link
   * Schema} object.
   *
   * <p>e.g. avro schema text { "namespace": "com.google.cloud.teleport.v2.packagename", "type":
   * "record", "name": "typeName", "fields": [ {"name": "field1","type": "string"}, {"name":
   * "field2", "type": "long"}, {"name": "nestedfield", "type": {"type": "map", "values":
   * "string"}}, ] }
   *
   * @param avroschema avroschema in text format
   * @return {@link Schema}
   */
  public static Schema parseAvroSchema(@Nonnull String avroschema) {
    checkArgument(
        avroschema != null,
        "avroschema argument to SchemaUtils.parseAvroSchema method cannot be null.");
    return new Schema.Parser().parse(avroschema);
  }

  /** Returns a {@link ProtoDomain} containing all the protos found in {@code schemaFileName}. */
  public static ProtoDomain getProtoDomain(String schemaFileName) {
    try {
      FileDescriptorSet descriptorSet =
          FileDescriptorSet.parseFrom(SchemaUtils.getGcsFileAsBytes(schemaFileName));

      return ProtoDomain.buildFrom(descriptorSet);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("Schema file is not valid.", e);
    }
  }
}
