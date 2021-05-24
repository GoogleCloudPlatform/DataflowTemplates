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

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.vendor.guava.v20_0.com.google.common.io.CharStreams;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Schema utilities for KafkaToBigQuery pipeline. */
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

  /**
   * Gets a proto descriptor.
   *
   * <p>This is similar to {@link SchemaUtils#getAvroSchema(String)} but for protos.
   *
   * @param schemaLocation GCS file location.
   * @param protoFileName Name of the original .proto file that was used to help generate this
   *     schema.
   * @param messageName Name of the message type in the .proto file.
   */
  public static Descriptor getProtoDescriptor(
      String schemaLocation, String protoFileName, String messageName) {
    try {
      FileDescriptorSet fileDescriptorSet =
          FileDescriptorSet.parseFrom(getGcsFileAsBytes(schemaLocation));

      ImmutableMap<String, FileDescriptorProto> knownFiles =
          createNameToFileDescriptorMapping(fileDescriptorSet);
      FileDescriptorProto protoFile = knownFiles.get(protoFileName);
      if (protoFile == null) {
        String msg =
            String.format("Could not find file '%s' in '%s'", protoFileName, schemaLocation);
        LOG.error(msg);
        throw new RuntimeException(msg);
      }

      FileDescriptor fileDescriptor = getFileDescriptor(protoFile, knownFiles);
      if (fileDescriptor == null) {
        String msg =
            String.format(
                "Could not resolve '%s' in '%s', possibly due to missing dependencies.",
                protoFileName, schemaLocation);
        LOG.error(msg);
        throw new RuntimeException(msg);
      }

      Descriptor descriptor = getDescriptor(fileDescriptor, messageName);
      if (descriptor == null) {
        String msg =
            String.format(
                "Could not resolve '%s' generated from '%s' in schema path '%s'",
                messageName, protoFileName, schemaLocation);
        LOG.error(msg);
        throw new RuntimeException(msg);
      }

      return descriptor;
    } catch (InvalidProtocolBufferException e) {
      LOG.error("Could not parse proto contents: " + e.getMessage());
      throw new RuntimeException(e);
    } catch (DescriptorValidationException e) {
      LOG.error("Could not validator descriptor: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a mapping between a file's name and its {@link FileDescriptorProto}.
   *
   * @param descriptorSet the set of {@link FileDescriptorProto} objects
   * @return an {@link ImmutableMap} where the key is the {@link String} representing the name of a
   *     file and the value is the {@link FileDescriptorProto} representing that file.
   */
  private static ImmutableMap<String, FileDescriptorProto> createNameToFileDescriptorMapping(
      FileDescriptorSet descriptorSet) {
    return descriptorSet.getFileList().stream()
        .collect(toImmutableMap(FileDescriptorProto::getName, proto -> proto));
  }

  /** Handles finding the descriptor for {@code messageName} from {@code file}. */
  @Nullable
  private static Descriptor getDescriptor(FileDescriptor file, String messageName) {
    List<String> allNames = Splitter.on('.').splitToList(messageName);
    if (allNames.isEmpty()) {
      return null;
    }

    Descriptor currentDescriptor = file.findMessageTypeByName(allNames.get(0));
    for (int i = 1; currentDescriptor != null && i < allNames.size(); ++i) {
      currentDescriptor = currentDescriptor.findNestedTypeByName(allNames.get(i));
    }

    return currentDescriptor;
  }

  /**
   * Handles creating the {@link FileDescriptor} for {@code root} using {@code knownFiles} to
   * resolve dependencies.
   *
   * @param root The {@link FileDescriptorProto} used as the root in the dependency graph.
   * @param knownFiles All files that are known. This should include, at minimum, all the
   *     dependencies, including transitive dependencies, for {@code root}.
   * @return The {@link FileDescriptor} describing {@code root}.
   * @throws DescriptorValidationException Thrown if there is an issue create a descriptor for
   *     {@code root} or any of its dependencies, including the transitive dependencies.
   */
  @Nullable
  private static FileDescriptor getFileDescriptor(
      FileDescriptorProto root, ImmutableMap<String, FileDescriptorProto> knownFiles)
      throws DescriptorValidationException {
    Map<String, FileDescriptor> resolved = new HashMap<>();
    Deque<FileDescriptorProto> resolutionStack = new ArrayDeque<>();
    resolutionStack.add(root);

    while (!resolutionStack.isEmpty()) {
      FileDescriptorProto current = resolutionStack.peek();
      if (resolved.containsKey(current.getName())) {
        resolutionStack.pop();
        continue;
      }
      if (current.getDependencyCount() == 0) {
        resolved.put(current.getName(), FileDescriptor.buildFrom(current, new FileDescriptor[0]));
        resolutionStack.pop();
        continue;
      }

      int startingStackSize = resolutionStack.size();
      for (String dependency : current.getDependencyList()) {
        if (resolved.containsKey(dependency)) {
          continue;
        }
        if (!knownFiles.containsKey(dependency)) {
          String msg = "Unknown dependency: " + dependency;
          LOG.error(msg);
          throw new RuntimeException(msg);
        }

        // Since protos don't support circular importing, we should be safe in assuming that
        // the dependency isn't already on the resolution stack.
        resolutionStack.push(knownFiles.get(dependency));
      }

      if (resolutionStack.size() == startingStackSize) {
        Set<String> dependencySet = new HashSet<>(current.getDependencyList());
        FileDescriptor descriptor =
            FileDescriptor.buildFrom(
                current,
                resolved.entrySet().stream()
                    .filter(entry -> dependencySet.contains(entry.getKey()))
                    .map(Entry::getValue)
                    .toArray(FileDescriptor[]::new));
        resolved.put(descriptor.getName(), descriptor);
        resolutionStack.pop();
      }
    }

    return resolved.get(root.getName());
  }
}
