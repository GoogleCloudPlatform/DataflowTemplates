/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.schemautils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Registry that maps column family/qualifier pairs to {@link ValueTransformer} instances.
 *
 * <p>The configuration string is a comma-separated list of entries with the format:
 *
 * <pre>column_family:column_qualifier:TRANSFORM_TYPE</pre>
 *
 * <p>Supported TRANSFORM_TYPE values:
 *
 * <ul>
 *   <li>{@code BIG_ENDIAN_TIMESTAMP} - interprets 8-byte big-endian values as Unix epoch millis
 *   <li>{@code PROTO_DECODE(package.MessageName)} - decodes protobuf-encoded values to JSON
 *       (requires {@code protoSchemaPath})
 * </ul>
 *
 * <p><b>Note:</b> Column qualifiers containing commas are not supported since comma is used as the
 * entry delimiter.
 */
public class ValueTransformerRegistry implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Two-level map: family -> column -> transformer. */
  private final Map<String, Map<String, ValueTransformer>> transformers;

  private ValueTransformerRegistry(Map<String, Map<String, ValueTransformer>> transformers) {
    this.transformers = transformers;
  }

  /**
   * Creates a registry from a flat map keyed by "family:column". Package-private for testing.
   *
   * @param flat map from "family:column" to transformer
   * @return a registry backed by the converted two-level map
   */
  static ValueTransformerRegistry of(Map<String, ValueTransformer> flat) {
    Map<String, Map<String, ValueTransformer>> twoLevel = new HashMap<>();
    flat.forEach(
        (key, transformer) -> {
          int colon = key.indexOf(':');
          String family = key.substring(0, colon);
          String column = key.substring(colon + 1);
          twoLevel.computeIfAbsent(family, k -> new HashMap<>()).put(column, transformer);
        });
    return new ValueTransformerRegistry(twoLevel);
  }

  /**
   * Returns the transformer for the given column family and qualifier, or null if none is
   * registered.
   */
  @Nullable
  public ValueTransformer getTransformer(String columnFamily, String columnQualifier) {
    Map<String, ValueTransformer> familyMap = transformers.get(columnFamily);
    return familyMap != null ? familyMap.get(columnQualifier) : null;
  }

  /**
   * Applies the registered transformer for the given column with an upper bound on the decoded
   * output size, delegating the per-transformer bounded semantics to {@link
   * ValueTransformer#transformBounded(byte[], long)}. Returns {@link
   * TransformResult.Status#NO_TRANSFORMER} when no transformer is registered so callers can fall
   * back to the raw cell value.
   *
   * @param columnFamily Bigtable column family
   * @param columnQualifier Bigtable column qualifier (UTF-8 string form)
   * @param bytes raw cell value bytes
   * @param maxBytes maximum allowed UTF-8 byte size of the decoded output; {@code <= 0} disables
   *     the bound
   * @return a {@link TransformResult} describing the outcome; never null
   */
  public TransformResult transformBounded(
      String columnFamily, String columnQualifier, byte[] bytes, long maxBytes) {
    ValueTransformer transformer = getTransformer(columnFamily, columnQualifier);
    return transformer == null
        ? TransformResult.noTransformer()
        : transformer.transformBounded(bytes, maxBytes);
  }

  /**
   * Parses a column transforms configuration string without proto support.
   *
   * @param config comma-separated list of {@code column_family:column:TRANSFORM_TYPE} entries, or
   *     null/empty for no transforms
   * @return a registry with the configured transformers
   */
  public static ValueTransformerRegistry parse(@Nullable String config) {
    return parse(config, null, false);
  }

  /**
   * Parses a column transforms configuration string.
   *
   * <p>The format is a comma-separated list of entries:
   *
   * <pre>column_family:column_qualifier:TRANSFORM_TYPE</pre>
   *
   * <p><b>Note:</b> Column qualifiers containing commas are not supported since comma is used as
   * the entry delimiter.
   *
   * @param config comma-separated list of {@code column_family:column:TRANSFORM_TYPE} entries, or
   *     null/empty for no transforms
   * @param protoSchemaPath GCS path to the proto schema file, required for PROTO_DECODE transforms
   * @param preserveProtoFieldNames whether to preserve proto field names in JSON output
   * @return a registry with the configured transformers
   */
  public static ValueTransformerRegistry parse(
      @Nullable String config, @Nullable String protoSchemaPath, boolean preserveProtoFieldNames) {
    Map<String, Map<String, ValueTransformer>> transformers = new HashMap<>();

    if (config == null || config.trim().isEmpty()) {
      return new ValueTransformerRegistry(transformers);
    }

    // Cache so that identical type strings share transformer instances.
    Map<String, ValueTransformer> transformerCache = new HashMap<>();

    String[] entries = config.split(",");
    for (String entry : entries) {
      String trimmed = entry.trim();
      if (trimmed.isEmpty()) {
        continue;
      }

      int firstColon = trimmed.indexOf(':');
      int lastColon = trimmed.lastIndexOf(':');
      if (firstColon == -1 || firstColon == lastColon) {
        throw new IllegalArgumentException(
            "Invalid columnTransforms entry '"
                + trimmed
                + "'. Expected format: column_family:column:TRANSFORM_TYPE");
      }
      String family = trimmed.substring(0, firstColon);
      String column = trimmed.substring(firstColon + 1, lastColon);
      String type = trimmed.substring(lastColon + 1);

      ValueTransformer transformer =
          transformerCache.computeIfAbsent(
              type, t -> createTransformer(t, protoSchemaPath, preserveProtoFieldNames));
      transformers.computeIfAbsent(family, k -> new HashMap<>()).put(column, transformer);
    }

    return new ValueTransformerRegistry(transformers);
  }

  private static ValueTransformer createTransformer(
      String type, @Nullable String protoSchemaPath, boolean preserveProtoFieldNames) {
    if ("BIG_ENDIAN_TIMESTAMP".equals(type)) {
      return new BigEndianTimestampTransformer();
    }

    if (type.startsWith("PROTO_DECODE(") && type.endsWith(")")) {
      String messageName = type.substring("PROTO_DECODE(".length(), type.length() - 1);
      if (messageName.isEmpty()) {
        throw new IllegalArgumentException(
            "PROTO_DECODE requires a message name, e.g. PROTO_DECODE(package.MessageName)");
      }
      if (protoSchemaPath == null || protoSchemaPath.isEmpty()) {
        throw new IllegalArgumentException(
            "PROTO_DECODE transform requires protoSchemaPath to be set");
      }
      return new ProtoDecodeTransformer(protoSchemaPath, messageName, preserveProtoFieldNames);
    }

    throw new IllegalArgumentException(
        "Unknown TRANSFORM_TYPE '"
            + type
            + "'. Supported types: BIG_ENDIAN_TIMESTAMP, PROTO_DECODE(package.MessageName)");
  }
}
