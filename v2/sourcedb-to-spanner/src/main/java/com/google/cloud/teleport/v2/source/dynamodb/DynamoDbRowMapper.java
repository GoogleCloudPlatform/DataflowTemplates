/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.source.dynamodb;

import com.google.cloud.teleport.v2.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/** Maps an Amazon DynamoDB item ({@code Map<String, AttributeValue>}) to a {@link SourceRow}. */
public class DynamoDbRowMapper implements Serializable {

  private static final Gson GSON = new GsonBuilder().serializeNulls().create();

  private final SourceSchemaReference sourceSchemaReference;
  private final SourceTableSchema sourceTableSchema;

  public DynamoDbRowMapper(
      SourceSchemaReference sourceSchemaReference, SourceTableSchema sourceTableSchema) {
    this.sourceSchemaReference = sourceSchemaReference;
    this.sourceTableSchema = sourceTableSchema;
  }

  /** Converts a single DynamoDB item to a {@link SourceRow}. */
  public SourceRow mapRow(Map<String, AttributeValue> item) {
    Instant now = Instant.now();
    long readTimeMicros =
        TimeUnit.SECONDS.toMicros(now.getEpochSecond())
            + TimeUnit.NANOSECONDS.toMicros(now.getNano());
    SourceRow.Builder rowBuilder =
        SourceRow.builder(sourceSchemaReference, sourceTableSchema, null, readTimeMicros);
    for (Map.Entry<String, SourceColumnType> entry :
        sourceTableSchema.sourceColumnNameToSourceColumnType().entrySet()) {
      String colName = entry.getKey();
      String colType = entry.getValue().getName().toUpperCase();
      AttributeValue attrValue = item != null ? item.get(colName) : null;
      rowBuilder.setField(colName, extractFieldValue(attrValue, colType));
    }
    return rowBuilder.build();
  }

  static Object extractFieldValue(AttributeValue attr, String colType) {
    if (attr == null || Boolean.TRUE.equals(attr.nul()) || attr.type() == AttributeValue.Type.NUL) {
      return null;
    }

    switch (colType) {
      case "S":
      case "STRING":
      case "NULL":
        if (attr.s() != null) {
          return attr.s();
        }
        return convertDynamicAttributeToString(attr);
      case "N":
      case "NUMBER":
        if (attr.n() != null) {
          return attr.n();
        }
        return convertDynamicAttributeToString(attr);
      case "B":
      case "BINARY":
        if (attr.b() != null) {
          return ByteBuffer.wrap(attr.b().asByteArray());
        }
        return null;
      case "BOOL":
      case "BOOLEAN":
        if (attr.bool() != null) {
          return attr.bool();
        }
        if (attr.s() != null) {
          return Boolean.parseBoolean(attr.s());
        }
        return null;
      case "M":
      case "MAP":
      case "L":
      case "LIST":
        return GSON.toJson(attributeValueToPlainObject(attr));
      case "SS":
      case "STRING_SET":
        if (attr.hasSs()) {
          return new ArrayList<>(attr.ss());
        }
        return null;
      case "NS":
      case "NUMBER_SET":
        if (attr.hasNs()) {
          return new ArrayList<>(attr.ns());
        }
        return null;
      case "BS":
      case "BINARY_SET":
        if (attr.hasBs()) {
          List<ByteBuffer> buffers = new ArrayList<>(attr.bs().size());
          for (SdkBytes sdkBytes : attr.bs()) {
            buffers.add(ByteBuffer.wrap(sdkBytes.asByteArray()));
          }
          return buffers;
        }
        return null;
      case "UNSUPPORTED":
      default:
        return null;
    }
  }

  private static String convertDynamicAttributeToString(AttributeValue attr) {
    Object plain = attributeValueToPlainObject(attr);
    if (plain == null) {
      return null;
    }
    if (plain instanceof String || plain instanceof Number || plain instanceof Boolean) {
      return plain.toString();
    }
    return GSON.toJson(plain);
  }

  static Object attributeValueToPlainObject(AttributeValue attr) {
    if (attr == null || Boolean.TRUE.equals(attr.nul()) || attr.type() == AttributeValue.Type.NUL) {
      return null;
    }
    switch (attr.type()) {
      case S:
        return attr.s();
      case N:
        try {
          return new BigDecimal(attr.n());
        } catch (NumberFormatException e) {
          return attr.n();
        }
      case B:
        return Base64.getEncoder().encodeToString(attr.b().asByteArray());
      case BOOL:
        return attr.bool();
      case M:
        Map<String, Object> map = new LinkedHashMap<>();
        if (attr.hasM()) {
          for (Map.Entry<String, AttributeValue> entry : attr.m().entrySet()) {
            map.put(entry.getKey(), attributeValueToPlainObject(entry.getValue()));
          }
        }
        return map;
      case L:
        List<Object> list = new ArrayList<>();
        if (attr.hasL()) {
          for (AttributeValue elem : attr.l()) {
            list.add(attributeValueToPlainObject(elem));
          }
        }
        return list;
      case SS:
        return attr.hasSs() ? new ArrayList<>(attr.ss()) : List.of();
      case NS:
        if (!attr.hasNs()) {
          return List.of();
        }
        return attr.ns().stream()
            .map(
                n -> {
                  try {
                    return new BigDecimal(n);
                  } catch (NumberFormatException e) {
                    return n;
                  }
                })
            .collect(Collectors.toList());
      case BS:
        if (!attr.hasBs()) {
          return List.of();
        }
        return attr.bs().stream()
            .map(b -> Base64.getEncoder().encodeToString(b.asByteArray()))
            .collect(Collectors.toList());
      case NUL:
      case UNKNOWN_TO_SDK_VERSION:
      default:
        return null;
    }
  }
}
