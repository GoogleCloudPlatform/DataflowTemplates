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

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Bytes;
import org.joda.time.format.ISODateTimeFormat;

public class BeamSchemaToBytesTransformers {
  public static final Map<Schema.TypeName, BiFunction<Row, Schema.Field, byte[]>>
      TYPE_TO_BYTES_TRANSFORMATIONS = buildBytesEncoders();

  private static byte[] toBytes(Object obj) {
    if (obj == null) {
      return new byte[0];
    } else {
      return obj.toString().getBytes(StandardCharsets.UTF_8);
    }
  }

  public static byte[] encodeArrayField(Row row, Schema.Field field) {
    if (field.getType().getCollectionElementType().equals(Schema.FieldType.DATETIME)) {
      throw new UnsupportedOperationException("Datetime arrays are not supported!");
    } else {
      return Bytes.concat(
          new byte[] {(byte) '['},
          row.getArray(field.getName()).stream()
              .map(obj -> obj == null ? "" : obj.toString())
              .collect(Collectors.joining(","))
              .getBytes(StandardCharsets.UTF_8),
          new byte[] {(byte) ']'});
    }
  }

  public static byte[] encodeRowField(Row row, Schema.Field field) {
    throw new UnsupportedOperationException("Nested fields are not supported!");
  }

  public static Map<Schema.TypeName, BiFunction<Row, Schema.Field, byte[]>> buildBytesEncoders() {
    return new HashMap<Schema.TypeName, BiFunction<Row, Schema.Field, byte[]>>() {
      {
        put(Schema.TypeName.BYTES, (row, field) -> row.getBytes(field.getName()));
        put(Schema.TypeName.STRING, ((row, field) -> toBytes(row.getString(field.getName()))));
        put(Schema.TypeName.BOOLEAN, (row, field) -> toBytes(row.getBoolean(field.getName())));
        put(Schema.TypeName.DOUBLE, (row, field) -> toBytes(row.getDouble(field.getName())));
        put(Schema.TypeName.FLOAT, (row, field) -> toBytes(row.getFloat(field.getName())));
        put(Schema.TypeName.INT16, (row, field) -> toBytes(row.getInt16(field.getName())));
        put(Schema.TypeName.INT32, (row, field) -> toBytes(row.getInt32(field.getName())));
        put(Schema.TypeName.INT64, (row, field) -> toBytes(row.getInt64(field.getName())));
        put(Schema.TypeName.DECIMAL, ((row, field) -> toBytes(row.getDecimal(field.getName()))));
        put(Schema.TypeName.ARRAY, BeamSchemaToBytesTransformers::encodeArrayField);
        put(Schema.TypeName.ROW, BeamSchemaToBytesTransformers::encodeRowField);
        put(
            Schema.TypeName.DATETIME,
            ((row, field) -> {
              if (row.getDateTime(field.getName()) == null) {
                return new byte[0];
              } else {
                return ISODateTimeFormat.basicDateTime()
                    .print(row.getDateTime(field.getName()))
                    .getBytes(StandardCharsets.UTF_8);
              }
            }));
      }
    };
  }
}
