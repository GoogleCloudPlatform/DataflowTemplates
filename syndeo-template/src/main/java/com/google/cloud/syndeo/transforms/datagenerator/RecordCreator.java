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
package com.google.cloud.syndeo.transforms.datagenerator;

import java.util.List;
import java.util.Random;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.Row.FieldValueBuilder;

public final class RecordCreator {

  private static final String ALPHA_NUMBERIC_STRING =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvxyz";

  public static Random random = new Random();

  public static Row createRowRecord(Schema avroSchema) {
    return createRowRecord(avroSchema, AvroUtils.toBeamSchema(avroSchema));
  }

  /**
   * Creates Row record based on schema
   *
   * @param avroSchema the AvroSchema passing fields with additional attributes like field size and
   *     list of selected values. This can be removed later once the beam schema can be used to pass
   *     the same attributes.
   * @param beamSchema the BeamSchema object for building Row.
   */
  public static Row createRowRecord(
      Schema avroSchema, org.apache.beam.sdk.schemas.Schema beamSchema) {
    Row.Builder builder = Row.withSchema(beamSchema);
    FieldValueBuilder fvbuilder = null;

    for (Field field : avroSchema.getFields()) {
      if (field.schema().getType() != Schema.Type.RECORD) {
        Object value = generateRandomValue(field);
        if (fvbuilder == null) {
          fvbuilder = builder.withFieldValue(field.name(), value);
        } else {
          fvbuilder = fvbuilder.withFieldValue(field.name(), value);
        }
      } else {
        if (fvbuilder == null) {
          fvbuilder = builder.withFieldValue(field.name(), createRowRecord(field.schema()));
        } else {
          fvbuilder = fvbuilder.withFieldValue(field.name(), createRowRecord(field.schema()));
        }
      }
    }
    if (fvbuilder == null) {
      return null;
    }
    return fvbuilder.build();
  }

  private static Object generateRandomValue(Field field) {
    Object selectObj = field.getObjectProp("select");
    if (selectObj != null && selectObj instanceof List) {
      List<Object> selectValues = (List) selectObj;
      Object valueObj = selectValues.get(random.nextInt(selectValues.size()));

      String valueStr = String.valueOf(valueObj);
      if (field.schema().getType() == Schema.Type.STRING) {
        return valueStr;
      } else if (field.schema().getType() == Schema.Type.DOUBLE) {
        return Double.valueOf(valueStr);
      } else if (field.schema().getType() == Schema.Type.FLOAT) {
        return Float.valueOf(valueStr);
      } else if (field.schema().getType() == Schema.Type.INT) {
        return Integer.valueOf(valueStr);
      } else if (field.schema().getType() == Schema.Type.LONG) {
        return Long.valueOf(valueStr);
      } else if (field.schema().getType() == Schema.Type.BOOLEAN) {
        return Boolean.valueOf(valueStr);
      } else {
        throw new IllegalArgumentException("Not supported type: " + field.schema().getType());
      }
    }

    if (field.schema().getType() == Schema.Type.STRING) {
      int size = 100;
      String sizeStr = field.getProp("size");
      if (sizeStr != null && !sizeStr.isEmpty()) {
        size = Integer.parseInt(sizeStr);
      }

      return generateRandomString(size, 0.3f);
    } else if (field.schema().getType() == Schema.Type.DOUBLE) {
      return random.nextDouble();
    } else if (field.schema().getType() == Schema.Type.FLOAT) {
      return random.nextFloat();
    } else if (field.schema().getType() == Schema.Type.INT) {
      return random.nextInt();
    } else if (field.schema().getType() == Schema.Type.LONG) {
      return random.nextLong();
    } else if (field.schema().getType() == Schema.Type.BOOLEAN) {
      return random.nextBoolean();
    } else {
      throw new IllegalArgumentException("Not supported type: " + field.schema().getType());
    }
  }

  public static String generateRandomString(int size, float variationOfSize) {
    int minSize = (int) (1.0 * size * (1 - variationOfSize));
    int maxSize = (int) (1.0 * size * (1 + variationOfSize));
    int randomSize = minSize + random.nextInt(maxSize - minSize);

    StringBuilder sb = new StringBuilder(randomSize);

    for (int i = 0; i < randomSize; i++) {
      int index = (int) (ALPHA_NUMBERIC_STRING.length() * Math.random());
      sb.append(ALPHA_NUMBERIC_STRING.charAt(index));
    }

    return sb.toString();
  }
}
