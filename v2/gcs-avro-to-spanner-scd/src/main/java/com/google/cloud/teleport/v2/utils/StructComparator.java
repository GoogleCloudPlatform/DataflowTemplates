/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.utils;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.utils.StructHelper.ValueHelper;
import java.util.Comparator;

public class StructComparator implements Comparator<Struct> {

  private final String orderByColumnName;

  StructComparator(String orderByColumnName) {
    this.orderByColumnName = orderByColumnName;
  }

  public static StructComparator create(String orderByColumnName) {
    return new StructComparator(orderByColumnName);
  }

  @Override
  public int compare(Struct a, Struct b) {
    Value aValue = a.getValue(orderByColumnName);
    Value bValue = b.getValue(orderByColumnName);

    // Sort NULL first (ASC), following SQL behaviour.
    if (aValue.isNull() && bValue.isNull()) {
      return 0;
    } else if (aValue.isNull()) {
      return -1;
    } else if (bValue.isNull()) {
      return 1;
    }

    switch (aValue.getType().getCode()) {
      case BOOL:
        return ValueHelper.of(aValue)
            .getBoolOrNull()
            .compareTo(ValueHelper.of(bValue).getBoolOrNull());
      case BYTES:
        throw new RuntimeException(
            String.format("Unable to sort by ByteArray field %s.", orderByColumnName));
      case DATE:
        return ValueHelper.of(aValue)
            .getDateOrNull()
            .compareTo(ValueHelper.of(bValue).getDateOrNull());
      case FLOAT32:
        return ValueHelper.of(aValue)
            .getFloat32OrNull()
            .compareTo(ValueHelper.of(bValue).getFloat32OrNull());
      case FLOAT64:
        return ValueHelper.of(aValue)
            .getFloat64OrNull()
            .compareTo(ValueHelper.of(bValue).getFloat64OrNull());
      case INT64:
        return ValueHelper.of(aValue)
            .getInt64OrNull()
            .compareTo(ValueHelper.of(bValue).getInt64OrNull());
      case JSON:
        return ValueHelper.of(aValue)
            .getJsonOrNull()
            .compareTo(ValueHelper.of(bValue).getJsonOrNull());
      case NUMERIC:
      case PG_NUMERIC:
        return ValueHelper.of(aValue)
            .getNumericOrNull()
            .compareTo(ValueHelper.of(bValue).getNumericOrNull());
      case PG_JSONB:
        return ValueHelper.of(aValue)
            .getPgJsonbOrNull()
            .compareTo(ValueHelper.of(bValue).getPgJsonbOrNull());
      case STRING:
        return ValueHelper.of(aValue)
            .getStringOrNull()
            .compareTo(ValueHelper.of(bValue).getStringOrNull());
      case TIMESTAMP:
        return ValueHelper.of(aValue)
            .getTimestampOrNull()
            .compareTo(ValueHelper.of(bValue).getTimestampOrNull());
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported Spanner field type %s.", aValue.getType().getCode()));
    }
  }
}
