/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.v2.datastream.values;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

/** Class {@link DmlInfo}. */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class DmlInfo implements Serializable {
  // TODO just failsafe value and all cleaning when creating the object?
  public abstract String getFailsafeValue();

  public abstract String getDmlSql();

  public abstract String getSchemaName();

  public abstract String getTableName();

  public abstract List<String> getAllPkFields();

  public abstract List<String> getOrderByFields();

  public abstract List<String> getPrimaryKeyValues();

  public abstract List<String> getOrderByValues();

  public abstract String getOriginalPayload();

  @SchemaCreate
  public static DmlInfo of(
      String failsafeValue,
      String dmlSql,
      String schemaName,
      String tableName,
      List<String> allPkFields,
      List<String> orderByFields,
      List<String> primaryKeyValues,
      List<String> orderByValues,
      String originalPayload) {
    return new AutoValue_DmlInfo(
        failsafeValue,
        dmlSql,
        schemaName,
        tableName,
        allPkFields,
        orderByFields,
        primaryKeyValues,
        orderByValues,
        originalPayload);
  }

  public String getStateWindowKey() {
    String pkValuesString = String.join("-", this.getPrimaryKeyValues());
    return this.getSchemaName() + "." + this.getTableName() + ":" + pkValuesString;
  }

  public String getOrderByValueString() {
    List<String> fields = this.getOrderByFields();
    List<String> values = this.getOrderByValues();
    List<String> normalizedValues = new ArrayList<>(values.size());
    for (int i = 0; i < values.size(); i++) {
      String val = values.get(i);
      if (fields != null && i < fields.size() && "_metadata_lsn".equals(fields.get(i))) {
        normalizedValues.add(normalizeLsnValue(val));
      } else {
        normalizedValues.add(val);
      }
    }
    return String.join("-", normalizedValues);
  }

  static String normalizeLsnValue(String lsnValue) {
    if (lsnValue == null
        || lsnValue.isEmpty()
        || lsnValue.equalsIgnoreCase("null")
        || lsnValue.equalsIgnoreCase("'null'")) {
      return "";
    }
    boolean quoted = lsnValue.length() >= 2 && lsnValue.startsWith("'") && lsnValue.endsWith("'");
    String raw = quoted ? lsnValue.substring(1, lsnValue.length() - 1) : lsnValue;
    int slashIdx = raw.indexOf('/');
    if (slashIdx > 0 && slashIdx < raw.length() - 1) {
      String high = raw.substring(0, slashIdx);
      String low = raw.substring(slashIdx + 1);
      try {
        long highVal = Long.parseUnsignedLong(high, 16);
        long lowVal = Long.parseUnsignedLong(low, 16);
        String padded = String.format("%08X/%08X", highVal, lowVal);
        return quoted ? "'" + padded + "'" : padded;
      } catch (NumberFormatException e) {
        // Non-hex LSN (e.g., SQL Server), return as-is
        return lsnValue;
      }
    }
    return lsnValue;
  }
}
