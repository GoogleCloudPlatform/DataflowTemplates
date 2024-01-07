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

  @SchemaCreate
  public static DmlInfo of(
      String failsafeValue,
      String dmlSql,
      String schemaName,
      String tableName,
      List<String> allPkFields,
      List<String> orderByFields,
      List<String> primaryKeyValues,
      List<String> orderByValues) {
    return new AutoValue_DmlInfo(
        failsafeValue,
        dmlSql,
        schemaName,
        tableName,
        allPkFields,
        orderByFields,
        primaryKeyValues,
        orderByValues);
  }

  public String getStateWindowKey() {
    String pkValuesString = String.join("-", this.getPrimaryKeyValues());
    return this.getSchemaName() + "." + this.getTableName() + ":" + pkValuesString;
  }

  public String getOrderByValueString() {
    return String.join("-", this.getOrderByValues());
  }
}
