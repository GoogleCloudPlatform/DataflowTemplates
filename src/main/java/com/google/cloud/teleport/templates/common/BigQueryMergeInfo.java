/*
 * Copyright (C) 2018 Google Inc.
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
package com.google.cloud.teleport.templates.common;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class BigQueryMergeInfo implements Serializable {
  public abstract String getTimestampField();

  public abstract String getDeleteField();

  public abstract String getStagingTable();

  public abstract String getReplicaTable();

  public abstract List<String> getAllFields();

  public abstract List<String> getAllPkFields();

  @SchemaCreate
  public static BigQueryMergeInfo create(
      String timestampField,
      String deleteField,
      String stagingTable,
      String replicaTable,
      List<String> allFields,
      List<String> allPkFields) {
    return new AutoValue_BigQueryMergeInfo(
        timestampField, deleteField, stagingTable, replicaTable, allFields, allPkFields);
  }
}