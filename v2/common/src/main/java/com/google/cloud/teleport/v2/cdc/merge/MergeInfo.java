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
package com.google.cloud.teleport.v2.cdc.merge;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

/** Class {@link MergeInfo}. */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class MergeInfo implements Serializable {
  public abstract List<String> getAllPkFields();

  public abstract List<String> getOrderByFields();

  public abstract String getDeleteField();

  public abstract String getStagingTable();

  public abstract String getReplicaTable();

  public abstract List<String> getAllFields();

  @SchemaCreate
  public static MergeInfo create(
      List<String> allPkFields,
      List<String> orderByFields,
      String deleteField,
      String stagingTable,
      String replicaTable,
      List<String> allFields) {
    return new AutoValue_MergeInfo(
        allPkFields, orderByFields, deleteField, stagingTable, replicaTable, allFields);
  }
}
