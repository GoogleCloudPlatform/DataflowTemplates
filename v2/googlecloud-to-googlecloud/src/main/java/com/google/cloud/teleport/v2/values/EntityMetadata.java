/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.values;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Schema;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1StorageFormat;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.util.UUID;

/**
 * The metadata for Dataplex entities.
 *
 * <p>The following fields a required:
 *
 * <ul>
 *   <li>storageFormat
 *   <li>dataPath
 *   <li>schema
 * </ul>
 *
 * <p>The following fields are optional:
 *
 * <ul>
 *   <li>assetName: Left optional to make it simpler to create multiple entities under one asset.
 *   <li>id: If not provided, a UUID will be used
 * </ul>
 */
@AutoValue
public abstract class EntityMetadata {

  /** Enum for the valid entity types. */
  public enum EntityType {
    TYPE_UNSPECIFIED,
    TABLE,
    FILESET
  }

  /** Enum for value storage system types. */
  public enum StorageSystem {
    STORAGE_SYSTEM_UNSPECIFIED,
    CLOUD_STORAGE,
    BIGQUERY
  }

  public abstract String assetName();

  public abstract String id();

  public abstract EntityType entityType();

  public abstract StorageSystem storageSystem();

  public abstract GoogleCloudDataplexV1StorageFormat storageFormat();

  public abstract String dataPath();

  public abstract GoogleCloudDataplexV1Schema schema();

  public abstract ImmutableList<PartitionMetadata> partitions();

  public static Builder builder() {
    return new AutoValue_EntityMetadata.Builder()
        .setAssetName("")
        .setId(UUID.randomUUID().toString())
        .setPartitions(ImmutableList.of());
  }

  /** Converts this to a {@link GoogleCloudDataplexV1Entity}. */
  public GoogleCloudDataplexV1Entity toDataplexEntity() {
    return setDataplexEntity(new GoogleCloudDataplexV1Entity());
  }

  /** Updates {@code entity} with all the values in this instance. */
  public void updateDataplexEntity(GoogleCloudDataplexV1Entity entity) {
    setDataplexEntity(entity);
  }

  /** Handles setting all the values of {@code entity} before returning it. */
  private GoogleCloudDataplexV1Entity setDataplexEntity(GoogleCloudDataplexV1Entity entity) {
    return entity
        .setAsset(assetName())
        .setId(id())
        .setType(entityType().toString())
        .setSystem(storageSystem().toString())
        .setFormat(storageFormat())
        .setDataPath(dataPath())
        .setSchema(schema());
  }

  /** Builder for {@link EntityMetadata}. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setAssetName(String value);

    public abstract Builder setId(String value);

    public abstract Builder setEntityType(EntityType value);

    public abstract Builder setStorageSystem(StorageSystem value);

    public abstract Builder setStorageFormat(GoogleCloudDataplexV1StorageFormat value);

    public abstract Builder setDataPath(String value);

    public abstract Builder setSchema(GoogleCloudDataplexV1Schema value);

    public abstract Builder setPartitions(ImmutableList<PartitionMetadata> value);

    abstract EntityMetadata autoBuild();

    public EntityMetadata build() {
      EntityMetadata metadata = autoBuild();
      checkState(!metadata.dataPath().isEmpty(), "dataPath cannot be empty");
      return metadata;
    }
  }
}
