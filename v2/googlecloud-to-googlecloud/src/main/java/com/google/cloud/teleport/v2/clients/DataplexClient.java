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
package com.google.cloud.teleport.v2.clients;

import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Asset;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Partition;
import com.google.cloud.teleport.v2.values.EntityMetadata;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;

/** Interface for interacting with Google Cloud Dataplex. */
public interface DataplexClient {

  /**
   * Looks up the Dataplex asset by its name.
   *
   * @param assetName example:
   *     projects/{name}/locations/{location}/lakes/{lake}/zones/{zone}/assets/{asset}
   * @return an asset from Dataplex
   */
  GoogleCloudDataplexV1Asset getAsset(String assetName) throws IOException;

  /**
   * Get Cloud Storage (StorageSystem.CLOUD_STORAGE) entities of the given asset.
   *
   * @param assetName example:
   *     projects/{name}/locations/{location}/lakes/{lake}/zones/{zone}/assets/{asset}
   */
  ImmutableList<GoogleCloudDataplexV1Entity> getCloudStorageEntities(String assetName)
      throws IOException;

  /**
   * Get entities by their names.
   *
   * @param entityNames example:
   *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/entities/
   *     {entity_id_1},
   *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/entities/
   *     {entity_id_2}
   */
  ImmutableList<GoogleCloudDataplexV1Entity> getEntities(List<String> entityNames)
      throws IOException;

  /**
   * Get partitions of the entity.
   *
   * @param entityName example:
   *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/entities/
   *     {entity_id}
   */
  ImmutableList<GoogleCloudDataplexV1Partition> getPartitions(String entityName) throws IOException;

  /**
   * Creates the metadata for {@code asset}.
   *
   * <p>Implementations may or may not throw an exception on failure. They should document their
   * chosen behavior.
   *
   * @param assetName example:
   *     projects/{name}/locations/{location}/lakes/{lake}/zones/{zone}/assets/{asset}
   * @param metadata entities and partitions to create and/or update
   * @param createBehavior what to do if an entity already exists (partitions can only be created)
   *     Implementations may create some or all of the non-existing records.
   */
  void createMetadata(
      String assetName, ImmutableList<EntityMetadata> metadata, CreateBehavior createBehavior)
      throws IOException;

  /** Determines what to do on a create request if a given resource already exists. */
  enum CreateBehavior {
    /** Fail (exit method) if the resource exists. */
    FAIL_IF_EXISTS,

    /** Update the existing resource with the new metadata. */
    UPDATE_IF_EXISTS
  }
}
