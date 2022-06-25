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
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Zone;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;

/** Interface for interacting with Google Cloud Dataplex. */
public interface DataplexClient {

  /**
   * Looks up the Dataplex zone by its name.
   *
   * @param zoneName example:
   *     projects/{name}/locations/{location}/lakes/{lake}/zones/{zone}/assets/{asset}
   * @return a Dataplex zone
   */
  GoogleCloudDataplexV1Zone getZone(String zoneName) throws IOException;

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
   * Lists all entities in a zone matching an optional filter.
   *
   * @param zoneName example:
   *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}
   * @param filter optional filter, can be {@code null} or empty, follows <a
   *     href="https://google.aip.dev/160">AIP-160</a> standard, Dataplex supports {@code asset} and
   *     {@code data_path} fields, example filter: {@code asset=&lt;asset name&gt; AND
   *     data_path="gs://bucket/directory"}
   */
  ImmutableList<GoogleCloudDataplexV1Entity> listEntities(String zoneName, String filter)
      throws IOException;

  /**
   * Creates a new entity in a specific zone (failing if it already exists).
   *
   * @param zoneName example:
   *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}
   * @param entity entity to create, {@link GoogleCloudDataplexV1Entity#getName() name} must be
   *     empty
   * @return the created instance as returned by Dataplex, with the new generated {@link
   *     GoogleCloudDataplexV1Entity#getName() name}
   * @throws IOException if entity with this name already exists
   */
  GoogleCloudDataplexV1Entity createEntity(String zoneName, GoogleCloudDataplexV1Entity entity)
      throws IOException;

  /**
   * Updates an existing entity (failing if it doesn't exist).
   *
   * @param entity entity to update, {@link GoogleCloudDataplexV1Entity#getName() name} must not be
   *     empty
   * @return the updated instance as returned by Dataplex, theoretically could be different from
   *     {@code entity}
   * @throws IOException if entity with this name doesn't exist
   */
  GoogleCloudDataplexV1Entity updateEntity(GoogleCloudDataplexV1Entity entity) throws IOException;

  /**
   * Creates a new partition for an entity.
   *
   * @param entityName example:
   *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/entities/{entity_id}
   * @param partition partition to create, {@code name} must be empty
   * @return the created instance as returned by Dataplex, with the new generated {@code name}
   */
  GoogleCloudDataplexV1Partition createPartition(
      String entityName, GoogleCloudDataplexV1Partition partition) throws IOException;

  /**
   * Creates a new partition for an entity, or updates it if it already exists.
   *
   * <p>Since Dataplex doesn't support partition updates, update means delete+create.
   *
   * @param entityName example:
   *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/entities/{entity_id}
   * @param partition partition to create, {@code name} must be empty
   * @return the created instance as returned by Dataplex, with the new generated {@code name}
   */
  GoogleCloudDataplexV1Partition createOrUpdatePartition(
      String entityName, GoogleCloudDataplexV1Partition partition) throws IOException;

  /**
   * Gets partitions of the entity.
   *
   * @param entityName example:
   *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/entities/
   *     {entity_id}
   */
  ImmutableList<GoogleCloudDataplexV1Partition> getPartitions(String entityName) throws IOException;
}
