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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.dataplex.v1.CloudDataplex;
import com.google.api.services.dataplex.v1.CloudDataplex.Projects.Locations.Lakes.Zones.Entities;
import com.google.api.services.dataplex.v1.CloudDataplex.Projects.Locations.Lakes.Zones.Entities.Partitions;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Asset;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1ListEntitiesResponse;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1ListPartitionsResponse;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Partition;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Zone;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.teleport.v2.values.EntityMetadata;
import com.google.cloud.teleport.v2.values.EntityMetadata.StorageSystem;
import com.google.cloud.teleport.v2.values.GetEntityRequestEntityView;
import com.google.cloud.teleport.v2.values.PartitionMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Default Dataplex implementation. This is still a work in progress and may change. */
public final class DefaultDataplexClient implements DataplexClient {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultDataplexClient.class);

  private static final Pattern ZONE_PATTERN =
      Pattern.compile("projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+");

  private static final String DEFAULT_ROOT_URL = "https://dataplex.googleapis.com/";
  private static final String DEFAULT_CLIENT_NAME = "DataflowTemplatesDataplexClient";

  private final CloudDataplex client;

  private DefaultDataplexClient(CloudDataplex client) {
    checkNotNull(client, "CloudDataplex instance cannot be null.");
    this.client = client;
  }

  /**
   * Returns an instance of {@link DefaultDataplexClient} that will utilize an instance of {@link
   * CloudDataplex}.
   *
   * @param client the instance to utilize
   * @return a new instance of {@link DefaultDataplexClient}
   */
  public static DefaultDataplexClient withClient(CloudDataplex client) {
    return new DefaultDataplexClient(client);
  }

  /**
   * Returns an instance of {@link DefaultDataplexClient} that will utilize the default {@link
   * CloudDataplex}.
   *
   * @return a new instance of {@link DefaultDataplexClient}
   */
  public static DefaultDataplexClient withDefaultClient() throws IOException {
    HttpTransport transport = Utils.getDefaultTransport();
    JsonFactory jsonFactory = Utils.getDefaultJsonFactory();
    HttpRequestInitializer httpInitializer =
        new HttpCredentialsAdapter(GoogleCredentials.getApplicationDefault());

    CloudDataplex client =
        new CloudDataplex.Builder(transport, jsonFactory, httpInitializer)
            .setApplicationName(DEFAULT_CLIENT_NAME)
            .setRootUrl(DEFAULT_ROOT_URL)
            .build();

    return new DefaultDataplexClient(client);
  }

  @Override
  public GoogleCloudDataplexV1Zone getZone(String zoneName) throws IOException {
    return client.projects().locations().lakes().zones().get(zoneName).execute();
  }

  @Override
  public GoogleCloudDataplexV1Asset getAsset(String assetName) throws IOException {
    return client.projects().locations().lakes().zones().assets().get(assetName).execute();
  }

  @Override
  public ImmutableList<GoogleCloudDataplexV1Entity> getCloudStorageEntities(String assetName)
      throws IOException {
    // the list entities response doesn't include entities' schemas, so the implementation is to
    // collect the entity names first and then request entities with schemas separately
    return getEntities(getCloudStorageEntityNames(assetName));
  }

  @Override
  public ImmutableList<GoogleCloudDataplexV1Entity> getEntities(List<String> entityNames)
      throws IOException {
    Entities entities = client.projects().locations().lakes().zones().entities();
    ImmutableList.Builder<GoogleCloudDataplexV1Entity> result = ImmutableList.builder();
    for (String entityName : entityNames) {
      result.add(
          entities.get(entityName).setView(GetEntityRequestEntityView.FULL.name()).execute());
    }
    return result.build();
  }

  @Override
  public ImmutableList<GoogleCloudDataplexV1Partition> getPartitions(String entityName)
      throws IOException {
    ImmutableList.Builder<GoogleCloudDataplexV1Partition> result = ImmutableList.builder();
    Partitions partitions = client.projects().locations().lakes().zones().entities().partitions();
    GoogleCloudDataplexV1ListPartitionsResponse response = partitions.list(entityName).execute();
    result.addAll(response.getPartitions());
    // the result of the list is paginated with the default page size being 10
    while (response.getNextPageToken() != null) {
      response = partitions.list(entityName).setPageToken(response.getNextPageToken()).execute();
      result.addAll(response.getPartitions());
    }
    return result.build();
  }

  /**
   * Creates entities and partitions.
   *
   * <p>This will log rather than throw exceptions.
   *
   * <p>If {@code createBehavior} is {@link CreateBehavior#FAIL_IF_EXISTS}, then all non-existing
   * entities will be created, but no entities will be updated. No partitions will be created.
   *
   * @param assetName example:
   *     projects/{name}/locations/{location}/lakes/{lake}/zones/{zone}/assets/{asset}
   * @param metadata entities and partitions to create and/or update
   * @param createBehavior what to do if an entity already exists (partitions can only be created)
   */
  @Override
  public void createMetadata(
      String assetName, ImmutableList<EntityMetadata> metadata, CreateBehavior createBehavior)
      throws IOException {
    if (shouldSkipCreatingMetadata(assetName)) {
      LOG.warn("Skipping creating metadata.");
      return;
    }

    Map<EntityMetadata, GoogleCloudDataplexV1Entity> metadataToEntities;
    try {
      metadataToEntities = createEntities(assetName, metadata, createBehavior);
    } catch (Exception e) {
      LOG.error(
          "At least some entities were not created or updated. Reason: {}\nEntity Data Paths: {}",
          e.getMessage(),
          metadata.stream().map(EntityMetadata::dataPath).collect(toList()));
      LOG.info("Skipping creating partitions.");
      return;
    }

    ImmutableMap<String, ImmutableList<PartitionMetadata>> nameToPartitions =
        metadataToEntities.entrySet().stream()
            .filter(e -> !e.getKey().partitions().isEmpty())
            .collect(
                ImmutableMap.toImmutableMap(
                    e -> e.getValue().getName(), e -> e.getKey().partitions()));

    try {
      createPartitions(nameToPartitions);
    } catch (Exception e) {
      LOG.error(
          "Some or all of the partitions could not be created. Reason: {}\nTarget entities: {}",
          e.getMessage(),
          nameToPartitions.keySet());
    }
  }

  /**
   * Determines if we should skip creating metadata under {@code assetName}.
   *
   * <p>Currently, we skip creating metadata if discovery is enabled on either the zone or asset.
   * Trying to create metadata manually when this is enabled can lead to undefined behavior.
   *
   * @param assetName name of the asset to check
   * @return true if we should skip metadata creation, false otherwise
   */
  private boolean shouldSkipCreatingMetadata(String assetName) throws IOException {
    GoogleCloudDataplexV1Asset asset = getAsset(assetName);
    if (asset.getDiscoverySpec().getEnabled()) {
      LOG.warn("Automatic discovery enabled for asset `{}`.", assetName);
      return true;
    }

    String zoneName = getZoneFromAsset(assetName);
    GoogleCloudDataplexV1Zone zone = getZone(zoneName);
    if (zone.getDiscoverySpec().getEnabled()) {
      LOG.warn("Automatic discovery enabled for zone `{}`", zoneName);
      return true;
    }

    return false;
  }

  /**
   * Handles creating and updating entities.
   *
   * <p>The return value is a map between the provided {@link EntityMetadata} and the Dataplex
   * object representing the newly-created (or updated) entity. These values should be the source of
   * truth for what is in Dataplex at the moment.
   */
  private Map<EntityMetadata, GoogleCloudDataplexV1Entity> createEntities(
      String assetName, ImmutableList<EntityMetadata> metadata, CreateBehavior createBehavior)
      throws IOException {
    List<GoogleCloudDataplexV1Entity> existing = getEntitiesUnderAsset(assetName);
    Map<String, GoogleCloudDataplexV1Entity> existingDataPaths =
        existing.stream().collect(toMap(GoogleCloudDataplexV1Entity::getDataPath, e -> e));
    Map<Boolean, List<EntityMetadata>> partitioned =
        metadata.stream().collect(partitioningBy(m -> existingDataPaths.containsKey(m.dataPath())));

    List<EntityMetadata> toCreate = partitioned.get(false);
    Map<EntityMetadata, GoogleCloudDataplexV1Entity> toUpdate =
        partitioned.get(true).stream()
            .collect(toMap(m -> m, m -> existingDataPaths.get(m.dataPath())));
    Map<EntityMetadata, GoogleCloudDataplexV1Entity> created = null;
    Map<EntityMetadata, GoogleCloudDataplexV1Entity> updated = null;

    if (!toCreate.isEmpty()) {
      created = createEntitiesUnderAsset(assetName, toCreate);
    }
    if (!toUpdate.isEmpty()) {
      if (createBehavior == CreateBehavior.FAIL_IF_EXISTS) {
        throw new IllegalArgumentException(
            String.format(
                "Some entities already exist: %s",
                toUpdate.keySet().stream().map(EntityMetadata::dataPath).collect(toList())));
      }
      updated = updateEntitiesUnderAsset(assetName, toUpdate);
    }

    Map<EntityMetadata, GoogleCloudDataplexV1Entity> flattened = new HashMap<>();
    if (created != null) {
      flattened.putAll(created);
    }
    if (updated != null) {
      flattened.putAll(updated);
    }

    return flattened;
  }

  /** Get Cloud Storage entity names of the given asset. */
  private ImmutableList<String> getCloudStorageEntityNames(String assetName) throws IOException {
    return getEntitiesUnderAssetStream(assetName)
        .filter(e -> Objects.equals(e.getSystem(), StorageSystem.CLOUD_STORAGE.name()))
        .map(GoogleCloudDataplexV1Entity::getName)
        .collect(toImmutableList());
  }

  /** Gets all entities under {@code assetName}. */
  private List<GoogleCloudDataplexV1Entity> getEntitiesUnderAsset(String assetName)
      throws IOException {
    return getEntitiesUnderAssetStream(assetName).collect(toList());
  }

  /** Gets a stream of all entities under {@code assetName}. */
  private Stream<GoogleCloudDataplexV1Entity> getEntitiesUnderAssetStream(String assetName)
      throws IOException {
    Entities entities = client.projects().locations().lakes().zones().entities();
    String zoneName = getZoneFromAsset(assetName);

    GoogleCloudDataplexV1ListEntitiesResponse response = entities.list(zoneName).execute();
    Stream<GoogleCloudDataplexV1Entity> result = getEntitiesUnderAssetForPage(response, assetName);
    // the result of the list is paginated with the default page size being 10
    while (response.getNextPageToken() != null) {
      response = entities.list(zoneName).setPageToken(response.getNextPageToken()).execute();
      result = Stream.concat(result, getEntitiesUnderAssetForPage(response, assetName));
    }
    return result;
  }

  private static Stream<GoogleCloudDataplexV1Entity> getEntitiesUnderAssetForPage(
      GoogleCloudDataplexV1ListEntitiesResponse response, String assetName) {
    return response.getEntities().stream()
        // Unfortunately, getting the entities from under an asset is not supported, so we need to
        // do the filtering on our end. Hopefully, the number of assets under a zone remain small
        // enough that this won't be too expensive.
        // TODO(zhoufek): Switch to just getting from an asset if/when Dataplex supports it.
        .filter(e -> Objects.equals(assetName, e.getAsset()));
  }

  /** Handles just the creation of entities. Each entity is logged after creation. */
  private Map<EntityMetadata, GoogleCloudDataplexV1Entity> createEntitiesUnderAsset(
      String assetName, List<EntityMetadata> metadata) throws IOException {
    Map<EntityMetadata, GoogleCloudDataplexV1Entity> metadataToEntity = new HashMap<>();
    for (EntityMetadata m : metadata) {
      GoogleCloudDataplexV1Entity entity =
          client
              .projects()
              .locations()
              .lakes()
              .zones()
              .entities()
              .create(assetName, m.toDataplexEntity().setAsset(assetName))
              .execute();
      LOG.info("Created entity with name '{}' pointing to '{}'", entity.getName(), m.dataPath());
      metadataToEntity.put(m, entity);
    }
    return metadataToEntity;
  }

  /** Handles just updating of entities. Each entity is logged after updating. */
  private Map<EntityMetadata, GoogleCloudDataplexV1Entity> updateEntitiesUnderAsset(
      String assetName, Map<EntityMetadata, GoogleCloudDataplexV1Entity> metadataToEntity)
      throws IOException {
    Map<EntityMetadata, GoogleCloudDataplexV1Entity> updatedMetadataToEntity = new HashMap<>();
    for (Map.Entry<EntityMetadata, GoogleCloudDataplexV1Entity> entry :
        metadataToEntity.entrySet()) {
      EntityMetadata metadata = entry.getKey();
      GoogleCloudDataplexV1Entity existing = entry.getValue();

      metadata.updateDataplexEntity(existing);
      GoogleCloudDataplexV1Entity updated =
          client
              .projects()
              .locations()
              .lakes()
              .zones()
              .entities()
              .update(existing.getName(), existing.setAsset(assetName))
              .execute();
      LOG.info(
          "Updated entity with name '{}' that points to data path '{}'",
          updated.getName(),
          metadata.dataPath());

      updatedMetadataToEntity.put(metadata, updated);
    }

    return updatedMetadataToEntity;
  }

  /** Handles creation of partitions. Each partition is logged after being created. */
  private void createPartitions(
      ImmutableMap<String, ImmutableList<PartitionMetadata>> entityNameToPartition)
      throws IOException {
    for (Map.Entry<String, ImmutableList<PartitionMetadata>> entry :
        entityNameToPartition.entrySet()) {
      ImmutableList<GoogleCloudDataplexV1Partition> partitions =
          entry.getValue().stream()
              .map(PartitionMetadata::toDataplexPartition)
              .collect(toImmutableList());
      for (GoogleCloudDataplexV1Partition partition : partitions) {
        GoogleCloudDataplexV1Partition result =
            client
                .projects()
                .locations()
                .lakes()
                .zones()
                .entities()
                .partitions()
                .create(entry.getKey(), partition)
                .execute();
        LOG.info("Created partition '{}' under entity '{}'", result.getName(), entry.getKey());
      }
    }
  }

  /** Gets the zone name from {@code assetName}. */
  private static String getZoneFromAsset(String assetName) {
    Matcher matcher = ZONE_PATTERN.matcher(assetName);
    if (matcher.find()) {
      return matcher.group();
    }
    throw new IllegalArgumentException(
        String.format("Asset '%s' not properly formatted", assetName));
  }
}
