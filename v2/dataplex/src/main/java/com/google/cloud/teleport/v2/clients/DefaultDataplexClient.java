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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.Collectors.toList;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
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
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.teleport.v2.utils.DataplexUtils;
import com.google.cloud.teleport.v2.values.DataplexEnums;
import com.google.cloud.teleport.v2.values.GetEntityRequestEntityView;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Default Dataplex implementation. This is still a work in progress and may change. */
public final class DefaultDataplexClient implements DataplexClient {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultDataplexClient.class);

  private static final String DEFAULT_ROOT_URL = "https://dataplex.googleapis.com/";
  private static final String DEFAULT_CLIENT_NAME = "DataflowTemplatesDataplexClient";
  private static final Integer MAX_LIST_ENTITIES_PAGE_SIZE = 500;

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
  public static DefaultDataplexClient withDefaultClient(Credentials credential) throws IOException {
    HttpTransport transport = Utils.getDefaultTransport();
    JsonFactory jsonFactory = Utils.getDefaultJsonFactory();
    HttpRequestInitializer httpInitializer = new HttpCredentialsAdapter(credential);

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
    // TODO(an2x): Dataplex now supports filtering so getCloudStorageEntityNames() can now be
    //  replaced with a call to listEntities(zone, filter) to prevent loading entities from other
    //  assets on the client. And the unused private methods can be removed.

    // The list entities response doesn't include entities' schemas, so the implementation is to
    // collect the entity names first and then request entities with schemas separately:
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
  public GoogleCloudDataplexV1Entity getEntity(String entityName) throws IOException {
    return client
        .projects()
        .locations()
        .lakes()
        .zones()
        .entities()
        .get(entityName)
        .setView(GetEntityRequestEntityView.FULL.name())
        .execute();
  }

  @Override
  public ImmutableList<GoogleCloudDataplexV1Entity> listEntities(String zoneName, String filter)
      throws IOException {
    Entities.List listReq =
        client
            .projects()
            .locations()
            .lakes()
            .zones()
            .entities()
            .list(zoneName)
            .setFilter(filter)
            .setPageSize(MAX_LIST_ENTITIES_PAGE_SIZE);

    ImmutableList.Builder<GoogleCloudDataplexV1Entity> result = ImmutableList.builder();
    String pageToken = null;

    do {
      listReq.setPageToken(pageToken);
      GoogleCloudDataplexV1ListEntitiesResponse response = listReq.execute();
      if (response.getEntities() != null) {
        result.addAll(response.getEntities());
      }
      pageToken = response.getNextPageToken();
    } while (pageToken != null && !pageToken.isEmpty());

    return result.build();
  }

  @Override
  public GoogleCloudDataplexV1Entity createEntity(
      String zoneName, GoogleCloudDataplexV1Entity entity) throws IOException {
    checkNotNull(entity.getId(), "Entity ID is required.");
    checkArgument(
        entity.getName() == null,
        "Entity name must not be set when creating a new entity (it will be auto-generated), but got: "
            + entity.getName());
    return client
        .projects()
        .locations()
        .lakes()
        .zones()
        .entities()
        .create(zoneName, entity)
        .execute();
  }

  @Override
  public GoogleCloudDataplexV1Entity updateEntity(GoogleCloudDataplexV1Entity entity)
      throws IOException {
    checkNotNull(entity.getName(), "Entity name is required.");
    return client
        .projects()
        .locations()
        .lakes()
        .zones()
        .entities()
        .update(entity.getName(), entity)
        .execute();
  }

  @Override
  public GoogleCloudDataplexV1Partition createPartition(
      String entityName, GoogleCloudDataplexV1Partition partition) throws IOException {
    return client
        .projects()
        .locations()
        .lakes()
        .zones()
        .entities()
        .partitions()
        .create(entityName, partition)
        .execute();
  }

  private void deletePartition(String partitionName) throws IOException {
    client
        .projects()
        .locations()
        .lakes()
        .zones()
        .entities()
        .partitions()
        .delete(partitionName)
        .execute();
  }

  @Override
  public GoogleCloudDataplexV1Partition createOrUpdatePartition(
      String entityName, GoogleCloudDataplexV1Partition partition) throws IOException {
    try {
      return createPartition(entityName, partition);
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() == 400
          && e.getMessage() != null
          && e.getMessage().toLowerCase().contains("already exists")) {
        String partitionName =
            entityName + "/partitions/" + String.join("/", partition.getValues());
        LOG.info("Partition already exists, recreating: {}", partitionName);
        try {
          deletePartition(partitionName);
        } catch (Exception ee) {
          throw new IOException(
              String.format("Partition %s already exists, but couldn't replace it.", partitionName),
              ee);
        }
        return createPartition(entityName, partition); // Re-create after deletion.
      }
      throw e;
    }
  }

  @Override
  public ImmutableList<GoogleCloudDataplexV1Partition> getPartitions(String entityName)
      throws IOException {
    ImmutableList.Builder<GoogleCloudDataplexV1Partition> result = ImmutableList.builder();
    Partitions partitions = client.projects().locations().lakes().zones().entities().partitions();
    GoogleCloudDataplexV1ListPartitionsResponse response = partitions.list(entityName).execute();
    if (response.getPartitions() == null) {
      return ImmutableList.of();
    }
    result.addAll(response.getPartitions());
    // the result of the list is paginated with the default page size being 10
    while (response.getNextPageToken() != null) {
      response = partitions.list(entityName).setPageToken(response.getNextPageToken()).execute();
      result.addAll(response.getPartitions());
    }
    return result.build();
  }

  /** Get Cloud Storage entity names of the given asset. */
  private ImmutableList<String> getCloudStorageEntityNames(String assetName) throws IOException {
    return getEntitiesUnderAssetStream(assetName)
        .filter(
            e -> Objects.equals(e.getSystem(), DataplexEnums.StorageSystem.CLOUD_STORAGE.name()))
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
    String zoneName = DataplexUtils.getZoneFromAsset(assetName);

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
    // The entities created after Sept 2021 should contain short asset names and entities created
    // before that should contain full names.
    String assetShortName = DataplexUtils.getShortAssetNameFromAsset(assetName);
    return response.getEntities().stream()
        // Unfortunately, getting the entities from under an asset is not supported, so we need to
        // do the filtering on our end. Hopefully, the number of assets under a zone remain small
        // enough that this won't be too expensive.
        // TODO(zhoufek): Switch to just getting from an asset if/when Dataplex supports it.
        .filter(
            e ->
                Objects.equals(assetShortName, e.getAsset())
                    || Objects.equals(assetName, e.getAsset()));
  }
}
