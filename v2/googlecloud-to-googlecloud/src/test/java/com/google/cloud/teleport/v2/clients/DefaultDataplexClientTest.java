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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.dataplex.v1.CloudDataplex;
import com.google.api.services.dataplex.v1.CloudDataplex.Projects.Locations.Lakes.Zones;
import com.google.api.services.dataplex.v1.CloudDataplex.Projects.Locations.Lakes.Zones.Assets;
import com.google.api.services.dataplex.v1.CloudDataplex.Projects.Locations.Lakes.Zones.Entities;
import com.google.api.services.dataplex.v1.CloudDataplex.Projects.Locations.Lakes.Zones.Entities.Partitions;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Asset;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1AssetDiscoverySpec;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1ListEntitiesResponse;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1ListPartitionsResponse;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Partition;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Schema;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1StorageFormat;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Zone;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1ZoneDiscoverySpec;
import com.google.cloud.teleport.v2.clients.DataplexClient.CreateBehavior;
import com.google.cloud.teleport.v2.values.EntityMetadata;
import com.google.cloud.teleport.v2.values.EntityMetadata.EntityType;
import com.google.cloud.teleport.v2.values.EntityMetadata.StorageSystem;
import com.google.cloud.teleport.v2.values.GetEntityRequestEntityView;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import org.junit.Test;
import org.mockito.Answers;

/**
 * Test cases for the {@link DefaultDataplexClient} class. Inspired by {@link CsvConvertersTest}.
 */
public class DefaultDataplexClientTest {

  private static final String ZONE_NAME =
      "projects/project_1/locations/location_1/lakes/lake_1/zones/zone_1";
  private static final String SHORT_ASSET_NAME1 = "asset_1";
  private static final String ASSET_NAME1 = ZONE_NAME + "/asset/" + SHORT_ASSET_NAME1;
  private static final String ASSET_NAME2 = ZONE_NAME + "/asset/asset_2";
  private static final String PAGE_TOKEN = "token_1";

  // Metadata for use in tests where it doens't  matter what the values are
  private static final EntityMetadata UNUSED_METADATA =
      EntityMetadata.builder()
          .setDataPath("invalid")
          .setSchema(new GoogleCloudDataplexV1Schema())
          .setStorageFormat(new GoogleCloudDataplexV1StorageFormat())
          .setEntityType(EntityType.TYPE_UNSPECIFIED)
          .setStorageSystem(StorageSystem.STORAGE_SYSTEM_UNSPECIFIED)
          .setAssetName("invalid")
          .setId("invalid")
          .build();

  private static final GoogleCloudDataplexV1AssetDiscoverySpec ENABLED_ASSET_DISCOVERY_SPEC =
      new GoogleCloudDataplexV1AssetDiscoverySpec().setEnabled(true);
  private static final GoogleCloudDataplexV1AssetDiscoverySpec DISABLED_ASSET_DISCOVERY_SPEC =
      new GoogleCloudDataplexV1AssetDiscoverySpec().setEnabled(false);

  private static final GoogleCloudDataplexV1ZoneDiscoverySpec ENABLED_ZONE_DISCOVERY_SPEC =
      new GoogleCloudDataplexV1ZoneDiscoverySpec().setEnabled(true);
  private static final GoogleCloudDataplexV1ZoneDiscoverySpec DISABLED_ZONE_DISCOVERY_SPEC =
      new GoogleCloudDataplexV1ZoneDiscoverySpec().setEnabled(false);

  @Test
  public void testGetGetCloudStorageEntitiesByAssetName() throws IOException {
    CloudDataplex cloudDataplexClient = mock(CloudDataplex.class, Answers.RETURNS_DEEP_STUBS);
    Entities.List entitiesListRequest = mock(Entities.List.class, Answers.RETURNS_DEEP_STUBS);
    when(cloudDataplexClient.projects().locations().lakes().zones().entities().list(ZONE_NAME))
        .thenReturn(entitiesListRequest);

    GoogleCloudDataplexV1Entity entity1 =
        new GoogleCloudDataplexV1Entity()
            .setName("entity1")
            .setAsset(ASSET_NAME1)
            .setSystem(StorageSystem.CLOUD_STORAGE.name());
    GoogleCloudDataplexV1Entity entity2 =
        new GoogleCloudDataplexV1Entity()
            .setName("entity2")
            .setAsset(ASSET_NAME1)
            .setSystem("BIGQUERY"); // storage mismatch
    GoogleCloudDataplexV1Entity entity3 =
        new GoogleCloudDataplexV1Entity()
            .setName("entity3")
            .setAsset(ASSET_NAME1)
            .setSystem(StorageSystem.CLOUD_STORAGE.name());
    GoogleCloudDataplexV1Entity entity4 =
        new GoogleCloudDataplexV1Entity()
            .setName("entity4")
            .setAsset(ASSET_NAME2)
            .setSystem(StorageSystem.CLOUD_STORAGE.name()); // asset mismatch
    // an entity with a short asset name
    GoogleCloudDataplexV1Entity entity5 =
        new GoogleCloudDataplexV1Entity()
            .setName("entity5")
            .setAsset(SHORT_ASSET_NAME1)
            .setSystem(StorageSystem.CLOUD_STORAGE.name());
    GoogleCloudDataplexV1ListEntitiesResponse response1 =
        new GoogleCloudDataplexV1ListEntitiesResponse();
    response1.setEntities(ImmutableList.of(entity1, entity2, entity3));
    response1.setNextPageToken(PAGE_TOKEN);
    GoogleCloudDataplexV1ListEntitiesResponse response2 =
        new GoogleCloudDataplexV1ListEntitiesResponse();
    response2.setEntities(ImmutableList.of(entity4, entity5));
    when(entitiesListRequest.setPageToken(any())).thenReturn(entitiesListRequest);
    when(entitiesListRequest.execute()).thenReturn(response1, response2);

    when(cloudDataplexClient
            .projects()
            .locations()
            .lakes()
            .zones()
            .entities()
            .get("entity1")
            .setView(GetEntityRequestEntityView.FULL.name())
            .execute())
        .thenReturn(entity1);
    when(cloudDataplexClient
            .projects()
            .locations()
            .lakes()
            .zones()
            .entities()
            .get("entity3")
            .setView(GetEntityRequestEntityView.FULL.name())
            .execute())
        .thenReturn(entity3);
    when(cloudDataplexClient
            .projects()
            .locations()
            .lakes()
            .zones()
            .entities()
            .get("entity5")
            .setView(GetEntityRequestEntityView.FULL.name())
            .execute())
        .thenReturn(entity5);

    assertEquals(
        ImmutableList.of(entity1, entity3, entity5),
        DefaultDataplexClient.withClient(cloudDataplexClient).getCloudStorageEntities(ASSET_NAME1));
  }

  @Test
  public void testGetEntitiesByEntityNames() throws IOException {
    CloudDataplex cloudDataplexClient = mock(CloudDataplex.class, Answers.RETURNS_DEEP_STUBS);

    GoogleCloudDataplexV1Entity entity1 =
        new GoogleCloudDataplexV1Entity()
            .setName("entity1")
            .setAsset(ASSET_NAME1)
            .setSystem(StorageSystem.CLOUD_STORAGE.name());
    GoogleCloudDataplexV1Entity entity2 =
        new GoogleCloudDataplexV1Entity()
            .setName("entity2")
            .setAsset(ASSET_NAME1)
            .setSystem(StorageSystem.CLOUD_STORAGE.name());
    GoogleCloudDataplexV1Entity entity3 =
        new GoogleCloudDataplexV1Entity()
            .setName("entity3")
            .setAsset(ASSET_NAME1)
            .setSystem(StorageSystem.CLOUD_STORAGE.name());

    when(cloudDataplexClient
            .projects()
            .locations()
            .lakes()
            .zones()
            .entities()
            .get("entity1")
            .setView(GetEntityRequestEntityView.FULL.name())
            .execute())
        .thenReturn(entity1);
    when(cloudDataplexClient
            .projects()
            .locations()
            .lakes()
            .zones()
            .entities()
            .get("entity2")
            .setView(GetEntityRequestEntityView.FULL.name())
            .execute())
        .thenReturn(entity2);
    when(cloudDataplexClient
            .projects()
            .locations()
            .lakes()
            .zones()
            .entities()
            .get("entity3")
            .setView(GetEntityRequestEntityView.FULL.name())
            .execute())
        .thenReturn(entity3);

    assertEquals(
        ImmutableList.of(entity1, entity2, entity3),
        DefaultDataplexClient.withClient(cloudDataplexClient)
            .getEntities(ImmutableList.of("entity1", "entity2", "entity3")));
  }

  @Test
  public void testGetPartitionsByEntityName() throws IOException {
    CloudDataplex cloudDataplexClient = mock(CloudDataplex.class, Answers.RETURNS_DEEP_STUBS);
    Partitions partitions = mock(Partitions.class, Answers.RETURNS_DEEP_STUBS);
    when(cloudDataplexClient.projects().locations().lakes().zones().entities().partitions())
        .thenReturn(partitions);

    GoogleCloudDataplexV1Partition partition1 =
        new GoogleCloudDataplexV1Partition().setName("partition1");
    GoogleCloudDataplexV1Partition partition2 =
        new GoogleCloudDataplexV1Partition().setName("partition2");
    GoogleCloudDataplexV1Partition partition3 =
        new GoogleCloudDataplexV1Partition().setName("partition2");

    GoogleCloudDataplexV1ListPartitionsResponse response1 =
        new GoogleCloudDataplexV1ListPartitionsResponse()
            .setPartitions(ImmutableList.of(partition1, partition2))
            .setNextPageToken(PAGE_TOKEN);
    GoogleCloudDataplexV1ListPartitionsResponse response2 =
        new GoogleCloudDataplexV1ListPartitionsResponse()
            .setPartitions(ImmutableList.of(partition3));

    when(partitions.list("entity0").execute()).thenReturn(response1);
    when(partitions.list("entity0").setPageToken(eq(PAGE_TOKEN)).execute()).thenReturn(response2);

    assertEquals(
        ImmutableList.of(partition1, partition2, partition3),
        DefaultDataplexClient.withClient(cloudDataplexClient).getPartitions("entity0"));
  }

  @Test
  public void testCreateEntitiesWhenDiscoveryEnabled() throws IOException {
    CloudDataplex dataplex = mock(CloudDataplex.class, Answers.RETURNS_DEEP_STUBS);
    Zones zones = getZones(dataplex);
    Assets assets = getAssets(dataplex);
    // Don't care about the order they're checked, so set up for two calls.
    when(assets.get(any()).execute())
        .thenReturn(createAsset(ENABLED_ASSET_DISCOVERY_SPEC))
        .thenReturn(createAsset(DISABLED_ASSET_DISCOVERY_SPEC));
    when(zones.get(any()).execute())
        .thenReturn(createZone(ENABLED_ZONE_DISCOVERY_SPEC))
        .thenReturn(createZone(DISABLED_ZONE_DISCOVERY_SPEC));
    DataplexClient client = DefaultDataplexClient.withClient(dataplex);

    client.createMetadata(
        ASSET_NAME1, ImmutableList.of(UNUSED_METADATA), CreateBehavior.UPDATE_IF_EXISTS);
    client.createMetadata(
        ASSET_NAME1, ImmutableList.of(UNUSED_METADATA), CreateBehavior.UPDATE_IF_EXISTS);

    verify(assets, atLeastOnce()).get(ASSET_NAME1);
    verify(zones, atLeastOnce()).get(ZONE_NAME);
    verify(zones, never()).entities(); // Enough to know nothing was done with partitions
  }

  private static Zones getZones(CloudDataplex dataplex) {
    return dataplex.projects().locations().lakes().zones();
  }

  private static Assets getAssets(CloudDataplex dataplex) {
    return dataplex.projects().locations().lakes().zones().assets();
  }

  private static GoogleCloudDataplexV1Zone createZone(
      GoogleCloudDataplexV1ZoneDiscoverySpec discoverySpec) {
    return new GoogleCloudDataplexV1Zone().setDiscoverySpec(discoverySpec);
  }

  private static GoogleCloudDataplexV1Asset createAsset(
      GoogleCloudDataplexV1AssetDiscoverySpec discoverySpec) {
    return new GoogleCloudDataplexV1Asset().setDiscoverySpec(discoverySpec);
  }
}
