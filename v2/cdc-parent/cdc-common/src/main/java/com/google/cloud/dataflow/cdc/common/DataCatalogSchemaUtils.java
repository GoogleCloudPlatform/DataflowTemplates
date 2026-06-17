/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.dataflow.cdc.common;

import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.dataplex.v1.Aspect;
import com.google.cloud.dataplex.v1.CatalogServiceClient;
import com.google.cloud.dataplex.v1.CreateEntryGroupRequest;
import com.google.cloud.dataplex.v1.CreateEntryRequest;
import com.google.cloud.dataplex.v1.Entry;
import com.google.cloud.dataplex.v1.EntryGroup;
import com.google.cloud.dataplex.v1.EntryGroupName;
import com.google.cloud.dataplex.v1.EntrySource;
import com.google.cloud.dataplex.v1.SearchEntriesRequest;
import com.google.cloud.dataplex.v1.SearchEntriesResult;
import com.google.cloud.dataplex.v1.UpdateEntryRequest;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Struct;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.schemas.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class with utilities to communicate with Google Cloud Dataplex Catalog. */
public class DataCatalogSchemaUtils {

  public static final String DEFAULT_LOCATION = "global";

  private static final Logger LOG = LoggerFactory.getLogger(DataCatalogSchemaUtils.class);

  /**
   * Builds an {@link EntryGroup} name for a particular pubsubTopic.
   *
   * <p>This method is intended for use in single-topic mode, where an {@link EntryGroup} with
   * multiple {@link Entry}s is created for a single Pub/Sub topic.
   */
  public static String entryGroupNameForTopic(String pubsubTopic) {
    return String.format("cdc-%s", pubsubTopic).replace('_', '-').replace('.', '-').toLowerCase();
  }

  public static DataCatalogSchemaManager getSchemaManager(
      String gcpProject, String pubsubTopicPrefix, Boolean singleTopic) {
    return getSchemaManager(gcpProject, pubsubTopicPrefix, DEFAULT_LOCATION, singleTopic);
  }

  public static DataCatalogSchemaManager getSchemaManager(
      String gcpProject, String pubsubTopicPrefix, String location, Boolean singleTopic) {
    if (singleTopic) {
      return new SingleTopicSchemaManager(gcpProject, location, pubsubTopicPrefix);
    } else {
      return new MultiTopicSchemaManager(gcpProject, location, pubsubTopicPrefix);
    }
  }

  public static Map<String, Schema> getSchemasForEntryGroup(
      String gcpProject, String entryGroupId) {
    Map<String, Schema> schemas = new HashMap<>();
    try (CatalogServiceClient client = CatalogServiceClient.create()) {
      String formattedParent =
          String.format(
              "projects/%s/locations/%s/entryGroups/%s",
              gcpProject, DEFAULT_LOCATION, entryGroupId);
      com.google.cloud.dataplex.v1.ListEntriesRequest request =
          com.google.cloud.dataplex.v1.ListEntriesRequest.newBuilder()
              .setParent(formattedParent)
              .build();
      CatalogServiceClient.ListEntriesPagedResponse response = client.listEntries(request);
      for (Entry entry : response.iterateAll()) {
        Struct schemaData = getSchemaAspectData(entry);
        if (schemaData != null) {
          String description =
              entry.hasEntrySource() ? entry.getEntrySource().getDescription() : entry.getName();
          schemas.put(description, SchemaUtils.toBeamSchema(schemaData));
        }
      }
    } catch (IOException e) {
      LOG.error("Unable to create a CatalogServiceClient", e);
      throw new RuntimeException("Unable to create a CatalogServiceClient", e);
    } catch (Exception e) {
      LOG.error("Failed to list entries: ", e);
    }
    return schemas;
  }

  public static Schema getSchemaFromPubSubTopic(String gcpProject, String pubsubTopic) {
    try (CatalogServiceClient client = CatalogServiceClient.create()) {
      Entry entry = lookupPubSubEntry(client, pubsubTopic, gcpProject);
      if (entry == null) {
        LOG.warn("PubSub entry not found, returning null schema");
        return null;
      }

      Struct schemaData = getSchemaAspectData(entry);
      if (schemaData != null) {
        return SchemaUtils.toBeamSchema(schemaData);
      }
      LOG.warn("PubSub entry without aspect, returning null schema, {}", entry);
      return null;
    } catch (IOException e) {
      LOG.error("Unable to create a CatalogServiceClient", e);
      throw new RuntimeException("Unable to create a CatalogServiceClient", e);
    }
  }

  static Struct getSchemaAspectData(Entry entry) {
    for (Map.Entry<String, Aspect> aspectEntry : entry.getAspectsMap().entrySet()) {
      if (aspectEntry.getKey().endsWith(".global.schema") || aspectEntry.getKey().equals("dataplex-types.global.schema")) {
        return aspectEntry.getValue().getData();
      }
    }
    return null;
  }

  static Entry lookupPubSubEntry(
      CatalogServiceClient client, String pubsubTopic, String gcpProject) {
    String locationName = String.format("projects/%s/locations/%s", gcpProject, DEFAULT_LOCATION);
    String query = String.format("name:%s", pubsubTopic);

    SearchEntriesRequest request =
        SearchEntriesRequest.newBuilder().setName(locationName).setQuery(query).build();

    try {
      CatalogServiceClient.SearchEntriesPagedResponse response = client.searchEntries(request);
      for (SearchEntriesResult result : response.iterateAll()) {
        String entryName = result.getDataplexEntry().getName();
        LOG.info("Dataplex entry found {}", entryName);
        return client.getEntry(entryName);
      }
    } catch (ApiException e) {
      LOG.error("ApiException thrown by Dataplex API:", e);
    }
    LOG.warn("PubSub entry not found");
    return null;
  }

  public abstract static class DataCatalogSchemaManager {
    final String gcpProject;
    final String location;
    CatalogServiceClient client;

    public abstract String getPubSubTopicForTable(String tableName);

    public abstract Entry updateSchemaForTable(String tableName, Schema beamSchema);

    public String getGcpProject() {
      return gcpProject;
    }

    DataCatalogSchemaManager(String gcpProject, String location) {
      this.gcpProject = gcpProject;
      this.location = location;
    }

    void setupDataCatalogClient() {
      if (client != null) {
        return;
      }

      try {
        client = CatalogServiceClient.create();
      } catch (IOException e) {
        throw new RuntimeException("Unable to create a CatalogServiceClient", e);
      }
    }
  }

  static class SingleTopicSchemaManager extends DataCatalogSchemaManager {
    private final String pubsubTopic;
    private Boolean entryGroupCreated = false;

    SingleTopicSchemaManager(String gcpProject, String location, String pubsubTopic) {
      super(gcpProject, location);
      this.pubsubTopic = pubsubTopic;
      createEntryGroup();
    }

    private void createEntryGroup() {
      if (this.entryGroupCreated) {
        return;
      }
      setupDataCatalogClient();
      EntryGroup entryGroup =
          EntryGroup.newBuilder()
              .setDisplayName(String.format("CDC_Debezium_on_Dataflow_%s", pubsubTopic))
              .setDescription(
                  "This EntryGroup represents a set of change streams from tables "
                      + "being replicated for CDC.")
              .build();

      CreateEntryGroupRequest entryGroupRequest =
          CreateEntryGroupRequest.newBuilder()
              .setParent(String.format("projects/%s/locations/%s", getGcpProject(), location))
              .setEntryGroupId(entryGroupNameForTopic(pubsubTopic))
              .setEntryGroup(entryGroup)
              .build();

      try {
        LOG.info("Creating EntryGroup {}", entryGroupRequest);
        client.createEntryGroupAsync(entryGroupRequest).get();
        LOG.info("Created EntryGroup: {}", entryGroupNameForTopic(pubsubTopic));

        this.entryGroupCreated = true;
      } catch (AlreadyExistsException e) {
        // EntryGroup already exists.
        this.entryGroupCreated = true;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.error("Interrupted while creating EntryGroup", e);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof AlreadyExistsException) {
          this.entryGroupCreated = true;
        } else {
          LOG.error("Failed to create EntryGroup", e);
        }
      }
    }

    @Override
    public String getPubSubTopicForTable(String tableName) {
      return pubsubTopic;
    }

    private static String sanitizeEntryName(String tableName) {
      String unsanitizedEntry = tableName;
      if (tableName.contains(".")) {
        unsanitizedEntry = tableName.split("\\.", 2)[1];
      }
      return unsanitizedEntry.replace('_', '-').replace('.', '-').toLowerCase();
    }

    @Override
    public Entry updateSchemaForTable(String tableName, Schema beamSchema) {
      Struct schemaData = SchemaUtils.fromBeamSchema(beamSchema);
      Struct genericData =
          Struct.newBuilder()
              .putFields(
                  "system",
                  com.google.protobuf.Value.newBuilder()
                      .setStringValue("DATAFLOW_CDC_ON_DEBEZIUM_DATA")
                      .build())
              .putFields(
                  "type",
                  com.google.protobuf.Value.newBuilder().setStringValue("BEAM_ROW_DATA").build())
              .build();

      Entry entry =
          Entry.newBuilder()
              .setEntryType("projects/dataplex-types/locations/global/entryTypes/generic")
              .setEntrySource(EntrySource.newBuilder().setDescription(tableName).build())
              .putAspects(
                  "dataplex-types.global.schema",
                  Aspect.newBuilder()
                      .setAspectType("projects/dataplex-types/locations/global/aspectTypes/schema")
                      .setData(schemaData)
                      .build())
              .putAspects(
                  "dataplex-types.global.generic",
                  Aspect.newBuilder()
                      .setAspectType("projects/dataplex-types/locations/global/aspectTypes/generic")
                      .setData(genericData)
                      .build())
              .build();

      CreateEntryRequest createEntryRequest =
          CreateEntryRequest.newBuilder()
              .setParent(
                  EntryGroupName.of(getGcpProject(), location, entryGroupNameForTopic(pubsubTopic))
                      .toString())
              .setEntryId(sanitizeEntryName(tableName))
              .setEntry(entry)
              .build();

      try {
        return client.createEntry(createEntryRequest);
      } catch (AlreadyExistsException e) {
        // If it exists, update it
        String entryName =
            String.format(
                "projects/%s/locations/%s/entryGroups/%s/entries/%s",
                getGcpProject(),
                location,
                entryGroupNameForTopic(pubsubTopic),
                sanitizeEntryName(tableName));

        Entry updateEntry = entry.toBuilder().setName(entryName).build();
        UpdateEntryRequest updateRequest =
            UpdateEntryRequest.newBuilder()
                .setEntry(updateEntry)
                .setUpdateMask(
                    FieldMask.newBuilder()
                        .addPaths("aspects")
                        .addPaths("entry_source.description")
                        .build())
                .build();
        return client.updateEntry(updateRequest);
      }
    }
  }

  static class MultiTopicSchemaManager extends DataCatalogSchemaManager {

    private final String pubsubTopicPrefix;

    MultiTopicSchemaManager(String gcpProject, String location, String pubsubTopicPrefix) {
      super(gcpProject, location);
      this.pubsubTopicPrefix = pubsubTopicPrefix;
    }

    @Override
    public String getPubSubTopicForTable(String tableName) {
      return String.format("%s%s", pubsubTopicPrefix, tableName);
    }

    public Entry updateSchemaForTable(String tableName, Schema beamSchema) {
      setupDataCatalogClient();
      if (client == null) {
        LOG.warn("Knowledge Catalog client missing");
        return null;
      }

      String pubsubTopic = getPubSubTopicForTable(tableName);
      Entry beforeChangeEntry = lookupPubSubEntry(client, pubsubTopic, this.gcpProject);

      if (beforeChangeEntry == null) {
        LOG.warn("No entry for PubSub topic {}", pubsubTopic);
        return null;
      }

      Struct schemaData = SchemaUtils.fromBeamSchema(beamSchema);
      Struct genericData =
          Struct.newBuilder()
              .putFields(
                  "system",
                  com.google.protobuf.Value.newBuilder()
                      .setStringValue("DATAFLOW_CDC_ON_DEBEZIUM_DATA")
                      .build())
              .putFields(
                  "type",
                  com.google.protobuf.Value.newBuilder().setStringValue("BEAM_ROW_DATA").build())
              .build();

      Entry updatedEntry =
          beforeChangeEntry.toBuilder()
              .putAspects(
                  "dataplex-types.global.schema",
                  Aspect.newBuilder()
                      .setAspectType("projects/dataplex-types/locations/global/aspectTypes/schema")
                      .setData(schemaData)
                      .build())
              .putAspects(
                  "dataplex-types.global.generic",
                  Aspect.newBuilder()
                      .setAspectType("projects/dataplex-types/locations/global/aspectTypes/generic")
                      .setData(genericData)
                      .build())
              .build();

      UpdateEntryRequest updateEntryRequest =
          UpdateEntryRequest.newBuilder()
              .setEntry(updatedEntry)
              .setUpdateMask(FieldMask.newBuilder().addPaths("aspects").build())
              .build();
      LOG.info("Dataplex updating schema {}", updateEntryRequest);
      return client.updateEntry(updateEntryRequest);
    }
  }
}
