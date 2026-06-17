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

  public static final String DEFAULT_LOCATION = "us-central1";

  private static final Logger LOG = LoggerFactory.getLogger(DataCatalogSchemaUtils.class);

  public static String entryGroupNameForTopic(String pubsubTopic) {
    return String.format("cdc_%s", pubsubTopic).replace('-', '_').replace('.', '_');
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
    CatalogServiceClient client = null;
    try {
      client = CatalogServiceClient.create();
    } catch (IOException e) {
      throw new RuntimeException("Unable to create a CatalogServiceClient", e);
    }
    if (client == null) {
      return null;
    }

    String locationName = String.format("projects/%s/locations/%s", gcpProject, DEFAULT_LOCATION);
    String query = String.format("entry_group:%s", entryGroupId);

    SearchEntriesRequest request =
        SearchEntriesRequest.newBuilder().setName(locationName).setQuery(query).build();

    Map<String, Schema> schemas = new HashMap<>();

    try {
      CatalogServiceClient.SearchEntriesPagedResponse response = client.searchEntries(request);
      for (SearchEntriesResult result : response.iterateAll()) {
        String entryName = result.getDataplexEntry().getName();
        Entry entry = client.getEntry(entryName);
        if (entry.containsAspects("dataplex-types.global.schema")) {
          Struct schemaData = entry.getAspectsMap().get("dataplex-types.global.schema").getData();
          String description =
              entry.hasEntrySource() ? entry.getEntrySource().getDescription() : entry.getName();
          schemas.put(description, SchemaUtils.toBeamSchema(schemaData));
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to search entries: ", e);
    }

    return schemas;
  }

  public static Schema getSchemaFromPubSubTopic(String gcpProject, String pubsubTopic) {
    CatalogServiceClient client = null;
    try {
      client = CatalogServiceClient.create();
    } catch (IOException e) {
      throw new RuntimeException("Unable to create a CatalogServiceClient", e);
    }
    if (client == null) {
      return null;
    }

    Entry entry = lookupPubSubEntry(client, pubsubTopic, gcpProject);
    if (entry == null) {
      return null;
    }

    if (entry.containsAspects("dataplex-types.global.schema")) {
      return SchemaUtils.toBeamSchema(
          entry.getAspectsMap().get("dataplex-types.global.schema").getData());
    }
    return null;
  }

  static Entry lookupPubSubEntry(
      CatalogServiceClient client, String pubsubTopic, String gcpProject) {
    String locationName = String.format("projects/%s/locations/%s", gcpProject, DEFAULT_LOCATION);
    String query = String.format("name:%s", pubsubTopic.replace('-', '_').replace('.', '_'));

    SearchEntriesRequest request =
        SearchEntriesRequest.newBuilder().setName(locationName).setQuery(query).build();

    try {
      CatalogServiceClient.SearchEntriesPagedResponse response = client.searchEntries(request);
      for (SearchEntriesResult result : response.iterateAll()) {
        return client.getEntry(result.getDataplexEntry().getName());
      }
    } catch (ApiException e) {
      LOG.error("ApiException thrown by Dataplex API:", e);
    }
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
      } catch (InterruptedException | ExecutionException e) {
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
      return unsanitizedEntry.replace('-', '_').replace('.', '_');
    }

    @Override
    public Entry updateSchemaForTable(String tableName, Schema beamSchema) {
      Struct schemaData = SchemaUtils.fromBeamSchema(beamSchema);

      Entry entry =
          Entry.newBuilder()
              .setEntryType("projects/dataplex-types/locations/global/entryTypes/generic")
              .setEntrySource(
                  EntrySource.newBuilder()
                      .setDescription(tableName)
                      .setSystem("DATAFLOW_CDC_ON_DEBEZIUM_DATA")
                      .putLabels("user_specified_type", "BEAM_ROW_DATA")
                      .build())
              .putAspects(
                  "dataplex-types.global.schema",
                  Aspect.newBuilder()
                      .setAspectType("projects/dataplex-types/locations/global/aspectTypes/schema")
                      .setData(schemaData)
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
                        .addPaths("entry_source.system")
                        .addPaths("entry_source.labels")
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
        return null;
      }

      String pubsubTopic = getPubSubTopicForTable(tableName);
      Entry beforeChangeEntry = lookupPubSubEntry(client, pubsubTopic, this.gcpProject);

      if (beforeChangeEntry == null) {
        return null;
      }

      Struct schemaData = SchemaUtils.fromBeamSchema(beamSchema);

      Entry updatedEntry =
          beforeChangeEntry.toBuilder()
              .putAspects(
                  "dataplex-types.global.schema",
                  Aspect.newBuilder()
                      .setAspectType("projects/dataplex-types/locations/global/aspectTypes/schema")
                      .setData(schemaData)
                      .build())
              .build();

      UpdateEntryRequest updateEntryRequest =
          UpdateEntryRequest.newBuilder()
              .setEntry(updatedEntry)
              .setUpdateMask(FieldMask.newBuilder().addPaths("aspects").build())
              .build();

      return client.updateEntry(updateEntryRequest);
    }
  }
}
