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
import com.google.cloud.dataplex.v1.Aspect;
import com.google.cloud.dataplex.v1.CatalogServiceClient;
import com.google.cloud.dataplex.v1.CreateEntryGroupRequest;
import com.google.cloud.dataplex.v1.CreateEntryRequest;
import com.google.cloud.dataplex.v1.Entry;
import com.google.cloud.dataplex.v1.EntryGroup;
import com.google.cloud.dataplex.v1.EntryGroupName;
import com.google.cloud.dataplex.v1.EntrySource;
import com.google.cloud.dataplex.v1.EntryView;
import com.google.cloud.dataplex.v1.GetEntryRequest;
import com.google.cloud.dataplex.v1.UpdateEntryRequest;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Struct;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
  private static final int MAX_ENTRY_GROUP_LIMIT = 63;

  public static String entryGroupNameForTopic(String topicOrPrefix) {
    String sanitized = topicOrPrefix.replace('_', '-').replace('.', '-').toLowerCase();
    if (sanitized.endsWith("-")) {
      sanitized = sanitized.substring(0, sanitized.length() - 1);
    }
    String entryGroupName;
    if (sanitized.startsWith("cdc-")) {
      entryGroupName = sanitized;
    } else {
      entryGroupName = "cdc-" + sanitized;
    }

    if (entryGroupName.length() > MAX_ENTRY_GROUP_LIMIT) {
      String prefix = entryGroupName.substring(0, 54); // 63 - 1 (dash) - 8 (hash)
      if (prefix.endsWith("-")) {
        prefix = prefix.substring(0, 53);
      }
      String hash =
          com.google.common.hash.Hashing.sha256()
              .hashString(entryGroupName, java.nio.charset.StandardCharsets.UTF_8)
              .toString()
              .substring(0, 8);
      entryGroupName = prefix + "-" + hash;
    }
    return entryGroupName;
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
        entry =
            client.getEntry(
                GetEntryRequest.newBuilder()
                    .setName(entry.getName())
                    .setView(EntryView.ALL)
                    .build());
        Struct schemaData = getSchemaAspectDataFromEntryGroup(entry);
        if (schemaData != null) {
          String description =
              entry.hasEntrySource() ? entry.getEntrySource().getDescription() : entry.getName();
          Schema beamSchema = SchemaUtils.toBeamSchema(schemaData);
          schemas.put(description, beamSchema);
        }
      }
    } catch (IOException e) {
      LOG.error("Unable to create a CatalogServiceClient", e);
      throw new RuntimeException("Unable to create a CatalogServiceClient", e);
    } catch (Exception e) {
      LOG.error("Failed to list entries: ", e);
      throw new RuntimeException("Failed to list entries for entry group " + entryGroupId, e);
    }
    return schemas;
  }

  static Struct getSchemaAspectDataFromEntryGroup(Entry entry) {
    for (Map.Entry<String, Aspect> aspectEntry : entry.getAspectsMap().entrySet()) {
      if (aspectEntry.getKey().endsWith(".global.schema")) {
        return aspectEntry.getValue().getData();
      }
    }

    return null;
  }

  public abstract static class DataCatalogSchemaManager implements AutoCloseable {
    final String gcpProject;
    final String location;
    CatalogServiceClient client;
    private final java.util.Set<String> createdEntryGroups = new java.util.HashSet<>();

    @Override
    public void close() {
      if (client != null) {
        client.close();
        client = null;
      }
    }

    public abstract String getEntryGroupId(String tableName);

    public abstract String getPubSubTopicForTable(String tableName);

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

    private void createEntryGroup(String entryGroupId) {
      if (this.createdEntryGroups.contains(entryGroupId)) {
        return;
      }
      setupDataCatalogClient();
      EntryGroup entryGroup =
          EntryGroup.newBuilder()
              .setDisplayName(String.format("CDC_Debezium_on_Dataflow_%s", entryGroupId))
              .setDescription(
                  "This EntryGroup represents a set of change streams from tables "
                      + "being replicated for CDC.")
              .build();

      CreateEntryGroupRequest entryGroupRequest =
          CreateEntryGroupRequest.newBuilder()
              .setParent(String.format("projects/%s/locations/%s", getGcpProject(), location))
              .setEntryGroupId(entryGroupId)
              .setEntryGroup(entryGroup)
              .build();

      try {
        LOG.info("Creating EntryGroup {}", entryGroupRequest);
        client.createEntryGroupAsync(entryGroupRequest).get(1, TimeUnit.MINUTES);
        LOG.info("Created EntryGroup: {}", entryGroupId);

        this.createdEntryGroups.add(entryGroupId);
      } catch (AlreadyExistsException e) {
        // EntryGroup already exists.
        this.createdEntryGroups.add(entryGroupId);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.error("Interrupted while creating EntryGroup", e);
        throw new RuntimeException("Interrupted while creating EntryGroup " + entryGroupId, e);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof AlreadyExistsException) {
          this.createdEntryGroups.add(entryGroupId);
        } else {
          LOG.error("Failed to create EntryGroup", e);
          throw new RuntimeException("Failed to create EntryGroup " + entryGroupId, e.getCause());
        }
      } catch (TimeoutException e) {
        LOG.error("Timeout while creating EntryGroup", e);
        throw new RuntimeException("Timeout while creating EntryGroup " + entryGroupId, e);
      }
    }

    private static String sanitizeEntryName(String tableName) {
      String unsanitizedEntry = tableName;
      if (tableName.contains(".")) {
        unsanitizedEntry = tableName.split("\\.", 2)[1];
      }
      return unsanitizedEntry.replace('_', '-').replace('.', '-').toLowerCase();
    }

    public Entry updateSchemaForTable(String tableName, Schema beamSchema) {
      String entryGroupId = getEntryGroupId(tableName);
      createEntryGroup(entryGroupId);

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
              .setParent(EntryGroupName.of(getGcpProject(), location, entryGroupId).toString())
              .setEntryId(sanitizeEntryName(tableName))
              .setEntry(entry)
              .build();

      try {
        LOG.info("Dataplex updating schema {}", createEntryRequest);
        return client.createEntry(createEntryRequest);
      } catch (AlreadyExistsException e) {
        // If it exists, update it
        String entryName =
            String.format(
                "projects/%s/locations/%s/entryGroups/%s/entries/%s",
                getGcpProject(), location, entryGroupId, sanitizeEntryName(tableName));

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
        LOG.info("Dataplex updating schema {}", updateRequest);
        return client.updateEntry(updateRequest);
      }
    }
  }

  static class SingleTopicSchemaManager extends DataCatalogSchemaManager {
    private final String pubsubTopic;

    SingleTopicSchemaManager(String gcpProject, String location, String pubsubTopic) {
      super(gcpProject, location);
      this.pubsubTopic = pubsubTopic;
    }

    @Override
    public String getEntryGroupId(String tableName) {
      return entryGroupNameForTopic(pubsubTopic);
    }

    @Override
    public String getPubSubTopicForTable(String tableName) {
      return pubsubTopic;
    }
  }

  static class MultiTopicSchemaManager extends DataCatalogSchemaManager {
    private final String pubsubTopicPrefix;

    MultiTopicSchemaManager(String gcpProject, String location, String pubsubTopicPrefix) {
      super(gcpProject, location);
      this.pubsubTopicPrefix = pubsubTopicPrefix;
    }

    @Override
    public String getEntryGroupId(String tableName) {
      return entryGroupNameForTopic(getPubSubTopicForTable(tableName));
    }

    @Override
    public String getPubSubTopicForTable(String tableName) {
      return String.format("%s%s", pubsubTopicPrefix, tableName);
    }
  }
}
