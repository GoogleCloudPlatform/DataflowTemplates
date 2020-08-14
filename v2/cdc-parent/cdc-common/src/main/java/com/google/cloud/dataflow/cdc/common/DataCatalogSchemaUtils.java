/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.cdc.common;

import com.google.api.client.util.Strings;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.datacatalog.v1.EntryGroupName;
import com.google.cloud.datacatalog.v1beta1.CreateEntryGroupRequest;
import com.google.cloud.datacatalog.v1beta1.CreateEntryRequest;
import com.google.cloud.datacatalog.v1beta1.DataCatalogClient;
import com.google.cloud.datacatalog.v1beta1.Entry;
import com.google.cloud.datacatalog.v1beta1.EntryGroup;
import com.google.cloud.datacatalog.v1beta1.ListEntriesResponse;
import com.google.cloud.datacatalog.v1beta1.ListEntriesRequest;
import com.google.cloud.datacatalog.v1beta1.LocationName;
import com.google.cloud.datacatalog.v1beta1.LookupEntryRequest;
import com.google.cloud.datacatalog.v1beta1.UpdateEntryRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class with utilities to communicate with Google Cloud Data Catalog. */
public class DataCatalogSchemaUtils {

  // TODO(pabloem)(#138): Avoid using a default location for schema catalog entities.
  public static final String DEFAULT_LOCATION = "us-central1";

  /**
   * Template for the URI of a Pub/Sub topic {@link Entry} in Data Catalog.
   */
  private static final String DATA_CATALOG_PUBSUB_URI_TEMPLATE =
      "//pubsub.googleapis.com/projects/%s/topics/%s";

  private static final Logger LOG = LoggerFactory.getLogger(DataCatalogSchemaUtils.class);

  /**
   * Builds an {@link EntryGroup} name for a particular pubsubTopic.
   *
   * This method is intended for use in single-topic mode, where an {@link EntryGroup} with multiple
   * {@link Entry}s is created for a single Pub/Sub topic.
   */
  public static String entryGroupNameForTopic(String pubsubTopic) {
    return String.format("cdc_%s", pubsubTopic);
  }

  /** Build a {@link DataCatalogSchemaManager} given certain parameters. */
  public static DataCatalogSchemaManager getSchemaManager(
      String gcpProject, String pubsubTopicPrefix, Boolean singleTopic) {
    return getSchemaManager(gcpProject, pubsubTopicPrefix, DEFAULT_LOCATION, singleTopic);
  }

  /** Build a {@link DataCatalogSchemaManager} given certain parameters. */
  public static DataCatalogSchemaManager getSchemaManager(
      String gcpProject, String pubsubTopicPrefix, String location, Boolean singleTopic) {
    if (singleTopic) {
      return new SingleTopicSchemaManager(gcpProject, location, pubsubTopicPrefix);
    } else {
      return new MultiTopicSchemaManager(gcpProject, location, pubsubTopicPrefix);
    }
  }

  /** Retrieve all of the {@link Schema}s associated to {@link Entry}s in an {@link EntryGroup}.*/
  public static Map<String, Schema> getSchemasForEntryGroup(
      String gcpProject, String entryGroupId) {
    DataCatalogClient client = null;
    try {
      client = DataCatalogClient.create();
    } catch (IOException e) {
      throw new RuntimeException("Unable to create a DataCatalogClient", e);
    }
    if (client == null) {
      return null;
    }

    String formattedParent = DataCatalogClient.formatEntryGroupName(
        gcpProject, DEFAULT_LOCATION, entryGroupId);

    List<Entry> entries = new ArrayList<>();
    ListEntriesRequest request = ListEntriesRequest.newBuilder()
            .setParent(formattedParent)
            .build();
    while (true) {
      ListEntriesResponse response = client.listEntriesCallable().call(request);
      entries.addAll(response.getEntriesList());
      String nextPageToken = response.getNextPageToken();
      if (!Strings.isNullOrEmpty(nextPageToken)) {
        request = request.toBuilder().setPageToken(nextPageToken).build();
      } else {
        break;
      }
    }

    LOG.debug("Fetched entries: {}", entries);

    return entries.stream()
        .collect(Collectors.toMap(
            Entry::getDescription, e -> SchemaUtils.toBeamSchema(e.getSchema())));
  }

  /**
   * Retrieve the {@link Schema} associated to a Pub/Sub topics in an.
   *
   * This method is to be used in multi-topic mode, where a single {@link Schema} is associated
   * to a single Pub/Sub topic.
   */
  public static Schema getSchemaFromPubSubTopic(String gcpProject, String pubsubTopic) {
    DataCatalogClient client = null;
    try {
      client = DataCatalogClient.create();
    } catch (IOException e) {
      throw new RuntimeException("Unable to create a DataCatalogClient", e);
    }
    if (client == null) {
      return null;
    }

    Entry entry = lookupPubSubEntry(client, pubsubTopic, gcpProject);
    if (entry == null) {
      return null; // TODO(pabloem) Handle a failed entry lookup
    }

    return SchemaUtils.toBeamSchema(entry.getSchema());
  }

  static Entry lookupPubSubEntry(DataCatalogClient client, String pubsubTopic, String gcpProject) {
    String linkedResource =
        String.format(DATA_CATALOG_PUBSUB_URI_TEMPLATE, gcpProject, pubsubTopic);

    LOG.info("Looking up LinkedResource {}", linkedResource);

    LookupEntryRequest request =
        LookupEntryRequest.newBuilder().setLinkedResource(linkedResource).build();

    try {
      Entry entry = client.lookupEntry(request);
      return entry;
    } catch (ApiException e) {
      System.out.println("CANT LOOKUP ENTRY" + e.toString());
      e.printStackTrace();
      LOG.error("ApiException thrown by Data Catalog API:", e);
      return null;
    }
  }

  /**
   * Abstract class to manage Data Catalog clients and schemas from the Debezium connector.
   *
   * It provides methods to store and retrieve schemas for given source tables in Data Catalog.
   */
  public abstract static class DataCatalogSchemaManager {
    final String gcpProject;
    final String location;
    DataCatalogClient client;

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
        client = DataCatalogClient.create();
      } catch (IOException e) {
        throw new RuntimeException("Unable to create a DataCatalogClient", e);
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
              .setDisplayName(
                  String.format("CDC_Debezium_on_Dataflow_%s", pubsubTopic))
              .setDescription("This EntryGroup represents a set of change streams from tables "
                                  + "being replicated for CDC.")
              .build();

      // Construct the EntryGroup request to be sent by the client.
      CreateEntryGroupRequest entryGroupRequest =
          CreateEntryGroupRequest.newBuilder()
              .setParent(
                  LocationName.of(getGcpProject(), location).toString())
              .setEntryGroupId(entryGroupNameForTopic(pubsubTopic))
              .setEntryGroup(entryGroup)
              .build();

      try {
        LOG.info("Creating EntryGroup {}", entryGroupRequest);
        EntryGroup createdEntryGroup = client.createEntryGroup(entryGroupRequest);
        LOG.info("Created EntryGroup: {}", createdEntryGroup.toString());

        this.entryGroupCreated = true;
      } catch (AlreadyExistsException e) {
        // EntryGroup already exists. There is no further action needed.
      }
    }

    @Override
    public String getPubSubTopicForTable(String tableName) {
      return pubsubTopic;
    }

    private static String sanitizeEntryName(String tableName) {
      // Remove the instance name
      String unsanitizedEntry = tableName.split("\\.", 2)[1];
      return unsanitizedEntry.replace('-','_').replace('.', '_');
    }

    @Override
    public Entry updateSchemaForTable(String tableName, Schema beamSchema) {
      LOG.debug("Converting Beam schema {} into a Data Catalog schema", beamSchema);
      com.google.cloud.datacatalog.v1beta1.Schema newEntrySchema =
          SchemaUtils.fromBeamSchema(beamSchema);
      LOG.info("Beam schema {} converted to Data Catalog schema {}", beamSchema, newEntrySchema);
      LOG.error("Entry group name: {}", EntryGroupName.of(
          getGcpProject(),
          location,
          entryGroupNameForTopic(pubsubTopic)).toString());

      CreateEntryRequest createEntryRequest =
          CreateEntryRequest.newBuilder()
              .setParent(
                  EntryGroupName.of(
                      getGcpProject(),
                      location,
                      entryGroupNameForTopic(pubsubTopic)).toString())
              .setEntryId(sanitizeEntryName(tableName))
              .setEntry(
                  Entry.newBuilder()
                      .setSchema(newEntrySchema)
                      .setDescription(tableName)
                      .setUserSpecifiedType("BEAM_ROW_DATA")
                      .setUserSpecifiedSystem("DATAFLOW_CDC_ON_DEBEZIUM_DATA")
                      .build())
              .build();

      LOG.info("CreateEntryRequest: {}", createEntryRequest.toString());

      try {
        return client.createEntry(createEntryRequest);
      } catch (AlreadyExistsException e) {
        // The Entry already exists. No further action is necessary.
        return createEntryRequest.getEntry();
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
        return null;  // TODO(pabloem) Handle a missing client
      }

      String pubsubTopic = getPubSubTopicForTable(tableName);

      Entry beforeChangeEntry = lookupPubSubEntry(client, pubsubTopic, this.gcpProject);
      if (beforeChangeEntry == null) {
        return null; // TODO(pabloem) Handle a failed entry lookup
      }
      LOG.info("Converting Beam schema {} into a Data Catalog schema", beamSchema);
      com.google.cloud.datacatalog.v1beta1.Schema newEntrySchema =
          SchemaUtils.fromBeamSchema(beamSchema);
      LOG.debug("Beam schema {} converted to Data Catalog schema {}", beamSchema, newEntrySchema);

      LOG.info("Publishing schema for table {} corresponding to topic {}",
          tableName, pubsubTopic);
      UpdateEntryRequest updateEntryRequest = UpdateEntryRequest.newBuilder()
          .setEntry(beforeChangeEntry.toBuilder().setSchema(newEntrySchema).build())
          .build();

      return client.updateEntry(updateEntryRequest);
    }

  }
}
