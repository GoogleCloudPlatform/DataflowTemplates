/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.dataflow.cdc.applier;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.dataflow.cdc.common.KnowledgeCatalogSchemaUtils;
import com.google.cloud.dataflow.cdc.common.SchemaUtils;
import com.google.cloud.dataplex.v1.Aspect;
import com.google.cloud.dataplex.v1.CatalogServiceClient;
import com.google.cloud.dataplex.v1.Entry;
import com.google.cloud.dataplex.v1.EntryView;
import com.google.cloud.dataplex.v1.GetEntryRequest;
import com.google.cloud.dataplex.v1.SearchEntriesRequest;
import com.google.cloud.dataplex.v1.SearchEntriesResult;
import com.google.cloud.dataplex.v1.UpdateEntryRequest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Struct;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(TemplateIntegrationTest.class)
@RunWith(JUnit4.class)
public class TopicDataplexSchemaIT {

  private static final Logger LOG = LoggerFactory.getLogger(TopicDataplexSchemaIT.class);
  private PubsubResourceManager pubsubResourceManager;
  private String project;
  private String testName;

  @Before
  public void setUp() throws IOException {
    project = System.getProperty("project", "radoslaws-playground-pso");
    testName = "test-dataplex-" + System.currentTimeMillis();
    CredentialsProvider credentialsProvider =
        FixedCredentialsProvider.create(GoogleCredentials.getApplicationDefault());
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, project, credentialsProvider).build();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(pubsubResourceManager);
  }

  @Test
  public void testReuseEmptySchema() {
    CatalogServiceClient client = null;
    try {
      client = CatalogServiceClient.create();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    String locationName = String.format("projects/%s/locations/%s", project, "global");
    String query = String.format("name:%s", "Tadasdas_fd-fds.4343");

    SearchEntriesRequest request =
        SearchEntriesRequest.newBuilder().setName(locationName).setQuery(query).build();
    Entry entry = null;

    CatalogServiceClient.SearchEntriesPagedResponse response = client.searchEntries(request);
    for (SearchEntriesResult result : response.iterateAll()) {
      String entryName = result.getDataplexEntry().getName();
      LOG.info("Dataplex entry found {}", entryName);
      entry =
          client.getEntry(
              GetEntryRequest.newBuilder().setName(entryName).setView(EntryView.ALL).build());
    }
    Map<String, Aspect> aspectsMap = entry.getAspectsMap();
    Optional<Map.Entry<String, Aspect>> first =
        aspectsMap.entrySet().stream()
            .filter(p -> p.getKey().endsWith(".global.schema"))
            .findFirst();
    String keyToUpdate = null;
    Schema testSchema = Schema.builder().addStringField("id").addInt32Field("age").build();
    Struct schemaData = SchemaUtils.fromBeamSchema(testSchema);
    // LOG.info("new schema {}", schemaData);
    Map<String, Aspect> mutableAspects = new HashMap<>(aspectsMap);
    if (first.isPresent()) {
      // LOG.info("found {} ", first.get().getValue());
      keyToUpdate = first.get().getKey();
      Aspect a = first.get().getValue();
      Aspect build = a.toBuilder().setData(schemaData).build();
      mutableAspects.put(keyToUpdate, build);
      LOG.info("new aspect {}", build);

    } else {
      keyToUpdate = "dataplex-types.global.schema";
      mutableAspects.put(
          keyToUpdate,
          Aspect.newBuilder()
              .setAspectType("projects/dataplex-types/locations/global/aspectTypes/schema")
              .setData(schemaData)
              .build());
    }

    Entry updatedEntry = entry.toBuilder().clearAspects().putAllAspects(mutableAspects).build();

    UpdateEntryRequest updateEntryRequest =
        UpdateEntryRequest.newBuilder()
            .setEntry(updatedEntry)
            .addAllAspectKeys(List.of(keyToUpdate))
            .setUpdateMask(FieldMask.newBuilder().addPaths("aspects").build())
            .build();
    LOG.info("Dataplex updating schema {}", updateEntryRequest);
    Entry entry1 = client.updateEntry(updateEntryRequest);
    LOG.info("Dataplex updating schema {}", entry1);
  }

  @Test
  public void testUpdateAndLookupSchema() throws Exception {
    String prefix = testName + "-";
    String tableName = "my_table";
    String expectedTopic = prefix + tableName;

    KnowledgeCatalogSchemaUtils schemaManager =
        KnowledgeCatalogSchemaUtils.getSchemaManager(project, prefix, false);

    Schema testSchema = Schema.builder().addStringField("id").addInt32Field("age").build();

    LOG.info("Updating Dataplex schema...");
    Entry updatedEntry = schemaManager.updateSchemaForTable(tableName, testSchema);

    LOG.info("Looking up {}", updatedEntry);
    assertNotNull("Updated entry should not be null", updatedEntry);

    LOG.info("Looking up schema from Dataplex...");
    Schema retrievedSchema =
        KnowledgeCatalogSchemaUtils.getSchemaFromPubSubTopic(project, expectedTopic);
    assertNotNull("Retrieved schema should not be null", retrievedSchema);
    assertTrue(retrievedSchema.hasField("id"));
    assertTrue(retrievedSchema.hasField("age"));
  }

  @Test
  public void testUpdateAndLookupSchemaSingleTopic() throws Exception {
    String topicNameStr = testName + "-test-topic";

    KnowledgeCatalogSchemaUtils schemaManager =
        KnowledgeCatalogSchemaUtils.getSchemaManager(project, topicNameStr, true);

    Schema testSchema = Schema.builder().addStringField("id").addInt32Field("age").build();
    LOG.info("Updating Dataplex schema...");
    Entry updatedEntry = schemaManager.updateSchemaForTable("bazbaz", testSchema);

    LOG.info("Looking up {}", updatedEntry);
    assertNotNull("Updated entry should not be null", updatedEntry);
    String entryGroupName = KnowledgeCatalogSchemaUtils.entryGroupNameForTopic(topicNameStr);

    Map<String, Schema> tableToSchema =
        KnowledgeCatalogSchemaUtils.getSchemasForEntryGroup(project, entryGroupName);
    LOG.info("Looking up schema from Dataplex... {}", tableToSchema);
    assertNotNull("Retrieved schema should not be null", tableToSchema);
    assertTrue(tableToSchema.containsKey("bazbaz"));
  }

  @Test
  public void testUpdateAndLookupSchemaWithLongName() throws Exception {
    String longPrefix =
        "a-very-long-prefix-that-exceeds-the-sixty-three-character-limit-for-dataplex-";
    String tableName = "my_table";
    String expectedTopic = longPrefix + tableName;

    KnowledgeCatalogSchemaUtils schemaManager =
        KnowledgeCatalogSchemaUtils.getSchemaManager(project, longPrefix, false);

    Schema testSchema = Schema.builder().addStringField("id").addInt32Field("age").build();

    LOG.info("Updating Dataplex schema with long name...");
    Entry updatedEntry = schemaManager.updateSchemaForTable(tableName, testSchema);
    assertNotNull("Updated entry should not be null", updatedEntry);

    LOG.info("Looking up schema from Dataplex...");
    String entryGroupName = KnowledgeCatalogSchemaUtils.entryGroupNameForTopic(expectedTopic);
    Map<String, Schema> tableToSchema =
        KnowledgeCatalogSchemaUtils.getSchemasForEntryGroup(project, entryGroupName);
    assertNotNull("Retrieved schema should not be null", tableToSchema);
  }
}
