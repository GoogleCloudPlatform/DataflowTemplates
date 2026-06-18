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
import com.google.cloud.dataflow.cdc.common.DataCatalogSchemaUtils;
import com.google.cloud.dataflow.cdc.common.DataCatalogSchemaUtils.DataCatalogSchemaManager;
import com.google.cloud.dataplex.v1.Entry;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  public void testUpdateAndLookupSchema() throws Exception {
    String topicNameStr = testName + "-test-topic";
    TopicName topic = pubsubResourceManager.createTopicWithoutPrefix(topicNameStr);
    LOG.info("Created topic: {}", topic.toString());

    // Wait for the system to create the dataplex entry. Pub/Sub -> Dataplex synchronization can
    // take 10-30 seconds.
    LOG.info("Waiting 30 seconds for Dataplex to create system entry for topic...");
    TimeUnit.SECONDS.sleep(30);

    DataCatalogSchemaManager schemaManager =
        DataCatalogSchemaUtils.getSchemaManager(project, "", false);

    Schema testSchema = Schema.builder().addStringField("id").addInt32Field("age").build();

    LOG.info("Updating Dataplex schema...");
    Entry updatedEntry = schemaManager.updateSchemaForTable(topicNameStr, testSchema);

    // Retry once if Dataplex was too slow.
    if (updatedEntry == null) {
      LOG.warn("Initial schema update returned null. Waiting 15 more seconds and retrying...");
      TimeUnit.SECONDS.sleep(15);
      updatedEntry = schemaManager.updateSchemaForTable(topicNameStr, testSchema);
    }
    LOG.info("Looking up {}", updatedEntry);
    assertNotNull("Updated entry should not be null", updatedEntry);

    LOG.info("Looking up schema from Dataplex...");
    Schema retrievedSchema = DataCatalogSchemaUtils.getSchemaFromPubSubTopic(project, topicNameStr);
    assertNotNull("Retrieved schema should not be null", retrievedSchema);
    assertTrue(retrievedSchema.hasField("id"));
    assertTrue(retrievedSchema.hasField("age"));
  }

  @Test
  public void testUpdateAndLookupSchemaSingleTopic() throws Exception {
    String topicNameStr = testName + "-test-topic";
    TopicName topic = pubsubResourceManager.createTopicWithoutPrefix(topicNameStr);
    LOG.info("Created topic: {}", topic.toString());

    // Wait for the system to create the dataplex entry. Pub/Sub -> Dataplex synchronization can
    // take 10-30 seconds.
    LOG.info("Waiting 30 seconds for Dataplex to create system entry for topic...");
    TimeUnit.SECONDS.sleep(30);

    DataCatalogSchemaManager schemaManager =
        DataCatalogSchemaUtils.getSchemaManager(project, topicNameStr, true);

    Schema testSchema = Schema.builder().addStringField("id").addInt32Field("age").build();

    LOG.info("Updating Dataplex schema...");
    Entry updatedEntry = schemaManager.updateSchemaForTable("bazbaz", testSchema);

    // Retry once if Dataplex was too slow.
    if (updatedEntry == null) {
      LOG.warn("Initial schema update returned null. Waiting 15 more seconds and retrying...");
      TimeUnit.SECONDS.sleep(15);
      updatedEntry = schemaManager.updateSchemaForTable("bazbaz", testSchema);
    }
    LOG.info("Looking up {}", updatedEntry);
    assertNotNull("Updated entry should not be null", updatedEntry);
    String entryGroupName = DataCatalogSchemaUtils.entryGroupNameForTopic(topicNameStr);

    Map<String, Schema> tableToSchema =
        DataCatalogSchemaUtils.getSchemasForEntryGroup(project, entryGroupName);
    LOG.info("Looking up schema from Dataplex... {}", tableToSchema);
    assertNotNull("Retrieved schema should not be null", tableToSchema);
  }
}
