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

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.datacatalog.v1beta1.DataCatalogClient;
import com.google.cloud.datacatalog.v1beta1.Entry;
import com.google.cloud.datacatalog.v1beta1.LookupEntryRequest;
import com.google.cloud.datacatalog.v1beta1.UpdateEntryRequest;
import java.io.IOException;
import org.apache.beam.sdk.schemas.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class with utilities to communicate with Google Cloud Data Catalog. */
public class DataCatalogSchemaUtils {

  private static final String DATA_CATALOG_PUBSUB_URI_TEMPLATE =
      "//pubsub.googleapis.com/projects/%s/topics/%s";

  private DataCatalogClient client;

  private static final Logger LOG = LoggerFactory.getLogger(DataCatalogSchemaUtils.class);

  public Entry setSchemaForPubSubTopic(String pubsubTopic, String gcpProject, Schema beamSchema) {
    setupDataCatalogClient();
    if (client == null) {
      return null; // TODO(pabloem) Handle a missing client
    }

    Entry beforeChangeEntry = lookupPubSubEntry(pubsubTopic, gcpProject);
    if (beforeChangeEntry == null) {
      return null; // TODO(pabloem) Handle a failed entry lookup
    }
    LOG.info("Converting Beam schema {} into a Data Catalog schema", beamSchema);
    com.google.cloud.datacatalog.v1beta1.Schema newEntrySchema =
        SchemaUtils.fromBeamSchema(beamSchema);
    LOG.debug("Beam schema {} converted to Data Catalog schema {}", beamSchema, newEntrySchema);

    UpdateEntryRequest updateEntryRequest = UpdateEntryRequest.newBuilder()
        .setEntry(beforeChangeEntry.toBuilder().setSchema(newEntrySchema).build())
        .build();

    Entry entryAfterChange = client.updateEntry(updateEntryRequest);
    return entryAfterChange;
  }

  public Schema getSchemaFromPubSubTopic(String gcpProject, String pubsubTopic) {
    setupDataCatalogClient();
    if (client == null) {
      return null; // TODO(pabloem) Handle a missing client
    }

    Entry entry = lookupPubSubEntry(pubsubTopic, gcpProject);
    if (entry == null) {
      return null; // TODO(pabloem) Handle a failed entry lookup
    }

    Schema outputSchema = SchemaUtils.toBeamSchema(entry.getSchema());
    return outputSchema;
  }

  private void setupDataCatalogClient() {
    if (client != null) {
      return;
    }

    try {
      client = DataCatalogClient.create();
    } catch (IOException e) {
      System.out.println("CANT CREATE CLIENTE" + e.toString());
      e.printStackTrace();
      LOG.error("Unable to create a DataCatalogClient.", e);
    }
  }

  private Entry lookupPubSubEntry(String pubsubTopic, String gcpProject) {
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

}
