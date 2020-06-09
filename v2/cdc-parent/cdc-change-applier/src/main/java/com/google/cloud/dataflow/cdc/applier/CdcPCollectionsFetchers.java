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

package com.google.cloud.dataflow.cdc.applier;

import static com.google.cloud.dataflow.cdc.applier.PubsubUtils.buildTopicSubscriptionSchemas;

import com.google.cloud.dataflow.cdc.applier.PubsubUtils.TopicSubscriptionSchema;
import com.google.cloud.dataflow.cdc.common.DataCatalogSchemaUtils;
import com.google.common.base.Preconditions;
import com.google.pubsub.v1.ProjectTopicName;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Classes in this file are in charge of querying Data Catalog, and building
 * a collection of {@link org.apache.beam.sdk.values.PCollection}s with schema
 * information from each source table.
 */
class CdcPCollectionsFetchers {

  private static final Logger LOG = LoggerFactory.getLogger(CdcPCollectionsFetchers.class);

  public static abstract class CdcPCollectionFetcher {
    public abstract Map<String, PCollection<Row>> changelogPcollections(Pipeline p);
  }

  public static CdcPCollectionFetcher create(
      CdcToBigQueryChangeApplierPipeline.CdcApplierOptions options) {
    if (options.getUseSingleTopic()) {
      return new SingleTopicPCollectionFetcher(options);
    } else {
      return new MultitopicPCollectionFetcher(options);
    }
  }

  static class MultitopicPCollectionFetcher extends CdcPCollectionFetcher {

    private CdcToBigQueryChangeApplierPipeline.CdcApplierOptions options;

    MultitopicPCollectionFetcher(CdcToBigQueryChangeApplierPipeline.CdcApplierOptions options) {
      this.options = options;
    }

    public Map<String, PCollection<Row>> changelogPcollections(Pipeline p) {
      Map<String, PCollection<Row>> result = new HashMap<>();

      List<TopicSubscriptionSchema> readSourceSchemas = buildTopicSubscriptionSchemas(
          options.as(GcpOptions.class).getProject(),
          options.getInputTopics(),
          options.getInputSubscriptions());

      for (TopicSubscriptionSchema rss: readSourceSchemas) {
        String transformTopicPrefix = rss.topic;

        PCollection<PubsubMessage> pubsubData;
        if (rss.subscription == null) {
          pubsubData = p.apply(
              String.format("%s/Read Updates from PubSub", transformTopicPrefix),
              PubsubIO.readMessagesWithAttributes()
                  .fromTopic(String.format(
                      "projects/%s/topics/%s",
                      options.as(GcpOptions.class).getProject(), rss.topic)));
        } else {
          pubsubData = p.apply(
              String.format("%s/Read Updates from PubSub", transformTopicPrefix),
              PubsubIO.readMessagesWithAttributes().fromSubscription(String.format(
                  "projects/%s/subscriptions/%s",
                  options.as(GcpOptions.class).getProject(), rss.subscription)));
        }

        PCollection<Row> collectionOfRows = pubsubData
            .apply(String.format("%s/Extract payload", transformTopicPrefix),
                MapElements.into(TypeDescriptor.of(byte[].class))
                    .via(PubsubMessage::getPayload))
            .apply(
                String.format("%s/Decode", transformTopicPrefix),
                DecodeRows.withSchema(rss.schema));

        result.put(transformTopicPrefix, collectionOfRows);
      }
      return result;
    }
  }

  static class SingleTopicPCollectionFetcher extends CdcPCollectionFetcher {

    private CdcToBigQueryChangeApplierPipeline.CdcApplierOptions options;

    SingleTopicPCollectionFetcher(CdcToBigQueryChangeApplierPipeline.CdcApplierOptions options) {
      this.options = options;
    }

    private ProjectTopicName getPubSubTopic() {
      if (options.getInputSubscriptions() != null && !options.getInputSubscriptions().isEmpty()) {
        try {
          return PubsubUtils.getPubSubTopicFromSubscription(
              options.as(GcpOptions.class).getProject(), options.getInputSubscriptions());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else {
        Preconditions.checkArgument(
            options.getInputTopics() != null && !options.getInputTopics().isEmpty(),
            "Must provide an inputSubscriptions or inputTopics parameter.");
        return ProjectTopicName.of(
            options.as(GcpOptions.class).getProject(), options.getInputTopics());
      }
    }

    @Override
    public Map<String, PCollection<Row>> changelogPcollections(Pipeline p) {
      Map<String, PCollection<Row>> result = new HashMap<>();
      LOG.info("Generating changelog PCollections by multiplexing a single PubSub topic");

      // This should be a single topic
      ProjectTopicName pubsubTopic = getPubSubTopic();
      String entryGroupName = DataCatalogSchemaUtils.entryGroupNameForTopic(pubsubTopic.getTopic());

      Map<String, Schema> tableToSchema = DataCatalogSchemaUtils.getSchemasForEntryGroup(
          options.as(GcpOptions.class).getProject(),
          entryGroupName);

      if (tableToSchema == null) {
        throw new RuntimeException(
            "Unable to fetch table schemas from Cloud Data Catalog. Can't build the pipeline.");
      }

      LOG.info("Table to schema map for MySQL tables: {}", tableToSchema);

      PCollection<PubsubMessage> pubsubData;
      if (options.getInputTopics() != null && !options.getInputTopics().isEmpty()) {
        pubsubData = p
            .apply(PubsubIO.readMessagesWithAttributes()
                .fromTopic(String.format(
                    "projects/%s/topics/%s",
                    options.as(GcpOptions.class).getProject(), options.getInputTopics())));
      } else {
        Preconditions.checkArgument(options.getInputSubscriptions() != null,
            "Must provide an inputSubscriptions or inputTopics parameter.");
        pubsubData = p
            .apply(PubsubIO.readMessagesWithAttributes()
                .fromSubscription(String.format(
                    "projects/%s/subscriptions/%s",
                    options.as(GcpOptions.class).getProject(), options.getInputSubscriptions())));
      }

      for (Map.Entry<String, Schema> tableAndSchema : tableToSchema.entrySet()) {
        PCollection<Row> tableData = filterAndDecode(
            pubsubData, tableAndSchema.getKey(), tableAndSchema.getValue());
        result.put(tableAndSchema.getKey(), tableData);
      }

      return result;
    }

    private static PCollection<Row> filterAndDecode(
        PCollection<PubsubMessage> input, final String tableName, Schema tableSchema) {
      return input
          .apply(
              String.format("Filter_%s", tableName),
              Filter.by(
                  message ->
                      message.getAttribute("table") != null
                          && message.getAttribute("table").equals(tableName)))
          .apply(
              String.format("Extract payload_%s", tableName),
              MapElements.into(TypeDescriptor.of(byte[].class)).via(PubsubMessage::getPayload))
          .apply(String.format("Decode_%s", tableName), DecodeRows.withSchema(tableSchema));
    }
  }
}
