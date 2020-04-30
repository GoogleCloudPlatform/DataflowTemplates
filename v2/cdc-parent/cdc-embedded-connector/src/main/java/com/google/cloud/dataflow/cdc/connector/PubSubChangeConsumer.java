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
package com.google.cloud.dataflow.cdc.connector;

import com.google.api.core.ApiFuture;
import com.google.cloud.datacatalog.Entry;
import com.google.cloud.dataflow.cdc.common.DataCatalogSchemaUtils;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.embedded.EmbeddedEngine.RecordCommitter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.LoggerFactory;


public class PubSubChangeConsumer implements EmbeddedEngine.ChangeConsumer {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(PubSubChangeConsumer.class);

  public static final SerializableFunction<ProjectTopicName, Publisher>
      DEFAULT_PUBLISHER_FACTORY = projectTopicName -> {
    try {
      return Publisher
          .newBuilder(projectTopicName)
          .build();
    } catch (IOException e) {
      LOG.error("Unable to create a PubSub Publisher", e);
      return null;
    }
  };

  private final Map<String, Publisher> pubsubPublisherMap;
  private final Map<String, RowCoder> rowCoderMap;

  private final String project;
  private final String pubsubTopicPrefix;
  private final Set<String> whitelistedTables;
  private final Set<String> observedTables;
  private final DataCatalogSchemaUtils schemaUpdater;
  private final SerializableFunction<ProjectTopicName, Publisher> pubSubPublisherFactory;
  private final DebeziumSourceRecordToDataflowCdcFormatTranslator translator =
      new DebeziumSourceRecordToDataflowCdcFormatTranslator();

  public PubSubChangeConsumer(
      String project,
      String pubsubTopicPrefix,
      Set<String> whitelistedTables,
      DataCatalogSchemaUtils schemaUpdater,
      SerializableFunction<ProjectTopicName, Publisher> pubSubPublisherFactory) {
    this.project = project;
    this.pubsubTopicPrefix = pubsubTopicPrefix;
    this.whitelistedTables = whitelistedTables;
    this.observedTables = new HashSet<>();
    this.pubsubPublisherMap = new HashMap<>();
    this.rowCoderMap = new HashMap<>();
    this.schemaUpdater = schemaUpdater;
    this.pubSubPublisherFactory = pubSubPublisherFactory;
  }

  public static String getPubsubTopicName(String pubsubTopicPrefix, String tableName) {
    return String.format("%s%s", pubsubTopicPrefix, tableName);
  }

  private Publisher getPubSubPublisher(String tableName) {
    if (!pubsubPublisherMap.containsKey(tableName)) {
      String topicName = getPubsubTopicName(pubsubTopicPrefix, tableName);

      Publisher result = pubSubPublisherFactory.apply(ProjectTopicName.of(this.project, topicName));
      pubsubPublisherMap.put(tableName, result);
      return result;
    }

    return pubsubPublisherMap.get(tableName);
  }

  private RowCoder getCoderForRow(String tableName, Row record) {
    if (!rowCoderMap.containsKey(tableName)) {
      RowCoder coderForTableTopic = RowCoder.of(record.getSchema());
      rowCoderMap.put(tableName, coderForTableTopic);
    }

    return rowCoderMap.get(tableName);
  }

  @Override
  public void handleBatch(
      List<SourceRecord> records, RecordCommitter committer) throws InterruptedException {

    ImmutableList.Builder<ApiFuture<String>> futureListBuilder = ImmutableList.builder();

    Set<Publisher> usedPublishers = new HashSet<>();

    // TODO(pabloem): Improve the commit logic.
    for (SourceRecord r : records) {

      // Debezium publishes updates for each table in a separate Kafka topic, which is the fully
      // qualified name of the MySQL table (e.g. dbInstanceName.databaseName.table_name).
      String tableName = r.topic();

      if (whitelistedTables.contains(tableName)) {
        Row updateRecord =
            translator.translate(r);
        if (updateRecord == null) {
          continue;
        }

        if (!observedTables.contains(tableName)) {
          String topicName = getPubsubTopicName(pubsubTopicPrefix, tableName);

          LOG.info("Publishing schema for table {} corresponding to topic {}", tableName, topicName);
          Entry result = schemaUpdater.setSchemaForPubSubTopic(
              topicName, project, updateRecord.getSchema());
          if (result == null) {
            throw new InterruptedException(
                "A problem occurred when communicating with Cloud Data Catalog");
          }
          observedTables.add(tableName);
        }

        Publisher pubSubPublisher = this.getPubSubPublisher(tableName);
        if (pubSubPublisher == null) {
          // We were unable to create a pubSubPublisher for this topic. This is bad, and we should
          // stop execution without committing any more messages.
          throw new InterruptedException("Unable to create a PubSub topic for table " + tableName);
        }
        usedPublishers.add(pubSubPublisher);

        PubsubMessage.Builder messageBuilder = PubsubMessage.newBuilder();
        LOG.debug("Update Record is: {}", updateRecord);

        try {
          RowCoder recordCoder = getCoderForRow(tableName, updateRecord);
          ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
          recordCoder.encode(updateRecord, outputStream);

          ByteString encodedUpdate = ByteString.copyFrom(outputStream.toByteArray());
          futureListBuilder.add(
              pubSubPublisher.publish(messageBuilder.setData(encodedUpdate).build()));
        } catch (IOException e) {
          LOG.error("Caught exception {} when trying to encode record {}. Stopping processing.",
              e, updateRecord);
          return ;
        }
      } else {
        LOG.debug("Discarding record: {}", r);
      }
      committer.markProcessed(r);
    }

    usedPublishers.forEach(p -> p.publishAllOutstanding());

    for(ApiFuture<String> f : futureListBuilder.build()) {
      try {
        String result = f.get();
        LOG.debug("Result from PubSub Publish Future: {}", result);
      } catch (ExecutionException e) {
        LOG.error("Exception when executing future {}: {}. Stopping execution.", f, e);
        return;
      }
    }

    committer.markBatchFinished();
  }
}
