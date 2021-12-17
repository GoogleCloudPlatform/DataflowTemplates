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
package com.google.cloud.dataflow.cdc.connector;

import com.google.api.core.ApiFuture;
import com.google.cloud.datacatalog.v1beta1.Entry;
import com.google.cloud.dataflow.cdc.common.DataCatalogSchemaUtils.DataCatalogSchemaManager;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.pubsub.v1.PubsubMessage;
import io.debezium.embedded.EmbeddedEngine.RecordCommitter;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Tests for PubSunChangeConsummer. */
public class PubSubChangeconsumerTest {

  @Test
  public void testBasicRecordAndFilteredRecordInput() throws InterruptedException {
    DataCatalogSchemaManager dataCatalogMock = Mockito.mock(DataCatalogSchemaManager.class);
    Publisher pubsubMock = Mockito.mock(Publisher.class);
    Mockito.when(
            dataCatalogMock.updateSchemaForTable(
                Mockito.anyString(), Mockito.any(org.apache.beam.sdk.schemas.Schema.class)))
        .thenReturn(Entry.newBuilder().build());
    Mockito.when(pubsubMock.publish(Mockito.any())).thenReturn(Mockito.mock(ApiFuture.class));

    PubSubChangeConsumer changeConsumer =
        new PubSubChangeConsumer(
            Sets.newHashSet("mainstance.cdcForDataflow.team_metadata", "table2"),
            dataCatalogMock,
            (input1, input2) -> pubsubMock);

    Schema keySchema = SchemaBuilder.struct().field("team", Schema.STRING_SCHEMA).build();
    Struct key = new Struct(keySchema).put("team", "team_PXHU");

    Schema valueAfterSchema =
        SchemaBuilder.struct()
            .field("team", Schema.STRING_SCHEMA)
            .field("city", Schema.STRING_SCHEMA)
            .field("country", Schema.STRING_SCHEMA)
            .field("year_founded", Schema.INT32_SCHEMA)
            .field("some_timestamp", Schema.INT64_SCHEMA)
            .build();

    Schema valueSchema =
        SchemaBuilder.struct()
            .field("after", valueAfterSchema)
            .field("op", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.INT64_SCHEMA)
            .build();

    Struct value =
        new Struct(valueSchema)
            .put("op", "c")
            .put("ts_ms", 1569287580660L)
            .put(
                "after",
                new Struct(valueAfterSchema)
                    .put("team", "team_PXHU")
                    .put("city", "Mexico")
                    .put("country", "Mexico as well")
                    .put("year_founded", 1916)
                    .put("some_timestamp", 123456579L));

    String topicName = "mainstance.cdcForDataflow.team_metadata";

    // We are going to pass two records to be checked, but only one of them belongs to a table that
    // is whitelisted, therefore, only one will be published to pubsub.
    List<SourceRecord> recordBatch =
        ImmutableList.of(
            new SourceRecord(
                ImmutableMap.of("server", "mainstance"),
                ImmutableMap.of(
                    "file",
                    "mysql-bin.000023",
                    "pos",
                    110489,
                    "gtids",
                    "36797132-a366-11e9-ac33-42010a800456:1-6407169",
                    "row",
                    1,
                    "snapshot",
                    true),
                topicName,
                keySchema,
                key,
                valueSchema,
                value),
            new SourceRecord(
                ImmutableMap.of("server", "mainstance"),
                ImmutableMap.of(
                    "file",
                    "mysql-bin.000023",
                    "pos",
                    110490,
                    "gtids",
                    "36797132-a366-11e9-ac33-42010a800456:1-6407169",
                    "row",
                    1,
                    "snapshot",
                    true),
                "NOTWHITELISTEDTOPIC!", // A topic that was NOT whitelisted
                keySchema,
                key,
                valueSchema,
                value));

    RecordCommitter mockCommitter = Mockito.mock(RecordCommitter.class);

    changeConsumer.handleBatch(recordBatch, mockCommitter);

    Mockito.verify(pubsubMock, Mockito.times(1)).publish(Mockito.any(PubsubMessage.class));
    Mockito.verify(mockCommitter).markProcessed(recordBatch.get(0));
    Mockito.verify(mockCommitter).markBatchFinished();
  }
}
