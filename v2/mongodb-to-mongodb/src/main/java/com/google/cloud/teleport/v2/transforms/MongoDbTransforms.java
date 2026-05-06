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
package com.google.cloud.teleport.v2.transforms;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/** Transforms for the MongoDB to MongoDB template. */
public class MongoDbTransforms {

  public static WriteWithDlq writeWithDlq() {
    return new WriteWithDlq();
  }

  public static class WriteWithDlq extends PTransform<PCollection<DocumentWithMetadata>, PDone> {
    private String uri;
    private String database;
    private String collection;
    private Integer batchSize = 5000;

    private String dlqPath;
    private String tmpDirectory;
    private Integer maxConcurrentAsyncWrites = 10;
    private Integer maxWriteRetries = 3;
    private Integer dlqMaxRetries = 3;
    private SerializableFunction<String, MongoClient> clientFactory = MongoClients::create;

    public WriteWithDlq withUri(String uri) {
      this.uri = uri;
      return this;
    }

    public WriteWithDlq withDatabase(String database) {
      this.database = database;
      return this;
    }

    public WriteWithDlq withCollection(String collection) {
      this.collection = collection;
      return this;
    }

    public WriteWithDlq withBatchSize(Integer batchSize) {
      if (batchSize != null) {
        this.batchSize = batchSize;
      }
      return this;
    }

    public WriteWithDlq withDlqPath(String dlqPath) {
      this.dlqPath = dlqPath;
      return this;
    }

    public WriteWithDlq withTmpDirectory(String tmpDirectory) {
      this.tmpDirectory = tmpDirectory;
      return this;
    }

    public WriteWithDlq withMaxConcurrentAsyncWrites(Integer maxConcurrentAsyncWrites) {
      if (maxConcurrentAsyncWrites != null) {
        this.maxConcurrentAsyncWrites = maxConcurrentAsyncWrites;
      }
      return this;
    }

    public WriteWithDlq withMaxWriteRetries(Integer maxWriteRetries) {
      if (maxWriteRetries != null) {
        this.maxWriteRetries = maxWriteRetries;
      }
      return this;
    }

    public WriteWithDlq withDlqMaxRetries(Integer dlqMaxRetries) {
      if (dlqMaxRetries != null) {
        this.dlqMaxRetries = dlqMaxRetries;
      }
      return this;
    }

    public WriteWithDlq withClientFactory(SerializableFunction<String, MongoClient> clientFactory) {
      if (clientFactory != null) {
        this.clientFactory = clientFactory;
      }
      return this;
    }

    @Override
    public PDone expand(PCollection<DocumentWithMetadata> input) {
      TupleTag<DocumentWithMetadata> successTag = new TupleTag<DocumentWithMetadata>() {};
      TupleTag<String> failureTag = new TupleTag<String>() {};

      PCollectionTuple writeResults =
          input
              .apply(
                  "AddRandomKey",
                  WithKeys.of(
                      doc ->
                          String.valueOf(
                              java.util.concurrent.ThreadLocalRandom.current().nextInt(1000))))
              .setCoder(
                  KvCoder.of(
                      StringUtf8Coder.of(), SerializableCoder.of(DocumentWithMetadata.class)))
              .apply("GroupIntoBatches", GroupIntoBatches.ofSize(batchSize))
              .apply(
                  "WriteBatches",
                  ParDo.of(
                          MongoDbWriteFn.builder()
                              .withUri(uri)
                              .withDatabase(database)
                              .withCollection(collection)
                              .withMaxConcurrentAsyncWrites(maxConcurrentAsyncWrites)
                              .withMaxWriteRetries(maxWriteRetries)
                              .withDlqMaxRetries(dlqMaxRetries)
                              .withClientFactory(clientFactory)
                              .withFailureTag(failureTag)
                              .build())
                      .withOutputTags(successTag, TupleTagList.of(failureTag)));

      if (dlqPath != null && !dlqPath.isEmpty()) {
        writeResults
            .get(failureTag)
            .apply(
                "WriteDlq",
                DLQWriteTransform.WriteDLQ.newBuilder()
                    .withDlqDirectory(dlqPath + "/" + collection)
                    .withTmpDirectory(tmpDirectory)
                    .build());
      }

      return PDone.in(input.getPipeline());
    }
  }
}
