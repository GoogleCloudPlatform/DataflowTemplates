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

import com.mongodb.MongoBulkWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.bson.Document;
import org.joda.time.Instant;

/** Transforms for the MongoDB to MongoDB template. */
public class MongoDbTransforms {

  public static WriteWithDlq writeWithDlq() {
    return new WriteWithDlq();
  }

  public static class WriteWithDlq extends PTransform<PCollection<Document>, PDone> {
    private String uri;
    private String database;
    private String collection;
    private Integer batchSize = 5000;
    private Boolean ordered = false;
    private String writeConcern;
    private Boolean journal;
    private String dlqPath;
    private Integer maxConcurrentAsyncWrites = 10;

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

    public WriteWithDlq withOrdered(Boolean ordered) {
      if (ordered != null) {
        this.ordered = ordered;
      }
      return this;
    }

    public WriteWithDlq withWriteConcern(String writeConcern) {
      this.writeConcern = writeConcern;
      return this;
    }

    public WriteWithDlq withJournal(Boolean journal) {
      this.journal = journal;
      return this;
    }

    public WriteWithDlq withDlqPath(String dlqPath) {
      this.dlqPath = dlqPath;
      return this;
    }

    public WriteWithDlq withMaxConcurrentAsyncWrites(Integer maxConcurrentAsyncWrites) {
      if (maxConcurrentAsyncWrites != null) {
        this.maxConcurrentAsyncWrites = maxConcurrentAsyncWrites;
      }
      return this;
    }

    @Override
    public PDone expand(PCollection<Document> input) {
      TupleTag<Document> successTag = new TupleTag<Document>() {};
      TupleTag<String> failureTag = new TupleTag<String>() {};

      PCollectionTuple writeResults =
          input
              .apply(
                  "AddRandomKey",
                  WithKeys.of(
                      doc ->
                          String.valueOf(
                              java.util.concurrent.ThreadLocalRandom.current().nextInt(1000))))
              .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(Document.class)))
              .apply("GroupIntoBatches", GroupIntoBatches.ofSize(batchSize))
              .apply(
                  "WriteBatches",
                  ParDo.of(
                          new DoFn<KV<String, Iterable<Document>>, Document>() {
                            private transient MongoClient mongoClient;
                            private transient MongoCollection<Document> mongoCollection;
                            private transient ExecutorService executor;
                            private transient Semaphore semaphore;
                            private transient ConcurrentLinkedQueue<CompletableFuture<Void>>
                                futures;
                            private transient ConcurrentLinkedQueue<String> failures;
                            private final Counter successfulDocs =
                                Metrics.counter(WriteWithDlq.class, "successful-documents-written");
                            private transient AtomicLong successfulCount;

                            @Setup
                            public void setup() {
                              executor = Executors.newFixedThreadPool(maxConcurrentAsyncWrites);
                              semaphore = new Semaphore(maxConcurrentAsyncWrites);
                            }

                            @Teardown
                            public void teardown() {
                              if (executor != null) {
                                executor.shutdown();
                              }
                            }

                            @StartBundle
                            public void startBundle() {
                              mongoClient = MongoClients.create(uri);
                              MongoDatabase db = mongoClient.getDatabase(database);

                              WriteConcern wc = WriteConcern.ACKNOWLEDGED;
                              if (writeConcern != null) {
                                if (writeConcern.equalsIgnoreCase("majority")) {
                                  wc = WriteConcern.MAJORITY;
                                } else {
                                  try {
                                    int w = Integer.parseInt(writeConcern);
                                    wc = new WriteConcern(w);
                                  } catch (NumberFormatException e) {
                                    // Fallback to default
                                  }
                                }
                              }
                              if (journal != null) {
                                wc = wc.withJournal(journal);
                              }

                              mongoCollection = db.getCollection(collection).withWriteConcern(wc);

                              futures = new ConcurrentLinkedQueue<>();
                              failures = new ConcurrentLinkedQueue<>();
                              successfulCount = new AtomicLong(0);
                            }

                            @ProcessElement
                            public void processElement(ProcessContext c)
                                throws InterruptedException {
                              Iterable<Document> docs = c.element().getValue();
                              List<WriteModel<Document>> updates = new ArrayList<>();
                              List<Document> docList = new ArrayList<>();

                              for (Document doc : docs) {
                                docList.add(doc);
                                Object id = doc.get("_id");
                                if (id != null) {
                                  updates.add(
                                      new ReplaceOneModel<>(
                                          new Document("_id", id),
                                          doc,
                                          new ReplaceOptions().upsert(true)));
                                }
                              }

                              if (!updates.isEmpty()) {
                                semaphore.acquire();
                                CompletableFuture<Void> future =
                                    CompletableFuture.runAsync(
                                        () -> {
                                          try {
                                            mongoCollection.bulkWrite(
                                                updates, new BulkWriteOptions().ordered(ordered));
                                            successfulCount.addAndGet(docList.size());
                                          } catch (MongoBulkWriteException e) {
                                            List<BulkWriteError> writeErrors = e.getWriteErrors();
                                            if (ordered) {
                                              // Ordered execution stops at first error.
                                              // Find the minimum index among errors.
                                              int firstErrorIndex = docList.size();
                                              for (BulkWriteError error : writeErrors) {
                                                firstErrorIndex =
                                                    Math.min(firstErrorIndex, error.getIndex());
                                              }
                                              successfulCount.addAndGet(firstErrorIndex);
                                              // Add all documents from firstErrorIndex to the end
                                              // to failures.
                                              for (int i = firstErrorIndex;
                                                  i < docList.size();
                                                  i++) {
                                                Document doc = docList.get(i);
                                                String msg =
                                                    "Ordered write failed or was not attempted.";
                                                if (i == firstErrorIndex) {
                                                  for (BulkWriteError error : writeErrors) {
                                                    if (error.getIndex() == i) {
                                                      msg = error.getMessage();
                                                      break;
                                                    }
                                                  }
                                                }
                                                failures.add(doc.toJson() + " - Error: " + msg);
                                              }
                                            } else {
                                              successfulCount.addAndGet(
                                                  docList.size() - writeErrors.size());
                                              // Unordered execution attempts all writes.
                                              // Add only specific failed documents to failures.
                                              for (BulkWriteError error : writeErrors) {
                                                int index = error.getIndex();
                                                if (index >= 0 && index < docList.size()) {
                                                  Document failedDoc = docList.get(index);
                                                  failures.add(
                                                      failedDoc.toJson()
                                                          + " - Error: "
                                                          + error.getMessage());
                                                }
                                              }
                                            }
                                          } catch (Exception e) {
                                            for (Document doc : docList) {
                                              failures.add(
                                                  doc.toJson() + " - Error: " + e.getMessage());
                                            }
                                          } finally {
                                            semaphore.release();
                                          }
                                        },
                                        executor);
                                futures.add(future);
                              }
                            }

                            @FinishBundle
                            public void finishBundle(FinishBundleContext c) {
                              CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                                  .join();

                              if (mongoClient != null) {
                                mongoClient.close();
                              }

                              successfulDocs.inc(successfulCount.get());

                              String failure;
                              while ((failure = failures.poll()) != null) {
                                c.output(failureTag, failure, Instant.now(), GlobalWindow.INSTANCE);
                              }
                            }
                          })
                      .withOutputTags(successTag, TupleTagList.of(failureTag)));

      if (dlqPath != null && !dlqPath.isEmpty()) {
        writeResults
            .get(failureTag)
            .apply("WriteDlq", TextIO.write().to(dlqPath + "/" + collection));
      }

      return PDone.in(input.getPipeline());
    }
  }
}
