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

import static com.google.cloud.teleport.v2.transforms.DocumentWithMetadata.ErrorType.RETRYABLE;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFn;
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
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  /** A {@link DoFn} that applies a JavaScript UDF to the document. */
  public static class ApplyUdfFn extends DoFn<DocumentWithMetadata, DocumentWithMetadata> {
    private static final Logger LOG = LoggerFactory.getLogger(ApplyUdfFn.class);

    private final String fileSystemPath;
    private final String functionName;
    private final Integer reloadIntervalMinutes;
    private final TupleTag<String> failureTag;
    private transient JavascriptTextTransformer.JavascriptRuntime javascriptRuntime;

    public ApplyUdfFn(
        String fileSystemPath,
        String functionName,
        Integer reloadIntervalMinutes,
        TupleTag<String> failureTag) {
      this.fileSystemPath = fileSystemPath;
      this.functionName = functionName;
      this.reloadIntervalMinutes = reloadIntervalMinutes;
      this.failureTag = failureTag;
    }

    @Setup
    public void setup() {
      if (fileSystemPath != null && functionName != null) {
        javascriptRuntime =
            JavascriptTextTransformer.JavascriptRuntime.newBuilder()
                .setFileSystemPath(fileSystemPath)
                .setFunctionName(functionName)
                .setReloadIntervalMinutes(reloadIntervalMinutes)
                .build();
      }
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      DocumentWithMetadata item = c.element();
      if (javascriptRuntime != null) {
        try {
          String transformed = javascriptRuntime.invoke(item.getOriginalDocument());
          if (transformed != null) {
            Document doc = Document.parse(transformed);
            c.output(
                DocumentWithMetadata.of(
                    doc,
                    item.getOriginalDocument(),
                    item.getRetryCount(),
                    item.getErrorMessage(),
                    item.getErrorType()));
          } else {
            LOG.warn("UDF returned null for document: {}", item.getOriginalDocument());
            c.output(
                failureTag, item.toDlqJson("UDF returned null", RETRYABLE, item.getRetryCount()));
          }
        } catch (Throwable e) {
          LOG.error("Failed to apply UDF: {}", e.getMessage());
          c.output(
              failureTag,
              item.toDlqJson("UDF failed: " + e.getMessage(), RETRYABLE, item.getRetryCount()));
        }
      } else {
        c.output(item);
      }
    }
  }
}
