/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.writer;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.cdc.dlq.StringDeadLetterQueueSanitizer;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.constants.MetricCounters;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.templates.RowContext;
import com.google.cloud.teleport.v2.transforms.DLQWriteTransform;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.util.Map;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages the dead letter queue in the pipeline. */
public class DeadLetterQueue implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(DeadLetterQueue.class);

  private final String dlqDirectory;

  private final Ddl ddl;

  private final PTransform<PCollection<String>, PDone> dlqTransform;

  public static final Counter FAILED_MUTATION_COUNTER =
      Metrics.counter(SpannerWriter.class, MetricCounters.FAILED_MUTATION_ERRORS);

  public static DeadLetterQueue create(String dlqDirectory, Ddl ddl) {
    return new DeadLetterQueue(dlqDirectory, ddl);
  }

  public String getDlqDirectory() {
    return dlqDirectory;
  }

  public PTransform<PCollection<String>, PDone> getDlqTransform() {
    return dlqTransform;
  }

  private DeadLetterQueue(String dlqDirectory, Ddl ddl) {
    this.dlqDirectory = dlqDirectory;
    this.dlqTransform = createDLQTransform(dlqDirectory);
    this.ddl = ddl;
  }

  @VisibleForTesting
  private PTransform<PCollection<String>, PDone> createDLQTransform(String dlqDirectory) {
    if (dlqDirectory == null) {
      throw new RuntimeException("Unable to start pipeline as DLQ is not configured");
    }
    if (dlqDirectory == "LOG") {
      LOG.warn("writing errors to log as no DLQ directory configured");
      return new WriteToLog();
    } else if (dlqDirectory == "IGNORE") {
      LOG.warn("the pipeline will ignore all errors");
      return null;
    } else {
      String dlqUri = FileSystems.matchNewResource(dlqDirectory, true).toString();
      LOG.info("setting up dead letter queue directory: {}", dlqDirectory);
      return DLQWriteTransform.WriteDLQ.newBuilder()
          .withDlqDirectory(dlqUri)
          .withTmpDirectory(dlqUri + "/tmp")
          .setIncludePaneInfo(true)
          .build();
    }
  }

  public static class WriteToLog extends PTransform<PCollection<String>, PDone> {

    @Override
    public PDone expand(PCollection<String> input) {
      input.apply(
          ParDo.of(
              new DoFn<String, String>() {
                @ProcessElement
                public void process(@Element String s) {
                  LOG.info("logging failed row: {}", s);
                  FAILED_MUTATION_COUNTER.inc();
                }
              }));
      return PDone.in(input.getPipeline());
    }
  }

  public void filteredEventsToDLQ(
      PCollection<@UnknownKeyFor @NonNull @Initialized RowContext> filteredRows) {
    LOG.warn("added filtered transformation output to pipeline");
    DoFn<RowContext, FailsafeElement<String, String>> rowContextToString =
        new DoFn<RowContext, FailsafeElement<String, String>>() {
          @ProcessElement
          public void processElement(
              @Element RowContext rowContext,
              OutputReceiver<FailsafeElement<String, String>> out,
              ProcessContext c) {
            c.output(rowContextToDlqElement(rowContext));
          }
        };
    filteredRows
        .apply("filteredRowTransformString", ParDo.of(rowContextToString))
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        .apply("SanitizeTransformWriteDLQ", MapElements.via(new StringDeadLetterQueueSanitizer()))
        .setCoder(StringUtf8Coder.of())
        .apply("FilteredRowsDLQ", dlqTransform);
    LOG.info("added filtering dlq stage after transformer");
  }

  public void failedTransformsToDLQ(
      PCollection<@UnknownKeyFor @NonNull @Initialized RowContext> failedRows) {
    // TODO - add the exception message
    LOG.warn("added failed transformation output to pipeline");
    DoFn<RowContext, FailsafeElement<String, String>> rowContextToString =
        new DoFn<RowContext, FailsafeElement<String, String>>() {
          @ProcessElement
          public void processElement(
              @Element RowContext rowContext,
              OutputReceiver<FailsafeElement<String, String>> out,
              ProcessContext c) {
            c.output(rowContextToDlqElement(rowContext));
          }
        };
    failedRows
        .apply("failedRowTransformString", ParDo.of(rowContextToString))
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        .apply("SanitizeTransformWriteDLQ", MapElements.via(new StringDeadLetterQueueSanitizer()))
        .setCoder(StringUtf8Coder.of())
        .apply("TransformerDLQ", dlqTransform);
    LOG.info("added dlq stage after transformer");
  }

  @VisibleForTesting
  protected FailsafeElement<String, String> rowContextToDlqElement(RowContext r) {
    GenericRecord record = r.row().getPayload();
    JSONObject json = new JSONObject();

    String sourceTableName = r.row().tableName();
    json.put("_metadata_table", sourceTableName);
    for (Field f : record.getSchema().getFields()) {
      Object value = record.get(f.name());
      json.put(f.name(), value == null ? null : value.toString());
    }
    FailsafeElement<String, String> dlqElement =
        FailsafeElement.of(json.toString(), json.toString());
    if (r.err() != null) {
      dlqElement =
          dlqElement.setErrorMessage(
              "TransformationFailed: " + r.err() + "\n" + r.getStackTraceString());
    }
    return dlqElement;
  }

  public void failedMutationsToDLQ(
      PCollection<@UnknownKeyFor @NonNull @Initialized MutationGroup> failedMutations) {
    // TODO - add the exception message
    // TODO - Explore windowing with CoGroupByKey to extract source row based on mutation
    LOG.warn("added mutation output to pipeline");
    failedMutations
        .apply(
            "failedMutationToString",
            ParDo.of(
                new DoFn<MutationGroup, FailsafeElement<String, String>>() {
                  @ProcessElement
                  public void processElement(
                      @Element MutationGroup mg,
                      OutputReceiver<FailsafeElement<String, String>> out,
                      ProcessContext c) {
                    for (Mutation m : mg) {
                      LOG.debug("saving failed mutation to DLQ Table: {}", m);
                      out.output(mutationToDlqElement(m));
                    }
                    FAILED_MUTATION_COUNTER.inc(mg.size());
                    LOG.info("completed stringifying of failed mutations: {}", mg.size());
                  }
                }))
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        .apply("SanitizeSpannerWriteDLQ", MapElements.via(new StringDeadLetterQueueSanitizer()))
        .setCoder(StringUtf8Coder.of())
        .apply("WriterDLQ", dlqTransform);
    LOG.info("added dlq stage after writer");
  }

  @VisibleForTesting
  protected FailsafeElement<String, String> mutationToDlqElement(Mutation m) {
    JSONObject json = new JSONObject();
    json.put("_metadata_table", m.getTable());

    Map<String, Value> mutationMap = m.asMap();
    for (Map.Entry<String, Value> entry : mutationMap.entrySet()) {
      Value value = entry.getValue();
      json.put(entry.getKey(), value == null ? null : String.valueOf(value));
    }

    return FailsafeElement.of(json.toString(), json.toString())
        .setErrorMessage("SpannerWriteFailed");
  }
}
