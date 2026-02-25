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
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.cdc.dlq.StringDeadLetterQueueSanitizer;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.constants.MetricCounters;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.avro.GenericRecordTypeConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.constants.Constants;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.templates.RowContext;
import com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants;
import com.google.cloud.teleport.v2.transforms.DLQWriteTransform;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
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
import org.apache.commons.codec.binary.Hex;
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

  private Map<String, String> srcTableToShardIdColumnMap;

  private final SQLDialect sqlDialect;

  private final ISchemaMapper schemaMapper;

  private final String shardId;

  public static final Counter FAILED_MUTATION_COUNTER =
      Metrics.counter(SpannerWriter.class, MetricCounters.FAILED_MUTATION_ERRORS);

  public static DeadLetterQueue create(
      String dlqDirectory,
      Ddl ddl,
      Map<String, String> srcTableToShardIdColumnMap,
      SQLDialect sqlDialect,
      ISchemaMapper iSchemaMapper,
      String shardId) {
    return new DeadLetterQueue(
        dlqDirectory, ddl, srcTableToShardIdColumnMap, sqlDialect, iSchemaMapper, shardId);
  }

  public static DeadLetterQueue create(
      String dlqDirectory,
      Ddl ddl,
      Map<String, String> srcTableToShardIdColumnMap,
      SQLDialect sqlDialect,
      ISchemaMapper iSchemaMapper) {
    return new DeadLetterQueue(
        dlqDirectory, ddl, srcTableToShardIdColumnMap, sqlDialect, iSchemaMapper, null);
  }

  public String getDlqDirectory() {
    return dlqDirectory;
  }

  private DeadLetterQueue(
      String dlqDirectory,
      Ddl ddl,
      Map<String, String> srcTableToShardIdColumnMap,
      SQLDialect sqlDialect,
      ISchemaMapper iSchemaMapper,
      String shardId) {
    this.dlqDirectory = dlqDirectory;
    this.ddl = ddl;
    this.srcTableToShardIdColumnMap = srcTableToShardIdColumnMap;
    this.sqlDialect = sqlDialect;
    this.schemaMapper = iSchemaMapper;
    this.shardId = shardId;
  }

  @VisibleForTesting
  PTransform<PCollection<String>, PDone> createDLQTransform(String dlqDirectory) {
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
          .setFileNamePrefix(UUID.randomUUID().toString())
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
        .apply("FilteredRowsDLQ", createDLQTransform(dlqDirectory));
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
        .apply("TransformerDLQ", createDLQTransform(dlqDirectory));
    LOG.info("added dlq stage after transformer");
  }

  @VisibleForTesting
  protected FailsafeElement<String, String> rowContextToDlqElement(RowContext r) {
    GenericRecord record = r.row().getPayload();
    JSONObject json = new JSONObject();
    initializeJsonNode(json, r.row().tableName(), r.row().getReadTimeMicros());

    for (Field f : record.getSchema().getFields()) {
      Object value = record.get(f.name());
      /*
       * We take special care that if GenericRecordTypeConvertor throws an exception,
       * we would still preserve the original record in DLQ.
       * Also note that here we are calling a static utility from GenericRecordTypeConvertor
       * Which just marshals types like logical, record etc. It does not pass the data via custom transform.
       */
      try {
        value =
            GenericRecordTypeConvertor.getJsonNodeObjectFromGenericRecord(
                record, f, r.row().tableName(), schemaMapper);
      } catch (Exception e) {
        LOG.error(
            "Error in mapping DLQ record field to Json Node, record, record = {}, field = {}. Unmapped record would be emitted to DLQ.",
            record,
            f,
            e);
      }
      putValueToJson(json, f.name(), value);
    }

    String spannerTable = r.row().tableName();
    try {
      if (schemaMapper != null) {
        spannerTable = schemaMapper.getSpannerTableName("", r.row().tableName());
      }
    } catch (Exception e) {
      LOG.debug("Could not map source table {} to Spanner table", r.row().tableName(), e);
    }
    if (r.row().shardId() != null) {
      json.put(Constants.EVENT_SHARD_ID, r.row().shardId());
      populateShardIdColumnAndValue(json, spannerTable, r.row().shardId());
    } else if (this.shardId != null && !this.shardId.isEmpty()) {
      json.put(Constants.EVENT_SHARD_ID, this.shardId);
      populateShardIdColumnAndValue(json, spannerTable, this.shardId);
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
        .apply("WriterDLQ", createDLQTransform(dlqDirectory));
    LOG.info("added dlq stage after writer");
  }

  @VisibleForTesting
  protected FailsafeElement<String, String> mutationToDlqElement(Mutation m) {
    JSONObject json = new JSONObject();
    json.put("_metadata_spanner_mutation", true);

    Instant instant = Instant.now();
    initializeJsonNode(
        json, m.getTable(), (instant.getEpochSecond() * 1000_000 + instant.getNano() / 1000));
    Map<String, Value> mutationMap = m.asMap();
    for (Map.Entry<String, Value> entry : mutationMap.entrySet()) {
      Value value = entry.getValue();
      Object val = null;
      if (value != null && !value.isNull()) {
        switch (value.getType().getCode()) {
          case BYTES:
            val = Hex.encodeHexString(value.getBytes().toByteArray());
            break;
          case INT64:
            val = value.getInt64();
            break;
          case FLOAT64:
            val = value.getFloat64();
            break;
          case NUMERIC:
            val = value.getNumeric();
            break;
          case BOOL:
            val = value.getBool();
            break;
          case ARRAY:
            if (value.getType().getArrayElementType().getCode() == Type.Code.BYTES) {
              val =
                  value.getBytesArray().stream()
                      .map(v -> v == null ? null : Hex.encodeHexString(v.toByteArray()))
                      .collect(Collectors.toList());
            } else {
              val = value.toString();
            }
            break;
          default:
            val = value.toString();
        }
      }
      putValueToJson(json, entry.getKey(), val);
    }

    // Attempt to extract and populate shard ID metadata
    try {
      if (this.shardId != null && !this.shardId.isEmpty()) {
        json.put(Constants.EVENT_SHARD_ID, this.shardId);
        populateShardIdColumnAndValue(json, m.getTable(), this.shardId);
      } else {
        // Just try to find if the mutation has the shard id column and populate it in metadata
        // We know that if the mutation has the shard id column, it must have the shard id value
        String shardIdColName = getShardIdColumnName(m.getTable());
        if (mutationMap.containsKey(shardIdColName)) {
          Value shardIdValue = mutationMap.get(shardIdColName);
          if (shardIdValue != null && !shardIdValue.isNull()) {
            String shardIdStr = shardIdValue.toString();
            if (shardIdValue.getType().getCode() == Type.Code.STRING) {
              shardIdStr = shardIdValue.getString();
            } else if (shardIdValue.getType().getCode() == Type.Code.INT64) {
              shardIdStr = Long.toString(shardIdValue.getInt64());
            }
            json.put(Constants.EVENT_SHARD_ID, shardIdStr);
            json.put(Constants.SHARD_ID_COLUMN_NAME, shardIdColName);
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("Failed to populate shard ID metadata for DLQ mutation", e);
    }

    return FailsafeElement.of(json.toString(), json.toString())
        .setErrorMessage("SpannerWriteFailed");
  }

  private void initializeJsonNode(JSONObject json, String tableName, long timeStamp) {
    json.put(DatastreamConstants.EVENT_CHANGE_TYPE_KEY, DatastreamConstants.UPDATE_INSERT_EVENT);
    json.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, tableName);
    json.put(DatastreamConstants.MYSQL_TIMESTAMP_KEY, timeStamp);
    json.put("_metadata_read_timestamp", timeStamp);
    json.put("_metadata_dataflow_timestamp", timeStamp);
    switch (this.sqlDialect) {
      case POSTGRESQL:
        json.put(
            DatastreamConstants.EVENT_SOURCE_TYPE_KEY, DatastreamConstants.POSTGRES_SOURCE_TYPE);
        break;
      default:
        json.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, DatastreamConstants.MYSQL_SOURCE_TYPE);
    }
  }

  /*
   * Puts the value into the JSON object.
   * If the value is an integral type (Long, Integer, Short, Byte, BigInteger), it is put as is.
   * Otherwise, it is converted to string.
   * This is to ensure that BIT datatypes (which are mapped to Long/BigInteger) are preserved as numbers in the JSON output.
   */
  private void putValueToJson(JSONObject json, String key, Object value) {
    if (value == null) {
      json.put(key, (Object) null);
    } else if (value instanceof Number || value instanceof java.util.Collection) {
      json.put(key, value);
    } else {
      json.put(key, value.toString());
    }
  }

  private void populateShardIdColumnAndValue(
      JSONObject json, String spannerTableName, String shardIdValue) {
    String shardIdColName = getShardIdColumnName(spannerTableName);

    // We want to populate these in the change event only if Spanner has a dedicated shard id
    // column.
    if (spannerTableName != null
        && ddl.table(spannerTableName) != null
        && ddl.table(spannerTableName).column(shardIdColName) != null) {
      json.put(shardIdColName, shardIdValue);
      json.put(Constants.SHARD_ID_COLUMN_NAME, shardIdColName);
    }
  }

  private String getShardIdColumnName(String spannerTableName) {
    String shardIdColName = null;
    if (schemaMapper != null) {
      try {
        if (spannerTableName != null) {
          shardIdColName = schemaMapper.getShardIdColumnName("", spannerTableName);
          if (shardIdColName != null && !shardIdColName.isEmpty()) {
            return shardIdColName;
          }
          shardIdColName =
              "migration_shard_id"; // we can fallback to this as we explicitly check the Spanner
          // DDL
        }
      } catch (Exception e) {
        LOG.debug("Could not get shard ID column for Spanner table {}", spannerTableName);
      }
    }
    return shardIdColName;
  }
}
