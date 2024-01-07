/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.transforms;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.spanner.utils.IShardIdFetcher;
import com.google.cloud.teleport.v2.spanner.utils.ShardIdRequest;
import com.google.cloud.teleport.v2.spanner.utils.ShardIdResponse;
import com.google.cloud.teleport.v2.templates.changestream.DataChangeRecordTypeConvertor;
import com.google.cloud.teleport.v2.templates.common.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.utils.ShardIdFetcherImpl;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Files;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This DoFn assigns the shardId as key to the record. */
public class AssignShardIdFn
    extends DoFn<TrimmedShardedDataChangeRecord, TrimmedShardedDataChangeRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(AssignShardIdFn.class);

  private final SpannerConfig spannerConfig;

  /* SpannerAccessor must be transient so that its value is not serialized at runtime. */
  private transient SpannerAccessor spannerAccessor;

  /* The information schema of the Cloud Spanner database */
  private final Ddl ddl;

  private final Schema schema;

  // Jackson Object mapper.
  private transient ObjectMapper mapper;

  private final String shardingMode;

  private final String shardName;

  private final String skipDirName;

  private final String customJarPath;

  private final String shardingCustomClassName;

  private IShardIdFetcher shardIdFetcher;

  public AssignShardIdFn(
      SpannerConfig spannerConfig,
      Schema schema,
      Ddl ddl,
      String shardingMode,
      String shardName,
      String skipDirName,
      String customJarPath,
      String shardingCustomClassName) {
    this.spannerConfig = spannerConfig;
    this.schema = schema;
    this.ddl = ddl;
    this.shardingMode = shardingMode;
    this.shardName = shardName;
    this.skipDirName = skipDirName;
    this.customJarPath = customJarPath;
    this.shardingCustomClassName = shardingCustomClassName;
  }

  // setSpannerAccessor is added to be used by unit tests
  public void setSpannerAccessor(SpannerAccessor spannerAccessor) {
    this.spannerAccessor = spannerAccessor;
  }

  // setMapper is added to be used by unit tests
  public void setMapper(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  // setShardIdFetcher is added to be used by unit tests
  public void setShardIdFetcher(IShardIdFetcher shardIdFetcher) {
    this.shardIdFetcher = shardIdFetcher;
  }

  /** Setup function connects to Cloud Spanner. */
  @Setup
  public void setup() {
    if (spannerConfig != null) {
      spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    }
    mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    shardIdFetcher = getShardIdFetcherImpl(customJarPath, shardingCustomClassName);
  }

  /** Teardown function disconnects from the Cloud Spanner. */
  @Teardown
  public void teardown() {
    if (spannerConfig != null) {
      spannerAccessor.close();
    }
  }

  /**
   * Assigns shard id to each spanner record. If custom jar path is specified, loads the custom
   * class locally and then fetches the shard id and if not specified relies on default
   * ShardIdFetcherImpl to fetch the shard id.
   */
  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    TrimmedShardedDataChangeRecord record = new TrimmedShardedDataChangeRecord(c.element());

    try {
      if (shardingMode.equals(Constants.SHARDING_MODE_SINGLE_SHARD)) {
        record.setShard(this.shardName);
        c.output(record);
      } else {
        String keysJsonStr = record.getMods().get(0).getKeysJson();
        JsonNode keysJson = mapper.readTree(keysJsonStr);

        String newValueJsonStr = record.getMods().get(0).getNewValuesJson();
        JsonNode newValueJson = mapper.readTree(newValueJsonStr);
        Map<String, Object> spannerRecord = new HashMap<>();
        // Query the spanner database in case of a DELETE event
        if (record.getModType() == ModType.DELETE) {
          spannerRecord =
              fetchSpannerRecord(
                  record.getTableName(),
                  record.getCommitTimestamp(),
                  record.getServerTransactionId(),
                  keysJson);
        } else {
          Table table = ddl.table(record.getTableName());

          // Add all fields from keysJson and valuesJson to spannerRecord
          for (Iterator<String> it = keysJson.fieldNames(); it.hasNext(); ) {
            String key = it.next();
            Column column = table.column(key);
            spannerRecord.put(key, getColumnValueFromJson(column, keysJson));
          }
          for (Iterator<String> it = newValueJson.fieldNames(); it.hasNext(); ) {
            String key = it.next();
            Column column = table.column(key);
            spannerRecord.put(key, getColumnValueFromJson(column, newValueJson));
          }
        }
        ShardIdRequest shardIdRequest = new ShardIdRequest(record.getTableName(), spannerRecord);

        ShardIdResponse shardIdResponse = shardIdFetcher.getShardId(shardIdRequest);
        String logicalShardId = shardIdResponse.getLogicalShardId();
        if (logicalShardId == null || logicalShardId.isEmpty() || logicalShardId.contains("/")) {
          throw new IllegalArgumentException(
              "Invalid logical shard id value: "
                  + logicalShardId
                  + " for spanner table: "
                  + record.getTableName());
        }
        record.setShard(logicalShardId);
        c.output(record);
      }

    } catch (Exception e) {
      StringWriter errors = new StringWriter();
      e.printStackTrace(new PrintWriter(errors));
      LOG.error("Error fetching shard Id column: " + e.getMessage() + ": " + errors.toString());
      throw e;
    }
  }

  public IShardIdFetcher getShardIdFetcherImpl(
      String customJarPath, String shardingCustomClassName) {
    if (!customJarPath.isEmpty() && !shardingCustomClassName.isEmpty()) {
      LOG.info(
          "Getting custom sharding fetcher : "
              + customJarPath
              + " with class: "
              + shardingCustomClassName);
      try {
        // Get the start time of loading the custom class
        Instant startTime = Instant.now();

        // Getting the jar URL which contains target class
        URL[] classLoaderUrls = saveFilesLocally(customJarPath);

        // Create a new URLClassLoader
        URLClassLoader urlClassLoader = new URLClassLoader(classLoaderUrls);

        // Load the target class
        Class<?> shardFetcherClass = urlClassLoader.loadClass(shardingCustomClassName);

        // Create a new instance from the loaded class
        Constructor<?> constructor = shardFetcherClass.getConstructor();
        IShardIdFetcher shardFetcher = (IShardIdFetcher) constructor.newInstance();
        // Get the end time of loading the custom class
        Instant endTime = Instant.now();
        LOG.info(
            "Custom jar "
                + customJarPath
                + ": Took "
                + (new Duration(startTime, endTime)).toString()
                + " to load");
        return shardFetcher;
      } catch (Exception e) {
        throw new RuntimeException("Error loading custom class : " + e.getMessage());
      }
    }
    // else return the core implementation
    ShardIdFetcherImpl shardIdFetcher = new ShardIdFetcherImpl(schema);
    return shardIdFetcher;
  }

  private URL[] saveFilesLocally(String driverJars) {
    List<String> listOfJarPaths = Splitter.on(',').trimResults().splitToList(driverJars);

    final String destRoot = Files.createTempDir().getAbsolutePath();
    List<URL> driverJarUrls = new ArrayList<>();
    listOfJarPaths.stream()
        .forEach(
            jarPath -> {
              try {
                ResourceId sourceResourceId = FileSystems.matchNewResource(jarPath, false);
                @SuppressWarnings("nullness")
                File destFile = Paths.get(destRoot, sourceResourceId.getFilename()).toFile();
                ResourceId destResourceId =
                    FileSystems.matchNewResource(destFile.getAbsolutePath(), false);
                copy(sourceResourceId, destResourceId);
                LOG.info("Localized jar: " + sourceResourceId + " to: " + destResourceId);
                driverJarUrls.add(destFile.toURI().toURL());
              } catch (IOException e) {
                LOG.warn("Unable to copy " + jarPath, e);
              }
            });
    return driverJarUrls.stream().toArray(URL[]::new);
  }

  private void copy(ResourceId source, ResourceId dest) throws IOException {
    try (ReadableByteChannel rbc = FileSystems.open(source)) {
      try (WritableByteChannel wbc = FileSystems.create(dest, MimeTypes.BINARY)) {
        ByteStreams.copy(rbc, wbc);
      }
    }
  }

  private Map<String, Object> fetchSpannerRecord(
      String tableName,
      com.google.cloud.Timestamp commitTimestamp,
      String serverTxnId,
      JsonNode keysJson)
      throws Exception {
    com.google.cloud.Timestamp staleReadTs =
        com.google.cloud.Timestamp.ofTimeSecondsAndNanos(
            commitTimestamp.getSeconds() - 1, commitTimestamp.getNanos());
    List<String> columns =
        ddl.table(tableName).columns().stream().map(Column::name).collect(Collectors.toList());
    // Stale read the spanner row for all the columns for timestamp 1 second less than the DELETE
    // event
    Struct row =
        spannerAccessor
            .getDatabaseClient()
            .singleUse(TimestampBound.ofReadTimestamp(staleReadTs))
            .readRow(tableName, generateKey(tableName, keysJson), columns);
    if (row == null) {
      throw new Exception(
          "stale read on Spanner returned null for table: "
              + tableName
              + ", commitTimestamp: "
              + commitTimestamp
              + " and serverTxnId:"
              + serverTxnId);
    }
    return getRowAsMap(row, columns, tableName);
  }

  public Map<String, Object> getRowAsMap(Struct row, List<String> columns, String tableName)
      throws Exception {
    Map<String, Object> spannerRecord = new HashMap<>();
    Table table = ddl.table(tableName);
    for (String columnName : columns) {
      Column column = table.column(columnName);
      Object columnValue =
          row.isNull(columnName) ? null : getColumnValueFromRow(column, row.getValue(columnName));
      spannerRecord.put(columnName, columnValue);
    }
    return spannerRecord;
  }

  private com.google.cloud.spanner.Key generateKey(String tableName, JsonNode keysJson)
      throws Exception {
    try {
      Table table = ddl.table(tableName);
      ImmutableList<IndexColumn> keyColumns = table.primaryKeys();
      com.google.cloud.spanner.Key.Builder pk = com.google.cloud.spanner.Key.newBuilder();

      for (IndexColumn keyColumn : keyColumns) {
        Column key = table.column(keyColumn.name());
        Type keyColType = key.type();
        String keyColName = key.name();
        switch (keyColType.getCode()) {
          case BOOL:
          case PG_BOOL:
            pk.append(
                DataChangeRecordTypeConvertor.toBoolean(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          case INT64:
          case PG_INT8:
            pk.append(
                DataChangeRecordTypeConvertor.toLong(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          case FLOAT64:
          case PG_FLOAT8:
            pk.append(
                DataChangeRecordTypeConvertor.toDouble(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          case STRING:
          case PG_VARCHAR:
          case PG_TEXT:
            pk.append(
                DataChangeRecordTypeConvertor.toString(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          case NUMERIC:
          case PG_NUMERIC:
            pk.append(
                DataChangeRecordTypeConvertor.toNumericBigDecimal(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          case JSON:
          case PG_JSONB:
            pk.append(
                DataChangeRecordTypeConvertor.toString(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          case BYTES:
          case PG_BYTEA:
            pk.append(
                DataChangeRecordTypeConvertor.toByteArray(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          case TIMESTAMP:
          case PG_TIMESTAMPTZ:
            pk.append(
                DataChangeRecordTypeConvertor.toTimestamp(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          case DATE:
          case PG_DATE:
            pk.append(
                DataChangeRecordTypeConvertor.toDate(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          default:
            throw new IllegalArgumentException(
                "Column name(" + keyColName + ") has unsupported column type(" + keyColType + ")");
        }
      }
      return pk.build();
    } catch (Exception e) {
      throw new Exception("Error generating key: " + e.getMessage());
    }
  }

  private Object getColumnValueFromJson(Column column, JsonNode valuesJson) throws Exception {
    try {
      Type colType = column.type();
      String colName = column.name();
      switch (colType.getCode()) {
        case BOOL:
        case PG_BOOL:
          return DataChangeRecordTypeConvertor.toBoolean(valuesJson, colName, false);
        case INT64:
        case PG_INT8:
          return DataChangeRecordTypeConvertor.toLong(valuesJson, colName, false);
        case FLOAT64:
        case PG_FLOAT8:
          return DataChangeRecordTypeConvertor.toDouble(valuesJson, colName, false);
        case STRING:
        case PG_VARCHAR:
        case PG_TEXT:
          return DataChangeRecordTypeConvertor.toString(valuesJson, colName, false);
        case NUMERIC:
        case PG_NUMERIC:
          return DataChangeRecordTypeConvertor.toNumericBigDecimal(valuesJson, colName, false);
        case JSON:
        case PG_JSONB:
          return DataChangeRecordTypeConvertor.toString(valuesJson, colName, false);
        case BYTES:
        case PG_BYTEA:
          return DataChangeRecordTypeConvertor.toByteArray(valuesJson, colName, false);
        case TIMESTAMP:
        case PG_TIMESTAMPTZ:
          return DataChangeRecordTypeConvertor.toTimestamp(valuesJson, colName, false);
        case DATE:
        case PG_DATE:
          return DataChangeRecordTypeConvertor.toDate(valuesJson, colName, false);
        default:
          throw new IllegalArgumentException(
              "Column name(" + colName + ") has unsupported column type(" + colType + ")");
      }
    } catch (Exception e) {
      throw new Exception("Error getting column value from json: " + e.getMessage());
    }
  }

  private Object getColumnValueFromRow(Column column, Value value) throws Exception {
    try {
      Type colType = column.type();
      String colName = column.name();
      switch (colType.getCode()) {
        case BOOL:
        case PG_BOOL:
          return value.getBool();
        case INT64:
        case PG_INT8:
          return value.getInt64();
        case FLOAT64:
        case PG_FLOAT8:
          return value.getFloat64();
        case STRING:
        case PG_VARCHAR:
        case PG_TEXT:
          return value.getString();
        case NUMERIC:
        case PG_NUMERIC:
          return value.getNumeric();
        case JSON:
        case PG_JSONB:
          return value.getString();
        case BYTES:
        case PG_BYTEA:
          return value.getBytesArray();
        case TIMESTAMP:
        case PG_TIMESTAMPTZ:
          return value.getTimestamp();
        case DATE:
        case PG_DATE:
          return value.getDate();
        default:
          throw new IllegalArgumentException(
              "Column name(" + colName + ") has unsupported column type(" + colType + ")");
      }
    } catch (Exception e) {
      throw new Exception("Error getting column value from row: " + e.getMessage());
    }
  }
}
