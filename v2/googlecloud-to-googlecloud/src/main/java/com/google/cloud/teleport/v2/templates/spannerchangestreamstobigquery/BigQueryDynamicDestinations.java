/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.auto.value.AutoValue;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils.BigQueryUtils;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils.SpannerToBigQueryUtils;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link BigQueryDynamicDestinations} loads into BigQuery tables in a dynamic fashion. The
 * destination table is inferred from the provided {@link TableRow}.
 */
public final class BigQueryDynamicDestinations
    extends DynamicDestinations<TableRow, KV<String, List<TableFieldSchema>>> {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryDynamicDestinations.class);

  private final String bigQueryProject, bigQueryDataset, bigQueryTableTemplate;
  private final Boolean useStorageWriteApi;
  private final ImmutableSet<String> ignoreFields;

  public static BigQueryDynamicDestinations of(
      BigQueryDynamicDestinationsOptions bigQueryDynamicDestinationsOptions) {
    Dialect dialect = getDialect(bigQueryDynamicDestinationsOptions.getSpannerConfig());
    return new BigQueryDynamicDestinations(bigQueryDynamicDestinationsOptions);
  }

  private BigQueryDynamicDestinations(
      BigQueryDynamicDestinationsOptions bigQueryDynamicDestinationsOptions) {
    this.ignoreFields = bigQueryDynamicDestinationsOptions.getIgnoreFields();
    this.bigQueryProject = bigQueryDynamicDestinationsOptions.getBigQueryProject();
    this.bigQueryDataset = bigQueryDynamicDestinationsOptions.getBigQueryDataset();
    this.bigQueryTableTemplate = bigQueryDynamicDestinationsOptions.getBigQueryTableTemplate();
    this.useStorageWriteApi = bigQueryDynamicDestinationsOptions.getUseStorageWriteApi();
  }

  private String getTableName(TableRow tableRow) {
    String bigQueryTableName =
        BigQueryConverters.formatStringTemplate(bigQueryTableTemplate, tableRow);

    return String.format("%s:%s.%s", bigQueryProject, bigQueryDataset, bigQueryTableName);
  }

  @Override
  public Coder<KV<String, List<TableFieldSchema>>> getDestinationCoder() {
    return KvCoder.of(StringUtf8Coder.of(), ListCoder.of(TableFieldSchemaCoder.of()));
  }

  @Override
  public KV<String, List<TableFieldSchema>> getDestination(ValueInSingleWindow<TableRow> element) {
    TableRow tableRow = element.getValue();
    // Get List<TableFieldSchema> for both user columns and metadata columns.
    return KV.of(getTableName(tableRow), getFields(tableRow));
  }

  @Override
  public TableDestination getTable(KV<String, List<TableFieldSchema>> destination) {
    return new TableDestination(destination.getKey(), "BigQuery changelog table.");
  }

  @Override
  public TableSchema getSchema(KV<String, List<TableFieldSchema>> destination) {
    List<TableFieldSchema> filteredFields = new ArrayList<>();
    for (TableFieldSchema field : destination.getValue()) {
      if (!ignoreFields.contains(field.getName())) {
        filteredFields.add(field);
      }
    }

    return new TableSchema().setFields(filteredFields);
  }

  // Returns List<TableFieldSchema> for user columns and metadata columns based on the parameter
  // TableRow.
  List<TableFieldSchema> getFields(TableRow tableRow) {
    // Add all user data fields (excluding metadata fields stored in metadataColumns).
    List<TableFieldSchema> fields =
        SpannerToBigQueryUtils.tableRowColumnsToBigQueryIOFields(tableRow, this.useStorageWriteApi);

    // Add all metadata fields.
    String requiredMode = Field.Mode.REQUIRED.name();
    fields.add(
        new TableFieldSchema()
            .setName(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_MOD_TYPE)
            .setType(StandardSQLTypeName.STRING.name())
            .setMode(requiredMode));
    fields.add(
        new TableFieldSchema()
            .setName(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_TABLE_NAME)
            .setType(StandardSQLTypeName.STRING.name())
            .setMode(requiredMode));
    fields.add(
        new TableFieldSchema()
            .setName(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_SPANNER_COMMIT_TIMESTAMP)
            .setType(StandardSQLTypeName.TIMESTAMP.name())
            .setMode(requiredMode));
    fields.add(
        new TableFieldSchema()
            .setName(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_SERVER_TRANSACTION_ID)
            .setType(StandardSQLTypeName.STRING.name())
            .setMode(requiredMode));
    fields.add(
        new TableFieldSchema()
            .setName(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_RECORD_SEQUENCE)
            .setType(StandardSQLTypeName.STRING.name())
            .setMode(requiredMode));
    fields.add(
        new TableFieldSchema()
            .setName(
                BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_IS_LAST_RECORD_IN_TRANSACTION_IN_PARTITION)
            .setType(StandardSQLTypeName.BOOL.name())
            .setMode(requiredMode));
    fields.add(
        new TableFieldSchema()
            .setName(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_NUMBER_OF_RECORDS_IN_TRANSACTION)
            .setType(StandardSQLTypeName.INT64.name())
            .setMode(requiredMode));
    fields.add(
        new TableFieldSchema()
            .setName(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_NUMBER_OF_PARTITIONS_IN_TRANSACTION)
            .setType(StandardSQLTypeName.INT64.name())
            .setMode(requiredMode));
    if (!useStorageWriteApi) {
      fields.add(
          new TableFieldSchema()
              .setName(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_BIGQUERY_COMMIT_TIMESTAMP)
              .setType(StandardSQLTypeName.TIMESTAMP.name())
              .setMode(requiredMode));
    }

    return fields;
  }

  /**
   * {@link BigQueryDynamicDestinationsOptions} provides options to initialize {@link
   * BigQueryDynamicDestinations}.
   */
  @AutoValue
  public abstract static class BigQueryDynamicDestinationsOptions implements Serializable {
    public abstract SpannerConfig getSpannerConfig();

    public abstract String getChangeStreamName();

    public abstract ImmutableSet<String> getIgnoreFields();

    public abstract String getBigQueryProject();

    public abstract String getBigQueryDataset();

    public abstract String getBigQueryTableTemplate();

    public abstract Boolean getUseStorageWriteApi();

    static Builder builder() {
      return new AutoValue_BigQueryDynamicDestinations_BigQueryDynamicDestinationsOptions.Builder();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setSpannerConfig(SpannerConfig spannerConfig);

      abstract Builder setChangeStreamName(String changeStreamName);

      abstract Builder setIgnoreFields(ImmutableSet<String> ignoreFields);

      abstract Builder setBigQueryProject(String bigQueryProject);

      abstract Builder setBigQueryDataset(String bigQueryDataset);

      abstract Builder setBigQueryTableTemplate(String bigQueryTableTemplate);

      abstract Builder setUseStorageWriteApi(Boolean useStorageWriteApi);

      abstract BigQueryDynamicDestinationsOptions build();
    }
  }

  private static Dialect getDialect(SpannerConfig spannerConfig) {
    DatabaseClient databaseClient = SpannerAccessor.getOrCreate(spannerConfig).getDatabaseClient();
    return databaseClient.getDialect();
  }

  /**
   * {@link TableFieldSchemaCoder} provides custom coder for TableFieldSchema with deterministic
   * serialization. This coder is only used within the context of this file where TableFieldSchema
   * objects are created in {@link BigQueryDynamicDestinations#getFields(TableRow)} method
   * deterministically.
   */
  private static class TableFieldSchemaCoder extends AtomicCoder<TableFieldSchema> {

    private static final ObjectMapper OBJECT_MAPPER;
    private static final TypeDescriptor<TableFieldSchema> TYPE_DESCRIPTOR;
    private static final TableFieldSchemaCoder INSTANCE;

    static {
      OBJECT_MAPPER =
          new ObjectMapper()
              .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
              .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
      TYPE_DESCRIPTOR = new TypeDescriptor<TableFieldSchema>() {};
      INSTANCE = new TableFieldSchemaCoder();
    }

    private static TableFieldSchemaCoder of() {
      return INSTANCE;
    }

    private TableFieldSchemaCoder() {}

    @Override
    public void encode(TableFieldSchema value, OutputStream outStream) throws IOException {
      String strValue = OBJECT_MAPPER.writeValueAsString(value);
      StringUtf8Coder.of().encode(strValue, outStream);
    }

    @Override
    public TableFieldSchema decode(InputStream inStream) throws IOException {
      String strValue = StringUtf8Coder.of().decode(inStream);
      return OBJECT_MAPPER.readValue(strValue, TableFieldSchema.class);
    }

    @Override
    public long getEncodedElementByteSize(TableFieldSchema value) throws Exception {
      String strValue = OBJECT_MAPPER.writeValueAsString(value);
      return StringUtf8Coder.of().getEncodedElementByteSize(strValue);
    }

    @Override
    public TypeDescriptor<TableFieldSchema> getEncodedTypeDescriptor() {
      return TYPE_DESCRIPTOR;
    }

    @Override
    public void verifyDeterministic() throws Coder.NonDeterministicException {}
  }
}
