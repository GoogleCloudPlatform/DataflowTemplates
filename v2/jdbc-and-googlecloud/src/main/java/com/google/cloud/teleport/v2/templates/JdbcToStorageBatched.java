/*
 * Copyright (C) 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.utils.KMSUtils.maybeDecrypt;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Jdbc_to_Storage_Batched:
 *
 * - Single Flex template.
 * - Reads from multiple JDBC tables/queries in parallel (fan-out).
 * - Writes each table to its own GCS directory as Avro:
 * Avro schema: { "data": "JSON string of the row" }
 *
 * Used by Airflow to launch ONE Dataflow job per tenant with multiple tables.
 */
@Template(name = "Jdbc_to_Storage_Batched", category = TemplateCategory.BATCH, displayName = "JDBC to Cloud Storage (Batched, JSON-in-Avro)", description = "Pipeline that reads from multiple JDBC tables/queries and writes each to its own "
        + "Cloud Storage path as Avro with a single 'data' field containing JSON.", optionsClass = JdbcToStorageBatchedOptions.class, flexContainerName = "jdbc-to-storage-batched-v2", documentation = "https://cloud.google.com/dataflow/docs/guides/templates/provided/jdbc-to-cloud-storage", contactInformation = "https://cloud.google.com/support", preview = true, requirements = {
                "The JDBC source must exist prior to running the pipeline.",
                "The output bucket/directory in Cloud Storage must exist prior to running the pipeline."
        })
public class JdbcToStorageBatched {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcToStorageBatched.class);
    private static final Gson GSON = new Gson();

    // Avro schema: single field 'data' (string)
    private static final Schema AVRO_SCHEMA = new Schema.Parser()
            .parse(
                    "{"
                            + "\"type\":\"record\","
                            + "\"name\":\"RowJson\","
                            + "\"fields\":[{\"name\":\"data\",\"type\":\"string\"}]"
                            + "}");

    /** Simple holder for per-table configuration. */
    private static class TableConfig {
        final String tableName;
        final String query;
        final String outputDirectory;
        final String outputFilenamePrefix;

        TableConfig(
                String tableName, String query, String outputDirectory, String outputFilenamePrefix) {
            this.tableName = tableName;
            this.query = query;
            this.outputDirectory = outputDirectory;
            this.outputFilenamePrefix = outputFilenamePrefix;
        }
    }

    /** Maps JDBC rows into GenericRecord with 'data' = JSON row. */
    public static class ResultSetToAvroRecord implements JdbcIO.RowMapper<GenericRecord> {
        @Override
        public GenericRecord mapRow(ResultSet resultSet) throws Exception {
            ResultSetMetaData metaData = resultSet.getMetaData();
            JSONObject json = new JSONObject();

            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                Object value = resultSet.getObject(i);
                String colName = metaData.getColumnLabel(i);

                if (value == null) {
                    json.put(colName, JSONObject.NULL);
                    continue;
                }

                String typeName = metaData.getColumnTypeName(i);
                if (typeName != null && typeName.equalsIgnoreCase("clob")) {
                    Clob clobObject = resultSet.getClob(i);
                    long len = clobObject.length();
                    if (len > Integer.MAX_VALUE) {
                        LOG.warn(
                                "The Clob value size {} in column {} exceeds 2GB and will be truncated.",
                                len,
                                colName);
                    }
                    json.put(
                            colName, clobObject.getSubString(1, (int) Math.min(len, Integer.MAX_VALUE)));
                } else {
                    json.put(colName, value);
                }
            }

            GenericRecord record = new GenericData.Record(AVRO_SCHEMA);
            record.put("data", json.toString());
            return record;
        }
    }

    public static void main(String[] args) {
        UncaughtExceptionLogger.register();
        JdbcToStorageBatchedOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(JdbcToStorageBatchedOptions.class);
        run(options);
    }

    public static PipelineResult run(JdbcToStorageBatchedOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        // --- Parse tableConfigs ---
        String tableConfigsJson = options.getTableConfigs();
        if (tableConfigsJson == null || tableConfigsJson.trim().isEmpty()) {
            throw new IllegalArgumentException("tableConfigs parameter is required.");
        }

        JsonArray array;
        try {
            array = GSON.fromJson(tableConfigsJson, JsonArray.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid tableConfigs JSON: " + e.getMessage(), e);
        }

        if (array == null || array.size() == 0) {
            throw new IllegalArgumentException("No tables specified in tableConfigs.");
        }

        int maxParallelTables = 8;
        if (options.getMaxParallelTables() != null
                && !options.getMaxParallelTables().isEmpty()) {
            try {
                maxParallelTables = Integer.parseInt(options.getMaxParallelTables());
            } catch (NumberFormatException e) {
                LOG.warn(
                        "Invalid maxParallelTables '{}', defaulting to {}",
                        options.getMaxParallelTables(),
                        maxParallelTables);
            }
        }

        LOG.info(
                "Starting Jdbc_to_Storage_Batched_v2. Tables: {}, maxParallelTables(advisory): {}",
                array.size(),
                maxParallelTables);

        // --- Build DataSourceConfiguration using ValueProviders + KMS utils ---
        ValueProvider<String> decryptedConnectionUrl = maybeDecrypt(options.getConnectionUrl(),
                options.getKMSEncryptionKey());

        JdbcIO.DataSourceConfiguration dataSourceConfiguration = JdbcIO.DataSourceConfiguration.create(
                options.getDriverClassName(), decryptedConnectionUrl)
                .withDriverJars(options.getDriverJars());

        if (options.getUsername() != null) {
            dataSourceConfiguration = dataSourceConfiguration.withUsername(
                    maybeDecrypt(options.getUsername(), options.getKMSEncryptionKey()));
        }

        if (options.getPassword() != null) {
            dataSourceConfiguration = dataSourceConfiguration.withPassword(
                    maybeDecrypt(options.getPassword(), options.getKMSEncryptionKey()));
        }

        if (options.getConnectionProperties() != null) {
            dataSourceConfiguration = dataSourceConfiguration.withConnectionProperties(
                    options.getConnectionProperties());
        }

        // --- For each table config, create an independent branch ---
        for (int i = 0; i < array.size(); i++) {
            JsonObject obj = array.get(i).getAsJsonObject();

            if (!obj.has("table_name") || !obj.has("query") || !obj.has("output_directory")) {
                LOG.error(
                        "Invalid tableConfigs[{}]: missing required fields. Skipping: {}",
                        i,
                        obj);
                continue;
            }

            final String tableName = obj.get("table_name").getAsString();
            final String query = obj.get("query").getAsString();
            final String outputDirectory = obj.get("output_directory").getAsString();
            final String prefix = obj.has("output_filename_prefix")
                    ? obj.get("output_filename_prefix").getAsString()
                    : "/part-";

            TableConfig cfg = new TableConfig(tableName, query, outputDirectory, prefix);

            try {
                LOG.info("Configuring table '{}' -> {}", cfg.tableName, cfg.outputDirectory);

                PCollection<GenericRecord> rows = pipeline.apply(
                        "Read_" + cfg.tableName,
                        JdbcIO.<GenericRecord>read()
                                .withDataSourceConfiguration(dataSourceConfiguration)
                                .withQuery(cfg.query)
                                .withRowMapper(new ResultSetToAvroRecord())
                                .withCoder(AvroCoder.of(AVRO_SCHEMA)));

                rows.apply(
                        "Write_" + cfg.tableName,
                        AvroIO.writeGenericRecords(AVRO_SCHEMA)
                                .to(cfg.outputDirectory + cfg.outputFilenamePrefix)
                                .withSuffix(".avro")
                                .withCodec(CodecFactory.deflateCodec(6))
                                .withNumShards(0));

            } catch (Exception e) {
                // Config-level failure for this table is logged and does not stop others.
                LOG.error(
                        "Failed to configure pipeline branch for table '{}'. This table will be skipped.",
                        cfg.tableName,
                        e);
            }
        }

        // Runtime JDBC failures will still fail the job, like any Teleport template.
        return pipeline.run();
    }
}
