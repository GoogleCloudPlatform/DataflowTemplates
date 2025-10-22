/*
 * Copyright (C) 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.utils.KMSUtils.maybeDecrypt;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.options.JdbcToStorageOptions;
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
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link JdbcToStorage} batch pipeline reads data from JDBC and writes to Cloud Storage as Avro.
 * The Avro schema contains a single field 'data' (string) with the JSON representation of each row.
 */
@Template(
    name = "Jdbc_to_Storage",
    category = TemplateCategory.BATCH,
    displayName = "JDBC to Cloud Storage (Avro)",
    description = "Pipeline that reads data from a JDBC source and saves it to Cloud Storage in Avro format, with each query result saved as JSON in a 'data' field.",
    optionsClass = JdbcToStorageOptions.class,
    flexContainerName = "jdbc-to-storage",
    documentation = "https://cloud.google.com/dataflow/docs/guides/templates/provided/jdbc-to-cloud-storage",
    contactInformation = "https://cloud.google.com/support",
    preview = true,
    requirements = {
      "The JDBC source must exist prior to running the pipeline.",
      "The output bucket/directory in Cloud Storage must exist prior to running the pipeline."
    })
public class JdbcToStorage {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcToStorage.class);

  // Avro schema: single field 'data' (string)
  private static final Schema AVRO_SCHEMA = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"RowJson\",\"fields\":[{\"name\":\"data\",\"type\":\"string\"}]}"
  );

  /**
   * JdbcIO.RowMapper implementation: converts each ResultSet row into an Avro GenericRecord,
   * with the 'data' field containing the row as JSON.
   */
  public static class ResultSetToAvroRecord implements JdbcIO.RowMapper<GenericRecord> {
    @Override
    public GenericRecord mapRow(ResultSet resultSet) throws Exception {
      ResultSetMetaData metaData = resultSet.getMetaData();
      JSONObject json = new JSONObject();
      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        Object value = resultSet.getObject(i);
        if (value == null) {
          json.put(metaData.getColumnLabel(i), JSONObject.NULL);
          continue;
        }
        switch (metaData.getColumnTypeName(i).toLowerCase()) {
          case "clob":
            Clob clobObject = resultSet.getClob(i);
            if (clobObject.length() > Integer.MAX_VALUE) {
              LOG.warn("The Clob value size {} in column {} exceeds 2GB and will be truncated.",
                  clobObject.length(), metaData.getColumnLabel(i));
            }
            json.put(metaData.getColumnLabel(i),
                clobObject.getSubString(1, (int) clobObject.length()));
            break;
          default:
            json.put(metaData.getColumnLabel(i), value);
        }
      }
      GenericRecord record = new GenericData.Record(AVRO_SCHEMA);
      record.put("data", json.toString());
      return record;
    }
  }

  /**
   * Main entry point for pipeline execution.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();
    JdbcToStorageOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(JdbcToStorageOptions.class);
    run(options);
  }

  /**
   * Runs a pipeline which reads from JDBC and writes Avro files to Cloud Storage.
   */
  public static PipelineResult run(JdbcToStorageOptions options) {
    Pipeline pipeline = Pipeline.create(options);
    LOG.info("Starting Jdbc-To-Storage Pipeline.");

    JdbcIO.DataSourceConfiguration dataSourceConfiguration =
        JdbcIO.DataSourceConfiguration.create(
                StaticValueProvider.of(options.getDriverClassName()),
                maybeDecrypt(options.getConnectionUrl(), options.getKMSEncryptionKey()))
            .withDriverJars(options.getDriverJars());
    if (options.getUsername() != null) {
      dataSourceConfiguration =
          dataSourceConfiguration.withUsername(
              maybeDecrypt(options.getUsername(), options.getKMSEncryptionKey()));
    }
    if (options.getPassword() != null) {
      dataSourceConfiguration =
          dataSourceConfiguration.withPassword(
              maybeDecrypt(options.getPassword(), options.getKMSEncryptionKey()));
    }
    if (options.getConnectionProperties() != null) {
      dataSourceConfiguration =
          dataSourceConfiguration.withConnectionProperties(options.getConnectionProperties());
    }

    PCollection<GenericRecord> jdbcData =
        pipeline.apply(
            "readFromJdbc",
            JdbcIO.<GenericRecord>read()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .withQuery(options.getQuery())
                .withCoder(AvroCoder.of(GenericRecord.class, AVRO_SCHEMA))
                .withRowMapper(new ResultSetToAvroRecord()));

    String outputPrefix =
        options.getOutputDirectory()
            + (options.getOutputFilenamePrefix() != null
                ? options.getOutputFilenamePrefix()
                : "part-");

    jdbcData.apply(
        "writeToAvro",
        AvroIO.writeGenericRecords(AVRO_SCHEMA)
            .to(outputPrefix)
            .withSuffix(".avro")
            // substitui DEFAULT_DEFLATE_CODEC pela API nova
            .withCodec(CodecFactory.deflateCodec(6))
            .withNumShards(0) // Let Beam decide
        );

    return pipeline.run();
  }
}
