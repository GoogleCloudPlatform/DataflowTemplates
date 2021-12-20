/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.v2.io.DynamicJdbcIO;
import com.google.cloud.teleport.v2.options.JdbcToPubsubOptions;
import com.google.cloud.teleport.v2.utils.KMSEncryptedNestedValue;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link JdbcToPubsub} streaming pipeline reads data from JdbcIO and publishes to Google Cloud
 * PubSub. <br>
 */
public class JdbcToPubsub {

  /* Logger for class.*/
  private static final Logger LOG = LoggerFactory.getLogger(JdbcToPubsub.class);

  private static KMSEncryptedNestedValue maybeDecrypt(String unencryptedValue, String kmsKey) {
    return new KMSEncryptedNestedValue(unencryptedValue, kmsKey);
  }

  /**
   * {@link JdbcIO.RowMapper} implementation to convert Jdbc ResultSet rows to UTF-8 encoded JSONs.
   */
  public static class ResultSetToJSONString implements JdbcIO.RowMapper<String> {

    @Override
    public String mapRow(ResultSet resultSet) throws Exception {
      ResultSetMetaData metaData = resultSet.getMetaData();
      JSONObject json = new JSONObject();

      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        Object value = resultSet.getObject(i);
        // JSONObject.put() does not support null values. The exception is JSONObject.NULL
        json.put(metaData.getColumnLabel(i), value == null ? JSONObject.NULL : value);
      }

      return json.toString();
    }
  }

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args) {
    JdbcToPubsubOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(JdbcToPubsubOptions.class);

    run(options);
  }

  /**
   * Runs a pipeline which reads message from JdbcIO and writes to Pub/Sub.
   *
   * @param options The execution options.
   * @return The pipeline result.
   */
  public static PipelineResult run(JdbcToPubsubOptions options) {
    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    LOG.info("Starting Jdbc-To-PubSub Pipeline.");

    /*
     * Steps:
     *  1) Read data from a Jdbc Table
     *  2) Write to Pub/Sub topic
     */
    DynamicJdbcIO.DynamicDataSourceConfiguration dataSourceConfiguration =
        DynamicJdbcIO.DynamicDataSourceConfiguration.create(
                options.getDriverClassName(),
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

    PCollection<String> jdbcData =
        pipeline.apply(
            "readFromJdbc",
            DynamicJdbcIO.<String>read()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .withQuery(options.getQuery())
                .withCoder(StringUtf8Coder.of())
                .withRowMapper(new ResultSetToJSONString()));

    jdbcData.apply("writeSuccessMessages", PubsubIO.writeStrings().to(options.getOutputTopic()));

    return pipeline.run();
  }
}
