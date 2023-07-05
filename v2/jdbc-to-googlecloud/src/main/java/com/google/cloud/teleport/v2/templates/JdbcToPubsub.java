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

import static com.google.cloud.teleport.v2.utils.KMSUtils.maybeDecrypt;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.options.JdbcToPubsubOptions;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link JdbcToPubsub} batch pipeline reads data from JDBC and publishes to Google Cloud
 * PubSub.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/jdbc-to-googlecloud/README_Jdbc_to_PubSub.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Jdbc_to_PubSub",
    category = TemplateCategory.BATCH,
    displayName = "JDBC to Pub/Sub",
    description =
        "A batch pipeline which ingests data from JDBC source and writes to a pre-existing Pub/Sub"
            + " topic as a JSON string. JDBC connection string, user name and password can be"
            + " passed in directly as plaintext or encrypted using the Google Cloud KMS API.  If"
            + " the parameter KMSEncryptionKey is specified, connectionUrl, username, and password"
            + " should be all in encrypted format.",
    additionalHelp =
        "A sample curl command for the KMS API encrypt"
            + " endpoint: curl -s -X POST"
            + " \"https://cloudkms.googleapis.com/v1/projects/your-project/locations/your-path/keyRings/your-keyring/cryptoKeys/your-key:encrypt\""
            + "  -d \"{\\\"plaintext\\\":\"PasteBase64EncodedString\\\"}\"  -H \"Authorization:"
            + " Bearer $(gcloud auth application-default print-access-token)\"  -H \"Content-Type:"
            + " application/json\"",
    optionsClass = JdbcToPubsubOptions.class,
    flexContainerName = "jdbc-to-pubsub",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/jdbc-to-pubsub",
    contactInformation = "https://cloud.google.com/support")
public class JdbcToPubsub {

  /* Logger for class.*/
  private static final Logger LOG = LoggerFactory.getLogger(JdbcToPubsub.class);

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
        if (value == null) {
          json.put(metaData.getColumnLabel(i), JSONObject.NULL);
          continue;
        }

        switch (metaData.getColumnTypeName(i).toLowerCase()) {
          case "clob":
            Clob clobObject = resultSet.getClob(i);
            if (clobObject.length() > Integer.MAX_VALUE) {
              LOG.warn(
                  "The Clob value size {} in column {} exceeds 2GB and will be truncated.",
                  clobObject.length(),
                  metaData.getColumnLabel(i));
            }
            json.put(
                metaData.getColumnLabel(i), clobObject.getSubString(1, (int) clobObject.length()));
            break;
          default:
            json.put(metaData.getColumnLabel(i), value);
        }
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
    UncaughtExceptionLogger.register();

    JdbcToPubsubOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(JdbcToPubsubOptions.class);

    run(options);
  }

  /**
   * Runs a pipeline which reads messages from JDBC and writes to Pub/Sub.
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

    PCollection<String> jdbcData =
        pipeline.apply(
            "readFromJdbc",
            JdbcIO.<String>read()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .withQuery(options.getQuery())
                .withCoder(StringUtf8Coder.of())
                .withRowMapper(new ResultSetToJSONString()));

    jdbcData.apply("writeSuccessMessages", PubsubIO.writeStrings().to(options.getOutputTopic()));

    return pipeline.run();
  }
}
