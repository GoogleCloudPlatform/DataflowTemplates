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
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.io.DynamicJdbcIO;
import com.google.cloud.teleport.v2.options.PubsubToJdbcOptions;
import com.google.cloud.teleport.v2.transforms.ErrorConverters;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Splitter;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
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
 * The {@link PubsubToJdbc} streaming pipeline reads data from Google Cloud PubSub and publishes to
 * JDBC. <br>
 */
@Template(
    name = "Pubsub_to_Jdbc",
    category = TemplateCategory.STREAMING,
    displayName = "Pub/Sub to JDBC",
    description =
        "A streaming pipeline which ingests data in the form of json strings from Pub/Sub"
            + " subscription and writes to a JDBC table. JDBC connection string, user name and"
            + " password can be passed in directly as plaintext or encrypted using the Google Cloud"
            + " KMS API.  If the parameter KMSEncryptionKey is specified, connectionUrl, username,"
            + " and password should be all in encrypted format. A sample curl command for the KMS"
            + " API encrypt endpoint: curl -s -X POST"
            + " \"https://cloudkms.googleapis.com/v1/projects/your-project/locations/your-path/keyRings/your-keyring/cryptoKeys/your-key:encrypt\""
            + "  -d \"{\\\"plaintext\\\":\\\"PasteBase64EncodedString\\\"}\"  -H \"Authorization:"
            + " Bearer $(gcloud auth application-default print-access-token)\" -H \"Content-Type:"
            + " application/json\"",
    optionsClass = PubsubToJdbcOptions.class,
    flexContainerName = "pubsub-to-jdbc",
    contactInformation = "https://cloud.google.com/support")
public class PubsubToJdbc {

  /* Logger for class.*/
  private static final Logger LOG = LoggerFactory.getLogger(PubsubToJdbc.class);

  /** String/String Coder for FailsafeElement. */
  public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    PubsubToJdbcOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(PubsubToJdbcOptions.class);

    run(options);
  }

  /**
   * Runs a pipeline which reads message from Pub/Sub and writes to JDBC.
   *
   * @param options The execution options.
   * @return The pipeline result.
   */
  public static PipelineResult run(PubsubToJdbcOptions options) {
    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    LOG.info("Starting Pubsub-to-Jdbc Pipeline.");

    /*
     * Steps:
     *  1) Read data from a Pub/Sub subscription
     *  2) Write to Jdbc Table
     *  3) Write errors to deadletter topic
     */
    PCollection<String> pubsubData =
        pipeline.apply(
            "readFromPubSubSubscription",
            PubsubIO.readStrings().fromSubscription(options.getInputSubscription()));

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

    PCollection<FailsafeElement<String, String>> errors =
        pubsubData
            .apply(
                "writeToJdbc",
                DynamicJdbcIO.<String>write()
                    .withDataSourceConfiguration(dataSourceConfiguration)
                    .withStatement(options.getStatement())
                    .withPreparedStatementSetter(
                        new MapJsonStringToQuery(getKeyOrder(options.getStatement()))))
            .setCoder(FAILSAFE_ELEMENT_CODER);

    errors.apply(
        "WriteFailedRecords",
        ErrorConverters.WriteStringMessageErrorsToPubSub.newBuilder()
            .setErrorRecordsTopic(options.getOutputDeadletterTopic())
            .build());

    return pipeline.run();
  }

  /** The {@link JdbcIO.PreparedStatementSetter} implementation for mapping json string to query. */
  public static class MapJsonStringToQuery implements JdbcIO.PreparedStatementSetter<String> {

    List<String> keyOrder;

    public MapJsonStringToQuery(List<String> keyOrder) {
      this.keyOrder = keyOrder;
    }

    public void setParameters(String element, PreparedStatement query) throws SQLException {
      try {
        JSONObject object = new JSONObject(element);
        for (int i = 0; i < keyOrder.size(); i++) {
          String key = keyOrder.get(i);
          if (object.get(key) == JSONObject.NULL) {
            query.setNull(i + 1, Types.NULL);
          } else {
            query.setObject(i + 1, object.get(key));
          }
        }
      } catch (Exception e) {
        LOG.error("Error while mapping PubSub strings to JDBC: {}", e.getMessage());
      }
    }
  }

  private static List<String> getKeyOrder(String statement) {
    int startIndex = statement.indexOf("(");
    int endIndex = statement.indexOf(")");
    String data = statement.substring(startIndex + 1, endIndex);
    return Splitter.on(',').splitToList(data);
  }
}
