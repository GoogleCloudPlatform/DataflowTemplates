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
package com.google.cloud.teleport.it.splunk;

import static com.google.cloud.teleport.it.splunk.SplunkResourceManagerUtils.generateHecToken;
import static com.google.cloud.teleport.it.splunk.SplunkResourceManagerUtils.generateSplunkPassword;
import static com.google.cloud.teleport.it.splunk.SplunkResourceManagerUtils.splunkEventToMap;

import com.google.cloud.teleport.it.common.ResourceManager;
import com.google.cloud.teleport.it.common.testcontainers.TestContainerResourceManager;
import com.google.common.annotations.VisibleForTesting;
import com.splunk.Job;
import com.splunk.ResultsReader;
import com.splunk.ResultsReaderXml;
import com.splunk.Service;
import com.splunk.ServiceArgs;
import java.io.IOException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.io.splunk.SplunkEvent;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;

/**
 * Client for managing Kafka resources.
 *
 * <p>The class supports one Splunk server instance.
 *
 * <p>The class is thread-safe.
 *
 * <p>Note: The Splunk TestContainer will only run on M1 Mac's if the Docker version is >= 4.16.0
 * and the "Use Rosetta for x86/amd64 emulation on Apple Silicon" setting is enabled.
 */
public class DefaultSplunkResourceManager extends TestContainerResourceManager<SplunkContainer>
    implements ResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultSplunkResourceManager.class);
  private static final String DEFAULT_SPLUNK_CONTAINER_NAME = "splunk/splunk";

  // A list of available Splunk Docker image tags can be found at
  // https://hub.docker.com/r/splunk/splunk/tags
  private static final String DEFAULT_SPLUNK_CONTAINER_TAG = "8.2";

  // 8088 is the default port that Splunk HEC is configured to listen on
  private static final int DEFAULT_SPLUNK_HEC_INTERNAL_PORT = 8088;
  // 8089 is the default port that Splunkd is configured to listen on
  private static final int DEFAULT_SPLUNKD_INTERNAL_PORT = 8089;

  private static final String DEFAULT_SPLUNK_USERNAME = "admin";

  private final ServiceArgs loginArgs;
  private final int hecPort;
  private final String hecScheme;
  private final String hecToken;

  @SuppressWarnings("resource")
  private DefaultSplunkResourceManager(DefaultSplunkResourceManager.Builder builder) {
    this(
        new SplunkContainer(
                DockerImageName.parse(builder.containerImageName)
                    .withTag(builder.containerImageTag))
            .withSplunkdSslDisabled(),
        builder);
  }

  @VisibleForTesting
  DefaultSplunkResourceManager(
      SplunkContainer container, DefaultSplunkResourceManager.Builder builder) {
    super(setup(container, builder), builder);

    // Validate builder args
    if (builder.useStaticContainer) {
      if (builder.hecPort < 0 || builder.splunkdPort < 0) {
        throw new SplunkResourceManagerException(
            "This manager was configured to use a static resource, but the hecPort and splunkdPort were not properly set.");
      }
    }

    String username = DEFAULT_SPLUNK_USERNAME;
    if (builder.useStaticContainer && builder.username != null) {
      username = builder.username;
    }
    hecPort =
        builder.useStaticContainer ? builder.hecPort : container.getMappedPort(builder.hecPort);
    int splunkdPort =
        builder.useStaticContainer
            ? builder.splunkdPort
            : container.getMappedPort(builder.splunkdPort);

    // TODO - add support for https scheme
    String splunkdScheme = "http";
    hecScheme = "http";

    hecToken = builder.hecToken;

    loginArgs = new ServiceArgs();
    loginArgs.setPort(splunkdPort);
    loginArgs.setHost(this.getHost());
    loginArgs.setScheme(splunkdScheme);
    loginArgs.setUsername(username);
    loginArgs.setPassword(builder.password);
  }

  /**
   * Helper method for injecting config information from the builder into the given SplunkContainer.
   *
   * @param container The SplunkContainer before config info is injected.
   * @param builder The DefaultSplunkResourceManager.Builder to extract config info from.
   * @return The SplunkContainer with all config info injected.
   */
  private static SplunkContainer setup(SplunkContainer container, Builder builder) {
    builder.hecPort = builder.hecPort < 0 ? DEFAULT_SPLUNK_HEC_INTERNAL_PORT : builder.hecPort;
    builder.splunkdPort =
        builder.splunkdPort < 0 ? DEFAULT_SPLUNKD_INTERNAL_PORT : builder.splunkdPort;
    builder.hecToken = builder.hecToken == null ? generateHecToken() : builder.hecToken;
    builder.password = builder.password == null ? generateSplunkPassword() : builder.password;
    // TODO - add support for ssl
    return container
        .withDefaultsFile(
            Transferable.of(
                String.format(
                    "splunk:\n"
                        + "  hec:\n"
                        + "    enable: true\n"
                        + "    ssl: %b\n"
                        + "    port: %s\n"
                        + "    token: %s",
                    false, builder.hecPort, builder.hecToken)))
        .withPassword(builder.password);
  }

  public static DefaultSplunkResourceManager.Builder builder(String testId) {
    return new DefaultSplunkResourceManager.Builder(testId);
  }

  /**
   * Returns the HTTP endpoint that this Splunk server is configured to listen on.
   *
   * @return the HTTP endpoint.
   */
  public String getHttpEndpoint() {
    return String.format("%s://%s:%d", hecScheme, getHost(), hecPort);
  }

  /**
   * Returns the HTTP Event Collector (HEC) endpoint that this Splunk server is configured to
   * receive events at.
   *
   * <p>This will be the HTTP endpoint concatenated with <code>'/services/collector/event'</code>.
   *
   * @return the HEC service endpoint.
   */
  public String getHecEndpoint() {
    return getHttpEndpoint() + "/services/collector/event";
  }

  /**
   * Returns the Splunk Http Event Collector (HEC) authentication token used to connect to this
   * Splunk instance's HEC service.
   *
   * @return the HEC authentication token.
   */
  public String getHecToken() {
    return hecToken;
  }

  /**
   * Helper method for converting the given SplunkEvent into JSON format.
   *
   * @param event The SplunkEvent to parse.
   * @return JSON String.
   */
  private static String splunkEventToJson(SplunkEvent event) {
    return new JSONObject(splunkEventToMap(event)).toString();
  }

  /**
   * Sends the given HTTP event to the Splunk Http Event Collector (HEC) service.
   *
   * <p>Note: Setting the <code>index</code> field in the Splunk event requires the index already
   * being configured in the Splunk instance. Unless using a static Splunk instance, omit this field
   * from the event.
   *
   * @param event The SpunkEvent to send to the HEC service.
   * @return True, if the request was successful.
   */
  public synchronized boolean sendHttpEvent(SplunkEvent event) {
    return sendHttpEvents(List.of(event));
  }

  /**
   * Sends the given HTTP events to the Splunk Http Event Collector (HEC) service.
   *
   * <p>Note: Setting the <code>index</code> field in the Splunk event requires the index already
   * being configured in the Splunk instance. Unless using a static Splunk instance, omit this field
   * from the events.
   *
   * @param events The SpunkEvents to send to the HEC service.
   * @return True, if the request was successful.
   */
  public synchronized boolean sendHttpEvents(Collection<SplunkEvent> events) {

    LOG.info("Attempting to send {} events to {}.", events.size(), getHecEndpoint());

    // Construct base HEC request
    HttpPost httppost = new HttpPost(getHecEndpoint());
    httppost.addHeader("Authorization", "Splunk " + hecToken);

    // Loop over events and send one-by-one
    events.forEach(
        event -> {
          try (CloseableHttpClient httpclient = HttpClientBuilder.create().build()) {
            String eventStr = splunkEventToJson(event);
            LOG.info("Sending HTTP event: {}", eventStr);

            // Send request
            HttpResponse response;
            try {
              httppost.setEntity(new StringEntity(eventStr));
              response = httpclient.execute(httppost);
            } catch (Exception e) {
              throw new SplunkResourceManagerException("Error sending event.", e);
            }

            // Check error code
            int code = response.getStatusLine().getStatusCode();
            if (code != 200) {
              throw new SplunkResourceManagerException(
                  "Received http error code " + code + " sending event.");
            }
          } catch (IOException e) {
            throw new SplunkResourceManagerException("Error closing HTTP client.");
          }
        });

    LOG.info("Successfully sent {} events.", events.size());

    return true;
  }

  /**
   * Return a list of all Splunk events retrieved from the Splunk server.
   *
   * @return All Splunk events on the server.
   */
  public synchronized List<SplunkEvent> getEvents() {
    return getEvents("search");
  }

  /**
   * Return a list of Splunk events retrieved from the Splunk server based on the given query.
   *
   * <p>e.g. query: <code>'search source=mySource sourcetype=mySourceType host=myHost'</code>
   *
   * @param query The query to filter events by.
   * @return
   */
  public synchronized List<SplunkEvent> getEvents(String query) {
    // Initialize the SDK client
    Service service = Service.connect(loginArgs);

    // Run a simple search by first creating the search job
    Job job = service.getJobs().create(query);

    LOG.info("Reading events from Splunk using query {}.", query);

    // Wait up to 1 minute for search results to be ready
    Awaitility.await("Retrieving events from Splunk")
        .atMost(Duration.ofMinutes(1))
        .pollInterval(Duration.ofMillis(500))
        .until(job::isDone);

    // Read results
    List<SplunkEvent> results = new ArrayList<>();
    try {
      ResultsReader reader = new ResultsReaderXml(job.getEvents());
      reader.forEach(
          event -> results.add(
              SplunkEvent.newBuilder()
                  .withEvent(event.get("_raw"))
                  .withSource(event.get("source"))
                  .withSourceType(event.get("_sourcetype"))
                  .withHost(event.get("host"))
                  .withTime(
                      OffsetDateTime.parse(
                              event.get("_time"), DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                          .toInstant()
                          .toEpochMilli())
                  .create()));

    } catch (Exception e) {
      throw new SplunkResourceManagerException("Error parsing XML results from Splunk.", e);
    }

    LOG.info("Successfully retrieved {} events.", results.size());
    return results;
  }

  /** Builder for {@link DefaultSplunkResourceManager}. */
  public static final class Builder
      extends TestContainerResourceManager.Builder<DefaultSplunkResourceManager> {

    private String username;
    private String password;
    private String hecToken;
    private int hecPort;
    private int splunkdPort;

    private Builder(String testId) {
      super(testId);
      this.containerImageName = DEFAULT_SPLUNK_CONTAINER_NAME;
      this.containerImageTag = DEFAULT_SPLUNK_CONTAINER_TAG;
      this.hecPort = -1;
      this.splunkdPort = -1;
    }

    /**
     * Set the username used to connect to a static Splunk instance.
     *
     * <p>Note: This method should only be used if {@code useStaticContainer()} is also called.
     *
     * @param username the username for the Splunk instance.
     * @return this builder with the username manually set.
     */
    public Builder setUsername(String username) {
      this.username = username;
      return this;
    }

    /**
     * Manually set the Splunk password to the given password. This password will be used by the
     * resource manager to authenticate with Splunk.
     *
     * @param password the password for the Splunk instance.
     * @return this builder with the password manually set.
     */
    public Builder setPassword(String password) {
      this.password = password;
      return this;
    }

    /**
     * Manually set the Splunk HTTP Event Collector (HEC) token to the given token. This token will
     * be used by the resource manager to authenticate with Splunk.
     *
     * @param hecToken the HEC token for the Splunk instance.
     * @return this builder with the HEC token manually set.
     */
    public Builder setHecToken(String hecToken) {
      this.hecToken = hecToken;
      return this;
    }

    @Override
    public Builder setHost(String containerHost) {
      super.setHost(containerHost);
      this.port = 0;
      return this;
    }

    @Override
    public Builder setPort(int port) {
      throw new UnsupportedOperationException(
          "Please use setHecPort() and setSplunkdPort() instead.");
    }

    /**
     * Sets the port that the Splunk Http Event Collector (HEC) service is hosted on.
     *
     * @param port the port hosting the HEC service.
     * @return this builder object with the HEC port set.
     */
    public Builder setHecPort(int port) {
      this.hecPort = port;
      return this;
    }

    /**
     * Sets the port that the Splunkd service is hosted on.
     *
     * @param port the port hosting the Splunkd service.
     * @return this builder object with the Splunkd port set.
     */
    public Builder setSplunkdPort(int port) {
      this.splunkdPort = port;
      return this;
    }

    @Override
    public DefaultSplunkResourceManager build() {
      return new DefaultSplunkResourceManager(this);
    }
  }
}
