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
package com.google.cloud.teleport.it.datadog;

import static com.google.cloud.teleport.it.datadog.DatadogResourceManagerUtils.datadogEntryToMap;
import static com.google.cloud.teleport.it.datadog.DatadogResourceManagerUtils.generateApiKey;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.JsonSchemaBody.jsonSchema;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.beam.it.common.ResourceManager;
import org.apache.beam.it.testcontainers.TestContainerResourceManager;
import org.apache.http.client.entity.GzipCompressingEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.json.JSONObject;
import org.mockserver.client.MockServerClient;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Client for managing Datadog resources.
 *
 * <p>The class supports one mock Datadog server instance.
 *
 * <p>The class is thread-safe.
 *
 * <p>Note: The MockServer TestContainer will only run on M1 Mac's if the Docker version is >=
 * 4.16.0 and the "Use Rosetta for x86/amd64 emulation on Apple Silicon" setting is enabled.
 */
public class DatadogResourceManager extends TestContainerResourceManager<MockServerContainer>
    implements ResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(DatadogResourceManager.class);
  private static final String DEFAULT_MOCKSERVER_CONTAINER_NAME = "mockserver/mockserver";

  // A list of available Mockserver Docker image tags can be found at
  // https://hub.docker.com/r/mockserver/mockserver/tags
  private static final String DEFAULT_MOCKSERVER_CONTAINER_TAG =
      MockServerClient.class.getPackage().getImplementationVersion();

  // See: https://docs.datadoghq.com/api/latest/logs/#send-logs
  private static final String SEND_LOGS_JSON_SCHEMA =
      "{"
          + "  \"$schema\": \"http://json-schema.org/draft-04/schema#\","
          + "  \"type\": \"array\","
          + "  \"items\": ["
          + "    {"
          + "      \"type\": \"object\","
          + "      \"properties\": {"
          + "        \"ddsource\": {"
          + "          \"type\": \"string\""
          + "        },"
          + "        \"ddtags\": {"
          + "          \"type\": \"string\""
          + "        },"
          + "        \"hostname\": {"
          + "          \"type\": \"string\""
          + "        },"
          + "        \"message\": {"
          + "          \"type\": \"string\""
          + "        },"
          + "        \"service\": {"
          + "          \"type\": \"string\""
          + "        }"
          + "      },"
          + "      \"required\": ["
          + "        \"message\""
          + "      ]"
          + "    }"
          + "  ]"
          + "}";

  private static final Gson GSON = new Gson();

  private final String apiKey;
  private final DatadogClientFactory clientFactory;

  private DatadogResourceManager(DatadogResourceManager.Builder builder) {
    this(
        new DatadogClientFactory(),
        new MockServerContainer(
            DockerImageName.parse(builder.containerImageName).withTag(builder.containerImageTag)),
        builder);
  }

  @VisibleForTesting
  DatadogResourceManager(
      DatadogClientFactory clientFactory,
      MockServerContainer container,
      DatadogResourceManager.Builder builder) {
    super(container, builder);

    this.apiKey = builder.apiKey;
    this.clientFactory = clientFactory;

    acceptRequests();
  }

  public static DatadogResourceManager.Builder builder(String testId) {
    return new DatadogResourceManager.Builder(testId);
  }

  /** Returns the port to connect to the mock Datadog server. */
  public int getPort() {
    return super.getPort(MockServerContainer.PORT);
  }

  /**
   * Returns the HTTP endpoint that this mock Datadog server is configured to listen on.
   *
   * @return the HTTP endpoint.
   */
  public String getHttpEndpoint() {
    return String.format("http://%s:%d", getHost(), getPort());
  }

  /**
   * Returns the API endpoint that this mock Datadog server is configured to receive events at.
   *
   * <p>This will be the HTTP endpoint concatenated with <code>'/api/v2/logs'</code>.
   *
   * @return the API endpoint.
   */
  public String getApiEndpoint() {
    return getHttpEndpoint() + "/api/v2/logs";
  }

  /**
   * Returns the Datadog API key used to connect to this mock Datadog server.
   *
   * @return the API key.
   */
  public String getApiKey() {
    return apiKey;
  }

  /**
   * Helper method for converting the given DatadogLogEntry into JSON format.
   *
   * @param event The DatadogLogEntry to parse.
   * @return JSON String.
   */
  private static String datadogEventToJson(DatadogLogEntry event) {
    return new JSONObject(datadogEntryToMap(event)).toString();
  }

  /**
   * Sends the given HTTP event to the mock Datadog server.
   *
   * @param event The {@link DatadogLogEntry} to send to the API.
   * @return True, if the request was successful.
   */
  public synchronized boolean sendHttpEvent(DatadogLogEntry event) {
    return sendHttpEvents(Collections.singletonList(event));
  }

  /**
   * Sends the given HTTP events to the mock Datadog server.
   *
   * @param events The {@link DatadogLogEntry}s to send to the API.
   * @return True, if the request was successful.
   */
  public synchronized boolean sendHttpEvents(Collection<DatadogLogEntry> events) {

    LOG.info("Attempting to send {} events to {}.", events.size(), getApiEndpoint());

    // Construct base API request
    HttpPost httppost = new HttpPost(getApiEndpoint());
    httppost.addHeader("Content-Encoding", "gzip");
    httppost.addHeader("Content-Type", "application/json");
    httppost.addHeader("dd-api-key", apiKey);

    String eventsData = GSON.toJson(events);

    try (CloseableHttpClient httpClient = clientFactory.getHttpClient()) {
      // Set request data
      try {
        httppost.setEntity(new GzipCompressingEntity(new StringEntity(eventsData)));
      } catch (UnsupportedEncodingException e) {
        throw new DatadogResourceManagerException(
            "Error setting HTTP message data to " + eventsData, e);
      }

      // Send request
      try (CloseableHttpResponse response = httpClient.execute(httppost)) {
        // Check error code
        int code = response.getStatusLine().getStatusCode();
        if (code != 202) {
          throw new DatadogResourceManagerException(
              "Received http error code " + code + " sending event.");
        }
      } catch (Exception e) {
        throw new DatadogResourceManagerException("Error sending event.", e);
      }
    } catch (IOException e) {
      throw new DatadogResourceManagerException("Error with HTTP client.", e);
    }

    LOG.info("Successfully sent {} events.", events.size());

    return true;
  }

  /**
   * Return a list of all Datadog entries retrieved from the mock Datadog server.
   *
   * @return All Datadog entries on the server.
   */
  public synchronized List<DatadogLogEntry> getEntries() {
    MockServerClient serviceClient = clientFactory.getServiceClient(getHost(), getPort());
    LOG.info("Reading events from Datadog");

    List<DatadogLogEntry> results = new ArrayList<>();
    for (HttpRequest request : serviceClient.retrieveRecordedRequests(null)) {
      String requestBody = request.getBodyAsString();

      List<AutoValue_DatadogLogEntry> events;
      try {
        events = GSON.fromJson(requestBody, new TypeToken<>() {});
      } catch (JsonParseException e) {
        throw new DatadogResourceManagerException(
            "Received a request with invalid JSON: " + requestBody, e);
      }

      results.addAll(events);
    }

    LOG.info("Successfully retrieved {} results.", results.size());
    return results;
  }

  /**
   * Sets up request definitions the mock Datadog server expects to receive, all other requests
   * return 404.
   */
  private void acceptRequests() {
    MockServerClient serviceClient = clientFactory.getServiceClient(getHost(), getPort());
    serviceClient
        .when(
            request()
                .withMethod("POST")
                .withContentType(MediaType.APPLICATION_JSON)
                .withHeader("dd-api-key", apiKey)
                .withHeader("content-encoding", "(?i)^(?:identity|gzip|deflate)$")
                .withBody(jsonSchema(SEND_LOGS_JSON_SCHEMA)))
        .respond(response().withStatusCode(202));
  }

  /** Builder for {@link DatadogResourceManager}. */
  public static final class Builder
      extends TestContainerResourceManager.Builder<DatadogResourceManager> {

    private String apiKey;

    private Builder(String testId) {
      super(testId, DEFAULT_MOCKSERVER_CONTAINER_NAME, DEFAULT_MOCKSERVER_CONTAINER_TAG);
    }

    /**
     * Manually set the Datadog API key to the given key. This key will be used by the resource
     * manager to authenticate with the mock Datadog server.
     *
     * @param apiKey the API key for the mock Datadog server.
     * @return this builder with the API key manually set.
     */
    public Builder setApiKey(String apiKey) {
      this.apiKey = apiKey;
      return this;
    }

    @Override
    public DatadogResourceManager build() {
      apiKey = apiKey == null ? generateApiKey() : apiKey;
      return new DatadogResourceManager(this);
    }
  }
}
