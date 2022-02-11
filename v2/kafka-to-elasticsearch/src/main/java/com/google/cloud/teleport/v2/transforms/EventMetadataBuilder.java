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
package com.google.cloud.teleport.v2.transforms;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.teleport.v2.elasticsearch.utils.ElasticsearchUtils;
import com.google.cloud.teleport.v2.options.KafkaToElasticsearchOptions;
import java.io.Serializable;
import java.util.NoSuchElementException;

/**
 * EventMetadataBuilder is used to insert metadata required by Elasticsearch. The metadata helps
 * Elasticsearch to visualize events on the dashboards, also uniform message format is needed for
 * data analytics.
 *
 * <p>Please refer to <b><a href=
 * "https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/master/v2/googlecloud-to-elasticsearch/src/test/resources/EventMetadataBuilder/inputGCPAuditlogMessageEnriched.json">
 * inputGCPAuditlogMessageEnriched.json</a></b> to see an example of enriched message.
 */
public class EventMetadataBuilder implements Serializable {

  @JsonProperty("@timestamp")
  private String timestamp;

  @JsonProperty("agent")
  private Agent agent;

  @JsonProperty("data_stream")
  private DataStream dataStream;

  @JsonProperty("ecs")
  private Ecs ecs;

  @JsonProperty("message")
  private String message;

  @JsonProperty("service")
  private Service service;

  @JsonProperty("event")
  private Event event;

  @JsonIgnore private String inputMessage;
  @JsonIgnore private JsonNode enrichedMessage;
  @JsonIgnore final ObjectMapper objectMapper = new ObjectMapper();
  @JsonIgnore EventMetadata eventMetadata;

  private EventMetadataBuilder(
      String inputMessage, KafkaToElasticsearchOptions kafkaToElasticsearchOptions) {
    eventMetadata = new EventMetadata();

    try {
      eventMetadata.timestamp =
          ElasticsearchUtils.getTimestampFromOriginalPayload(objectMapper.readTree(inputMessage));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Cannot parse input message as JSON: " + inputMessage, e);
    } catch (NoSuchElementException e) {
      // if timestamp is not found, we generate it
      eventMetadata.timestamp =
          new java.sql.Timestamp(System.currentTimeMillis()).toInstant().toString();
    }

    this.inputMessage = inputMessage;

    eventMetadata.ecs = new Ecs();
    eventMetadata.message = inputMessage;
    eventMetadata.agent = new Agent();
    eventMetadata.agent.version = kafkaToElasticsearchOptions.getElasticsearchTemplateVersion();

    eventMetadata.dataStream = new DataStream();
    eventMetadata.dataStream.dataset = kafkaToElasticsearchOptions.getDataset().getKeyWithPrefix();
    eventMetadata.dataStream.namespace = kafkaToElasticsearchOptions.getNamespace();

    eventMetadata.service = new Service();
    eventMetadata.event = new Event();
    eventMetadata.service.type = kafkaToElasticsearchOptions.getDataset().getKeyWithPrefix();
    eventMetadata.event.dataset = kafkaToElasticsearchOptions.getDataset().getKeyWithPrefix();
  }

  public static EventMetadataBuilder build(
      String inputMessage, KafkaToElasticsearchOptions pubSubToElasticsearchOptions) {
    return new EventMetadataBuilder(inputMessage, pubSubToElasticsearchOptions);
  }

  private void enrich() {
    try {
      enrichedMessage = objectMapper.readTree(inputMessage);
      ((ObjectNode) enrichedMessage).putAll((ObjectNode) objectMapper.valueToTree(eventMetadata));
      ((ObjectNode) enrichedMessage).remove("timestamp");
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(
          "Exception occurred while processing input message: " + inputMessage, e);
    }
  }

  public String getEnrichedMessageAsString() {
    if (enrichedMessage == null) {
      this.enrich();
    }

    try {
      return objectMapper.writeValueAsString(enrichedMessage);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(
          "Exception occurred while building enriched message: " + enrichedMessage, e);
    }
  }

  public JsonNode getEnrichedMessageAsJsonNode() {
    if (enrichedMessage == null) {
      this.enrich();
    }

    return enrichedMessage;
  }

  @Override
  public String toString() {
    try {
      return objectMapper.writeValueAsString(enrichedMessage);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(
          "Exception occurred while writing EventMetadataBuilder as String.", e);
    }
  }

  static class EventMetadata {
    @JsonProperty("@timestamp")
    private String timestamp;

    @JsonProperty("agent")
    private Agent agent;

    @JsonProperty("data_stream")
    private DataStream dataStream;

    @JsonProperty("ecs")
    private Ecs ecs;

    @JsonProperty("message")
    private String message;

    @JsonProperty("service")
    private Service service;

    @JsonProperty("event")
    private Event event;

    @JsonIgnore private String inputMessage;
    @JsonIgnore private JsonNode enrichedMessage;
    @JsonIgnore final ObjectMapper objectMapper = new ObjectMapper();
  }

  private static class Agent {
    @JsonProperty("type")
    private final String type = "dataflow";

    @JsonProperty("name")
    private final String name = "";

    @JsonProperty("version")
    private String version;

    @JsonProperty("id")
    private final String id = "";
  }

  private static class DataStream {
    @JsonProperty("type")
    private final String type = "logs";

    @JsonProperty("dataset")
    private String dataset;

    @JsonProperty("namespace")
    private String namespace;
  }

  private static class Ecs {
    @JsonProperty("version")
    private final String version = "1.10.0";
  }

  private static class Service {
    @JsonProperty("type")
    private String type;
  }

  private static class Event {
    @JsonProperty("module")
    private final String module = "gcp";

    @JsonProperty("dataset")
    private String dataset;
  }
}
