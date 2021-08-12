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
package com.google.cloud.teleport.v2.elasticsearch.transforms;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.teleport.v2.elasticsearch.options.PubSubToElasticsearchOptions;
import com.google.cloud.teleport.v2.elasticsearch.utils.Dataset;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.commons.lang.StringUtils;

/**
 * EventMetadata adds metadata required by Elasticsearch.
 */
public class EventMetadata implements Serializable {

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
    @JsonIgnore
    private String inputMessage;
    @JsonIgnore
    private JsonNode enrichedMessage;
    @JsonIgnore
    private PubSubToElasticsearchOptions pubSubToElasticsearchOptions;
    @JsonIgnore
    final ObjectMapper objectMapper = new ObjectMapper();

    private EventMetadata(String inputMessage, PubSubToElasticsearchOptions pubSubToElasticsearchOptions) {
        this.inputMessage = inputMessage;
        this.pubSubToElasticsearchOptions = pubSubToElasticsearchOptions;
        event = new Event();
        dataStream = new DataStream();
        ecs = new Ecs();
        service = new Service();
        message = inputMessage;

        JsonNode node = null;
        try {
            node = objectMapper.readTree(inputMessage);
            this.timestamp = findTimestampValue(node);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Error processing input message.", e);
        } catch (NoSuchElementException e) {
            this.timestamp = new java.sql.Timestamp(System.currentTimeMillis()).toInstant().toString();
        }

        agent = new Agent();
        agent.name = "";
        agent.version = StringUtils.isNotBlank(pubSubToElasticsearchOptions.getElasticsearchTemplateVersion()) ?
                pubSubToElasticsearchOptions.getElasticsearchTemplateVersion()
                : "1.0.0";
        agent.id = "";

        dataStream.dataset = pubSubToElasticsearchOptions.getDataset();
        dataStream.namespace = pubSubToElasticsearchOptions.getNamespace();

        service.type = event.dataset = pubSubToElasticsearchOptions.getDataset();
    }

    public List<JsonNode> asList() {
        JsonNode root = objectMapper.convertValue(this, JsonNode.class);

        List<JsonNode> nodes = new ArrayList<>();

        for (JsonNode node : root) {
            nodes.add(node);
        }

        return nodes;
    }

    private class Agent {
        @JsonProperty("type")
        private final String type = "dataflow";
        @JsonProperty("name")
        private String name;
        @JsonProperty("version")
        private String version;
        @JsonProperty("id")
        private String id;
    }

    private class DataStream {
        @JsonProperty("type")
        private final String type = "logs";
        @JsonProperty("dataset")
        private Dataset dataset;
        @JsonProperty("namespace")
        private String namespace;
    }

    private class Ecs {
        @JsonProperty("version")
        private final String version = "1.10.0";
    }

    private class Service {
        @JsonProperty("type")
        private Dataset type;
    }

    private class Event {
        @JsonProperty("module")
        private final String module = "gcp";
        @JsonProperty("dataset")
        private Dataset dataset;
    }

    public static EventMetadata build(String inputMessage, PubSubToElasticsearchOptions pubSubToElasticsearchOptions) {
        EventMetadata eventMetadata = new EventMetadata(inputMessage, pubSubToElasticsearchOptions);
        eventMetadata.enrich();
        return eventMetadata;
    }

    private void enrich() {
        try {
            enrichedMessage = objectMapper.readTree(inputMessage);
            ObjectNode metadata = (ObjectNode) objectMapper.readTree(
                    objectMapper.writerFor(EventMetadata.class).writeValueAsString(this));
            ((ObjectNode) enrichedMessage).putAll(metadata);
            ((ObjectNode) enrichedMessage).remove("timestamp");
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Error processing input message.", e);
        }
    }

    public String getEnrichedMessageAsString() {
        try {
            return objectMapper.writeValueAsString(enrichedMessage);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Error building enriched message.", e);
        }
    }

    public JsonNode getEnrichedMessageAsJsonNode() {
        return enrichedMessage;
    }

    private String findTimestampValue(JsonNode node) throws NoSuchElementException {
        if (node.has("timestamp")) {
            return node.get("timestamp").asText();
        }

        throw new NoSuchElementException();
    }

    @Override
    public String toString() {
        String node = null;
        try {
            node = objectMapper.writerFor(EventMetadata.class).writeValueAsString(this);
            return node;
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Error writing EventMetadata as String.", e);
        }
    }

}
