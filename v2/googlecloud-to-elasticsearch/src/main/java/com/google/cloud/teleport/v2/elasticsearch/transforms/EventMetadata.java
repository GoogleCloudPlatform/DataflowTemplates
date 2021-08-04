package com.google.cloud.teleport.v2.elasticsearch.transforms;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.elasticsearch.options.PubSubToElasticsearchOptions;

import java.util.NoSuchElementException;

public class EventMetadata {
    @JsonProperty("@timestamp")
    private String timestamp;
    @JsonProperty("agent")
    private Agent agent;
    @JsonProperty("dataStream")
    private DataStream dataStream;
    @JsonProperty("ecs")
    private Ecs ecs;
    @JsonProperty("message")
    private String message;
    @JsonProperty("service")
    private Service service;
    @JsonIgnore
    private String inputMessage;
    @JsonIgnore
    private PubSubToElasticsearchOptions pubSubToElasticsearchOptions;

    private EventMetadata(String inputMessage, PubSubToElasticsearchOptions pubSubToElasticsearchOptions) {
        inputMessage = inputMessage;
        pubSubToElasticsearchOptions = pubSubToElasticsearchOptions;
        agent = new Agent();
        dataStream = new DataStream();
        ecs = new Ecs();
        service = new Service();
    }

    private class Agent {
        @JsonProperty("type")
        private static final String type = "dataflow";
        @JsonProperty("name")
        private String name;
        @JsonProperty("version")
        private String version;
        @JsonProperty("id")
        private String id;
    }

    private class DataStream {
        @JsonProperty("type")
        private static final String type = "logs";
        @JsonProperty("dataset")
        private String dataset;
        @JsonProperty("namespace")
        private String namespace;
    }

    private class Ecs {
        @JsonProperty("version")
        private static final String version = "1.10.0";
    }

    private class Service {
        @JsonProperty("type")
        private String type;
    }

    private class Event {
        @JsonProperty("module")
        private static final String module = "gcp";
        @JsonProperty("dataset")
        private String dataset;
    }

    public static EventMetadata build() {
        EventMetadata eventMetadata = new EventMetadata();

        return eventMetadata;
    }

    private void setOrGenerateTimestamp() {

        final ObjectMapper objectMapper = new ObjectMapper();

        JsonNode node = null;
        try {
            node = objectMapper.readTree(inputMessage);
            this.timestamp = findTimestampValue(node);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Error processing input message.", e);
        } catch (NoSuchElementException e) {
            this.timestamp = new java.sql.Timestamp(System.currentTimeMillis()).toInstant().toString();
        }
    }

    private String findTimestampValue(JsonNode node) throws NoSuchElementException {
        if (node.has("timestamp")) {
            return node.get("timestamp").asText();
        }

        throw new NoSuchElementException();
    }

    private void setAgent() {
        agent.name = "";
        agent.version = "1.0.0";
        agent.id = "";
    }

    private void setDataStream() {
        dataStream.dataset = pubSubToElasticsearchOptions.getWriteDataset();
    }

    public static class Builder {

        private static EventMetadata eventMetadata = new EventMetadata();

        private String inputMessage;

        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
        private static final String TIMESTAMP_FIELD_NAME = "@timestamp";

        public Builder(String inputMessage) {
            this.inputMessage = inputMessage;
        }

        public static EventMetadata build() {
            setOrGenerateTimestamp();
            return eventMetadata;
        }

        private Builder setOrGenerateTimestamp() {
            JsonNode node = null;
            try {
                node = OBJECT_MAPPER.readTree(inputMessage);
            } catch (JsonProcessingException e) {
                throw new IllegalStateException("Error processing input message.", e);
            }

            String foundTimestamp = findTimestampValue(node);
            if (foundTimestamp != null) {
                eventMetadata.timestamp = foundTimestamp;
            } else {
                eventMetadata.timestamp = new java.sql.Timestamp(System.currentTimeMillis()).toInstant().toString();
            }

            return this;
        }

        private Builder setAgent() {

            return this;
        }

        private String findTimestampValue(JsonNode node) {
            if (node.has("timestamp")) {
                return node.get("timestamp").asText();
            }

            return null;
        }

        private Builder setAgent() {

            return this;
        }

    }

}
