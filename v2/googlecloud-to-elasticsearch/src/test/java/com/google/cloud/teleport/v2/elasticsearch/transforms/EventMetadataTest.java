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
import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.teleport.v2.elasticsearch.options.PubSubToElasticsearchOptions;
import com.google.cloud.teleport.v2.elasticsearch.utils.Dataset;
import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test cases for {@link EventMetadata}.
 * Used to test the correctness of {@link EventMetadata} implementation,
 * including insertion of metadata and error handling.
 */
public class EventMetadataTest {

    private static final String RESOURCES_DIR = "EventMetadata/";
    private static final String INPUT_MESSAGE_FILE_PATH =
            Resources.getResource(RESOURCES_DIR + "inputMessage.json").getPath();
    private static final String INPUT_MESSAGE_INVALID_FILE_PATH =
            Resources.getResource(RESOURCES_DIR + "inputMessageInvalid.json").getPath();
    private static final boolean IS_WINDOWS = System.getProperty("os.name").contains("indow");

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void testEventMetadata() throws IOException {
        PubSubToElasticsearchOptions options =
                TestPipeline.testingPipelineOptions().as(PubSubToElasticsearchOptions.class);

        options.setDeadletterTable("test:dataset.table");
        options.setElasticsearchUsername("test");
        options.setElasticsearchPassword("test");
        options.setDataset(Dataset.AUDIT);
        options.setNamespace("test-namespace");
        options.setElasticsearchTemplateVersion("999.999.999");

        String inputMessage = Files.lines(
                Paths.get(
                        IS_WINDOWS
                                ? INPUT_MESSAGE_FILE_PATH.substring(1)
                                : INPUT_MESSAGE_FILE_PATH), StandardCharsets.UTF_8)
                .collect(Collectors.joining());

        EventMetadata eventMetadata = EventMetadata.build(inputMessage, options);

        JsonNode enrichedMessageAsJson = eventMetadata.getEnrichedMessageAsJsonNode();
        String enrichedMessageAsString = eventMetadata.getEnrichedMessageAsString();

        Assert.assertTrue(StringUtils.isNotBlank(enrichedMessageAsString));
        Assert.assertEquals(inputMessage, enrichedMessageAsJson.get("message").textValue());
        Assert.assertEquals("999.999.999", enrichedMessageAsJson.get("agent").get("version").textValue());
        Assert.assertEquals(Dataset.AUDIT.getKey(), enrichedMessageAsJson.get("data_stream").get("dataset").textValue());
        Assert.assertEquals("test-namespace", enrichedMessageAsJson.get("data_stream").get("namespace").textValue());
        Assert.assertEquals(Dataset.AUDIT.getKey(), enrichedMessageAsJson.get("service").get("type").textValue());
        Assert.assertEquals("2021-07-14T10:35:17.528142Z", enrichedMessageAsJson.get("@timestamp").textValue());
    }

    @Test
    public void testEventMetadataAppend() throws IOException {
        PubSubToElasticsearchOptions options =
                TestPipeline.testingPipelineOptions().as(PubSubToElasticsearchOptions.class);

        options.setDeadletterTable("test:dataset.table");
        options.setElasticsearchUsername("test");
        options.setElasticsearchPassword("test");
        options.setDataset(Dataset.AUDIT);
        options.setNamespace("test-namespace");

        String inputMessage = Files.lines(
                Paths.get(
                        IS_WINDOWS
                                ? INPUT_MESSAGE_FILE_PATH.substring(1)
                                : INPUT_MESSAGE_FILE_PATH), StandardCharsets.UTF_8)
                .collect(Collectors.joining());

        EventMetadata eventMetadata = EventMetadata.build(inputMessage, options);

        JsonNode enrichedMessageAsJson = eventMetadata.getEnrichedMessageAsJsonNode();

        //if elasticsearchTemplateVersion is not set, 1.0.0 is the default value
        Assert.assertEquals("1.0.0", enrichedMessageAsJson.get("agent").get("version").textValue());
        Assert.assertEquals(enrichedMessageAsJson.get("data_stream").get("dataset").textValue(), Dataset.AUDIT.getKey());
    }

    @Test
    public void testEventMetadataAppendFailed() throws IOException {
        exceptionRule.expect(IllegalStateException.class);

        PubSubToElasticsearchOptions options =
                TestPipeline.testingPipelineOptions().as(PubSubToElasticsearchOptions.class);

        options.setDeadletterTable("test:dataset.table");
        options.setElasticsearchUsername("test");
        options.setElasticsearchPassword("test");
        options.setDataset(Dataset.AUDIT);
        options.setNamespace("test-namespace");

        String inputMessageInvalid = Files.lines(
                Paths.get(
                        IS_WINDOWS
                                ? INPUT_MESSAGE_INVALID_FILE_PATH.substring(1)
                                : INPUT_MESSAGE_INVALID_FILE_PATH), StandardCharsets.UTF_8)
                .collect(Collectors.joining());

        EventMetadata eventMetadata = EventMetadata.build(inputMessageInvalid, options);

        JsonNode enrichedMessageAsJson = eventMetadata.getEnrichedMessageAsJsonNode();

        //if elasticsearchTemplateVersion is not set, 1.0.0 is the default value
        Assert.assertEquals("1.0.0", enrichedMessageAsJson.get("agent").get("version").textValue());
        Assert.assertEquals(enrichedMessageAsJson.get("data_stream").get("dataset").textValue(), Dataset.AUDIT.getKey());
    }


    @Test
    public void testEventMetadataAsList() throws IOException {
        PubSubToElasticsearchOptions options =
                TestPipeline.testingPipelineOptions().as(PubSubToElasticsearchOptions.class);

        options.setDeadletterTable("test:dataset.table");
        options.setElasticsearchUsername("test");
        options.setElasticsearchPassword("test");
        options.setDataset(Dataset.AUDIT);
        options.setNamespace("test-namespace");

        String inputMessage = Files.lines(
                Paths.get(
                        IS_WINDOWS
                                ? INPUT_MESSAGE_FILE_PATH.substring(1)
                                : INPUT_MESSAGE_FILE_PATH), StandardCharsets.UTF_8)
                .collect(Collectors.joining());

        EventMetadata eventMetadata = EventMetadata.build(inputMessage, options);

        Assert.assertTrue(eventMetadata.asList().size() > 0);
        Assert.assertTrue(eventMetadata.asList().stream().anyMatch(x -> x.has("@timestamp")));
    }
}
