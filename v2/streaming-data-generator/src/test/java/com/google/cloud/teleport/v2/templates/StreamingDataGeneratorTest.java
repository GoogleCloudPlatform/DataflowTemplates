/*
 * Copyright (C) 2020 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.v2.templates.StreamingDataGenerator.MessageGeneratorFn;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v20_0.com.google.common.io.ByteStreams;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.hamcrest.number.OrderingComparison.lessThan;

/**
 * Test cases for the {@link StreamingDataGenerator} class.
 */
@RunWith(JUnit4.class)
public class StreamingDataGeneratorTest {

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();
    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    /**
     * Tests the {@link MessageGeneratorFn} generates fake data.
     */
    @Test
    public void testMessageGenerator() throws IOException {
        // Arrange
        //
        String schema =
                "{"
                        + "\"id\": \"{{uuid()}}\", "
                        + "\"eventTime\": \"{{timestamp()}}\", "
                        + "\"username\": \"{{username()}}\", "
                        + "\"score\": {{integer(0,100)}}"
                        + "}";

        File file = tempFolder.newFile();
        writeToFile(file.getAbsolutePath(), schema);

        // Act
        //
        PCollection<PubsubMessage> results =
                pipeline
                        .apply("CreateInput", Create.of(0L))
                        .apply("GenerateMessage", ParDo.of(new MessageGeneratorFn(file.getAbsolutePath())));

        // Assert
        //
        PAssert.that(results)
                .satisfies(
                        input -> {
                            PubsubMessage message = input.iterator().next();

                            assertThat(message, is(notNullValue()));
                            assertThat(message.getPayload(), is(notNullValue()));
                            assertThat(message.getAttributeMap(), is(notNullValue()));

                            return null;
                        });

        pipeline.run();
    }

    /**
     * Tests the {@link MessageGeneratorFn} does not fail when given invalid schema.
     */
    @Test
    public void testMessageGeneratorInvalidSchema() throws IOException {
        // Arrange
        //
        String schema = "{\"name: \"Invalid\"";

        File file = tempFolder.newFile();
        writeToFile(file.getAbsolutePath(), schema);

        // Act
        //
        PCollection<PubsubMessage> results =
                pipeline
                        .apply("CreateInput", Create.of(0L))
                        .apply("GenerateMessage", ParDo.of(new MessageGeneratorFn(file.getAbsolutePath())));

        // Assert
        //
        PAssert.that(results).satisfies(input -> {
            PubsubMessage message = input.iterator().next();
            assertThat(message, is(notNullValue()));
            assertThat(new String(message.getPayload()), is(equalTo(schema)));
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testGenerateAttributes() throws IOException {

        String schema =
                "{"
                        + "\"id\": \"{{uuid()}}\", "
                        + "\"eventTime\": \"{{timestamp()}}\", "
                        + "\"username\": \"{{username()}}\", "
                        + "\"score\": {{integer(0,100)}}"
                        + "}";
        String attributeSchema = "{ \"attribute_test\": \"{{integer(5,100)}}\"}";


        File file = tempFolder.newFile();
        writeToFile(file.getAbsolutePath(), schema);

        File attributeFile = tempFolder.newFile();
        writeToFile(attributeFile.getAbsolutePath(), attributeSchema);

        // Act
        //
        PCollection<PubsubMessage> results =
                pipeline
                        .apply("CreateInput", Create.of(0L))
                        .apply("GenerateMessage", ParDo.of(new MessageGeneratorFn(file.getAbsolutePath(), attributeFile.getAbsolutePath())));

        // Assert
        //
        PAssert.that(results)
                .satisfies(
                        input -> {
                            PubsubMessage message = input.iterator().next();
                            assertThat(message, is(notNullValue()));
                            assertThat(message.getPayload(), is(notNullValue()));
                            assertThat(message.getAttributeMap(), is(notNullValue()));
                            assertThat(message.getAttribute("attribute_test"), is(notNullValue()));
                            assertThat(Integer.parseInt(message.getAttribute("attribute_test")), is(greaterThan(5)));
                            assertThat(Integer.parseInt(message.getAttribute("attribute_test")), is(lessThan(100)));
                            return null;
                        });

        pipeline.run();

    }

    /**
     * Helper to generate files for testing.
     *
     * @param filePath     The path to the file to write.
     * @param fileContents The content to write.
     * @return The file written.
     * @throws IOException If an error occurs while creating or writing the file.
     */
    private static ResourceId writeToFile(String filePath, String fileContents) throws IOException {

        ResourceId resourceId = FileSystems.matchNewResource(filePath, false);

        // Write the file contents to the channel and close.
        try (ReadableByteChannel readChannel =
                     Channels.newChannel(new ByteArrayInputStream(fileContents.getBytes()))) {
            try (WritableByteChannel writeChannel = FileSystems.create(resourceId, MimeTypes.TEXT)) {
                ByteStreams.copy(readChannel, writeChannel);
            }
        }

        return resourceId;
    }
}
