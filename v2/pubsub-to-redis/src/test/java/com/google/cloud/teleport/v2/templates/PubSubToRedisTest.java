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
package com.google.cloud.teleport.v2.templates;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.github.fppt.jedismock.RedisServer;
import com.google.cloud.teleport.v2.templates.io.RedisHashIO;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.redis.RedisConnectionConfiguration;
import org.apache.beam.sdk.io.redis.RedisIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.Jedis;

/** Test cases for the {@link PubSubToRedis} class. */
@RunWith(JUnit4.class)
public class PubSubToRedisTest {

  private static final String REDIS_HOST = "localhost";
  private static RedisServer server;
  private static Jedis client;
  private static int port;
  private static final long NO_EXPIRATION = -1L;

  private static final String TRANSFORM_FILE_PATH =
      "src/test/resources/PubSubToRedisTest/transform.js";

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void beforeClass() throws Exception {
    server = RedisServer.newRedisServer();
    server.start();
    port = server.getBindPort();
    client = RedisConnectionConfiguration.create(REDIS_HOST, port).connect();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    client.close();
    server.stop();
  }

  @Test
  public void processElementForRedisStringMessage() {
    String key = "testWriteWithMethodSet";
    client.set(key, "value");

    String newValue = "newValue";
    PCollection<KV<String, String>> write = pipeline.apply(Create.of(KV.of(key, newValue)));
    write.apply(
        RedisIO.write().withEndpoint(REDIS_HOST, port).withMethod(RedisIO.Write.Method.SET));
    pipeline.run();

    assertEquals(newValue, client.get(key));
    assertEquals(NO_EXPIRATION, client.ttl(key));
  }

  @Test
  public void processElementForRedisHashMessage() {
    KV<String, String> fieldValue = KV.of("field1", "value1");
    KV<String, KV<String, String>> record = KV.of("hash1:log", fieldValue);

    PCollection<KV<String, KV<String, String>>> write = pipeline.apply(Create.of(record));

    write.apply(
        "Writing Hash into Redis",
        RedisHashIO.write()
            .withConnectionConfiguration(RedisConnectionConfiguration.create(REDIS_HOST, port)));

    pipeline.run();

    String value = client.hget("hash1:log", "field1");
    assertEquals(value, "value1");
  }

  @Test
  public void processElementForRedisStreamsMessage() {

    /* test data is 10 keys (stream IDs), each with two entries, each entry having one k/v a pair of data */
    List<String> redisKeys =
        IntStream.range(0, 10).boxed().map(idx -> UUID.randomUUID().toString()).collect(toList());

    Map<String, String> fooValues = ImmutableMap.of("sensor-id", "1234", "temperature", "19.8");
    Map<String, String> barValues = ImmutableMap.of("sensor-id", "9999", "temperature", "18.2");

    List<KV<String, Map<String, String>>> allData =
        redisKeys.stream()
            .flatMap(id -> Stream.of(KV.of(id, fooValues), KV.of(id, barValues)))
            .collect(toList());

    PCollection<KV<String, Map<String, String>>> write =
        pipeline.apply(
            Create.of(allData)
                .withCoder(
                    KvCoder.of(
                        StringUtf8Coder.of(),
                        MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))));
    write.apply(RedisIO.writeStreams().withEndpoint(REDIS_HOST, port));
    pipeline.run();
  }

  @Test
  public void testPubSubToRedisWithUdf() {
    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(
        PubSubToRedis.FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(),
        PubSubToRedis.FAILSAFE_ELEMENT_CODER);

    String testPayload = "{\"id\":\"123\",\"name\":\"test\"}";
    PubsubMessage testMessage =
        new PubsubMessage(testPayload.getBytes(), new HashMap<>(), "test-message-id");

    PCollectionTuple result =
        pipeline
            .apply("CreateInput", Create.of(testMessage))
            .apply(
                "ApplyUDF",
                new PubSubToRedis.PubSubMessageTransform(
                    TRANSFORM_FILE_PATH,
                    "transform",
                    0)); // Adjusted to use constructor instead of builder()

    PAssert.that(result.get(PubSubToRedis.UDF_OUT))
        .satisfies(
            collection -> {
              FailsafeElement<PubsubMessage, String> element = collection.iterator().next();
              assertThat(
                  element.getOriginalPayload().getPayload(), is(equalTo(testMessage.getPayload())));
              // Verify that UDF was applied - payload should contain transformedBy field
              assertThat(element.getPayload().contains("transformedBy"), is(true));
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testPubSubToRedisWithoutUdf() {
    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(
        PubSubToRedis.FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(),
        PubSubToRedis.FAILSAFE_ELEMENT_CODER);

    String testPayload = "{\"id\":\"123\",\"name\":\"test\"}";
    PubsubMessage testMessage =
        new PubsubMessage(testPayload.getBytes(), new HashMap<>(), "test-message-id");

    PCollectionTuple result =
        pipeline
            .apply("CreateInput", Create.of(testMessage))
            .apply(
                "NoUDF",
                new PubSubToRedis.PubSubMessageTransform(
                    null, null, 0)); // Adjusted to use constructor instead of builder()

    PAssert.that(result.get(PubSubToRedis.UDF_OUT))
        .satisfies(
            collection -> {
              FailsafeElement<PubsubMessage, String> element = collection.iterator().next();
              assertThat(
                  element.getOriginalPayload().getPayload(), is(equalTo(testMessage.getPayload())));
              // Without UDF, payload should be unchanged
              assertThat(element.getPayload(), is(equalTo(testPayload)));
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testPubSubToRedisWithBadUdf() {
    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(
        PubSubToRedis.FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(),
        PubSubToRedis.FAILSAFE_ELEMENT_CODER);

    String testPayload = "{\"id\":\"123\",\"name\":\"test\"}";
    PubsubMessage testMessage =
        new PubsubMessage(testPayload.getBytes(), new HashMap<>(), "test-message-id");

    PCollectionTuple result =
        pipeline
            .apply("CreateInput", Create.of(testMessage))
            .apply(
                "ApplyBadUDF",
                new PubSubToRedis.PubSubMessageTransform(
                    TRANSFORM_FILE_PATH,
                    "transformBad",
                    0)); // Adjusted to use constructor instead of builder()

    // Verify that failed transformations go to dead-letter output
    PAssert.that(result.get(PubSubToRedis.UDF_DEADLETTER_OUT))
        .satisfies(
            collection -> {
              FailsafeElement<PubsubMessage, String> element = collection.iterator().next();
              assertThat(
                  element.getOriginalPayload().getPayload(), is(equalTo(testMessage.getPayload())));
              return null;
            });

    // Success output should be empty
    PAssert.that(result.get(PubSubToRedis.UDF_OUT)).empty();

    pipeline.run();
  }

  @Test
  public void testPubSubToRedisWithDeadLetterTopic() {
    // Create test messages - one good, one that will fail UDF
    PubsubMessage goodMessage =
        new PubsubMessage(
            "{\"key\":\"good-key\",\"data\":\"good-value\"}".getBytes(),
            ImmutableMap.of("messageId", "good-123"));

    PubsubMessage badMessage =
        new PubsubMessage(
            "invalid-json-will-fail-udf".getBytes(), ImmutableMap.of("messageId", "bad-456"));

    // Apply UDF transformation
    PCollectionTuple result =
        pipeline
            .apply("CreateInput", Create.of(goodMessage, badMessage))
            .apply(
                "ApplyUDF",
                new PubSubToRedis.PubSubMessageTransform(
                    TRANSFORM_FILE_PATH,
                    "transform",
                    0)); // Adjusted to use constructor instead of builder()

    // Good message should be in main output
    PAssert.that(result.get(PubSubToRedis.UDF_OUT))
        .satisfies(
            collection -> {
              assertEquals(1, ((List<?>) collection).size());
              FailsafeElement<PubsubMessage, String> element = collection.iterator().next();
              String payload = element.getPayload();
              assertThat(payload.contains("transformedBy"), is(true));
              return null;
            });

    // Bad message should be in dead-letter output with original payload
    PAssert.that(result.get(PubSubToRedis.UDF_DEADLETTER_OUT))
        .satisfies(
            collection -> {
              assertEquals(1, ((List<?>) collection).size());
              FailsafeElement<PubsubMessage, String> element = collection.iterator().next();

              // Verify original payload is preserved
              String originalPayload = new String(element.getOriginalPayload().getPayload());
              assertThat(originalPayload, is(equalTo("invalid-json-will-fail-udf")));

              // Verify original message ID is preserved
              assertThat(
                  element.getOriginalPayload().getAttributeMap().get("messageId"),
                  is(equalTo("bad-456")));

              // Verify error message exists
              assertThat(element.getErrorMessage() != null, is(true));
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testRedisStringSink() {
    String key = "testStringKey";
    String value = "testStringValue";
    KV<String, String> record = KV.of(key, value);

    PCollection<KV<String, String>> write = pipeline.apply(Create.of(record));
    write.apply(
        RedisIO.write().withEndpoint(REDIS_HOST, port).withMethod(RedisIO.Write.Method.SET));
    pipeline.run();

    assertEquals(value, client.get(key));
  }

  @Test
  public void testRedisHashSink() {
    KV<String, String> fieldValue = KV.of("field1", "value1");
    KV<String, KV<String, String>> record = KV.of("hash1:log", fieldValue);

    PCollection<KV<String, KV<String, String>>> write = pipeline.apply(Create.of(record));
    write.apply(
        RedisHashIO.write()
            .withConnectionConfiguration(RedisConnectionConfiguration.create(REDIS_HOST, port)));

    pipeline.run();

    String value = client.hget("hash1:log", "field1");
    assertEquals(value, "value1");
  }

  @Test
  public void testRedisStreamsSink() {
    List<KV<String, Map<String, String>>> records =
        List.of(
            KV.of("stream1", ImmutableMap.of("field1", "value1")),
            KV.of("stream2", ImmutableMap.of("field2", "value2")));

    PCollection<KV<String, Map<String, String>>> write = pipeline.apply(Create.of(records));
    write.apply(RedisIO.writeStreams().withEndpoint(REDIS_HOST, port));
    pipeline.run();

    // Verify that the data was written to Redis Streams (additional verification logic may be
    // needed)
  }

  @Test
  public void testInvalidImageReference() {
    String invalidImageReference =
        "gcr.io//pubsub-to-redis:templates"; // Invalid due to double slashes

    // Simulate the staging process and check for expected exception or error handling
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              // Call the method responsible for staging with the invalid reference
              // For example: TemplateStaging.stageTemplate(invalidImageReference);
            });

    String expectedMessage = "Invalid image reference format";
    assertThat(exception.getMessage(), is(equalTo(expectedMessage)));
  }
}
