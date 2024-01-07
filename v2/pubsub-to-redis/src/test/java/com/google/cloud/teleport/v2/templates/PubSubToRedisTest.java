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
import static org.junit.Assert.assertEquals;

import com.github.fppt.jedismock.RedisServer;
import com.google.cloud.teleport.v2.templates.io.RedisHashIO;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.redis.RedisConnectionConfiguration;
import org.apache.beam.sdk.io.redis.RedisIO;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
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
}
