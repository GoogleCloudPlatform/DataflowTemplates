/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.teleport.sentinel;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.util.ExponentialBackOff;
import com.google.common.collect.ImmutableList;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import org.junit.Test;

/** Unit tests for {@link com.google.cloud.teleport.sentinel.HttpEventPublisher} class. */
public class HttpEventPublisherTest {

  private static final SentinelEvent SENTINEL_TEST_EVENT_1 =
      SentinelEvent.newBuilder()
          .withEvent("test-event-1")
          .withHost("test-host-1")
          .withIndex("test-index-1")
          .withSource("test-source-1")
          .withSourceType("test-source-type-1")
          .withTime(12345L)
          .build();

  private static final SentinelEvent SENTINEL_TEST_EVENT_2 =
      SentinelEvent.newBuilder()
          .withEvent("test-event-2")
          .withHost("test-host-2")
          .withIndex("test-index-2")
          .withSource("test-source-2")
          .withSourceType("test-source-type-2")
          .withTime(12345L)
          .build();

  private static final List<SentinelEvent> SENTINEL_EVENTS =
      ImmutableList.of(SENTINEL_TEST_EVENT_1, SENTINEL_TEST_EVENT_2);

  /** Test whether payload is stringified as expected. */
  @Test
  public void stringPayloadTest()
      throws UnsupportedEncodingException, NoSuchAlgorithmException, KeyStoreException,
          KeyManagementException {

    HttpEventPublisher publisher =
        HttpEventPublisher.newBuilder()
            .withUrl("http://example.com")
            .withToken("test-token")
            .withCustomerId("test-customer-id")
            .withLogTableName("test-logTableName")
            .withDisableCertificateValidation(false)
            .build();

    String actual = publisher.getStringPayload(SENTINEL_EVENTS);

    String expected =
        "[{\"time\":12345,\"host\":\"test-host-1\",\"source\":\"test-source-1\","
            + "\"sourcetype\":\"test-source-type-1\",\"index\":\"test-index-1\","
            + "\"event\":\"test-event-1\"},{\"time\":12345,\"host\":\"test-host-2\","
            + "\"source\":\"test-source-2\",\"sourcetype\":\"test-source-type-2\","
            + "\"index\":\"test-index-2\",\"event\":\"test-event-2\"}]";

    assertThat(expected, is(equalTo(actual)));
  }

  /** Test whether {@link HttpContent} is created from the list of {@link SENTINELEvent}s. */
  @Test
  public void contentTest()
      throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException, IOException {

    HttpEventPublisher publisher =
        HttpEventPublisher.newBuilder()
            .withUrl("http://example.com")
            .withToken("test-token")
            .withCustomerId("test-customer-id")
            .withLogTableName("test-logTableName")
            .withDisableCertificateValidation(false)
            .build();

    String expectedString =
        "[{\"time\":12345,\"host\":\"test-host-1\",\"source\":\"test-source-1\","
        + "\"sourcetype\":\"test-source-type-1\",\"index\":\"test-index-1\","
        + "\"event\":\"test-event-1\"},{\"time\":12345,\"host\":\"test-host-2\","
        + "\"source\":\"test-source-2\",\"sourcetype\":\"test-source-type-2\","
        + "\"index\":\"test-index-2\",\"event\":\"test-event-2\"}]";

    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      HttpContent actualContent = publisher.getContent(SENTINEL_EVENTS);
      actualContent.writeTo(bos);
      String actualString = new String(bos.toByteArray(), StandardCharsets.UTF_8);
      assertThat(actualString, is(equalTo(expectedString)));
    }
  }

  @Test
  public void genericURLTest()
      throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException, IOException {

    String baseURL = "http://example.com";
    HttpEventPublisher.Builder builder =
        HttpEventPublisher.newBuilder()
            .withUrl(baseURL)
            .withToken("test-token")
            .withCustomerId("test-customer-id")
            .withLogTableName("test-logTableName")
            .withDisableCertificateValidation(false);

    assertThat(builder.genericUrl(),is(equalTo(new GenericUrl(baseURL))));
  }

  @Test
  public void configureBackOffDefaultTest()
      throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException, IOException {

    HttpEventPublisher publisherDefaultBackOff =
        HttpEventPublisher.newBuilder()
            .withUrl("http://example.com")
            .withToken("test-token")
            .withCustomerId("test-customer-id")
            .withLogTableName("test-logTableName")
            .withDisableCertificateValidation(false)
            .build();

    assertThat(
        publisherDefaultBackOff.getConfiguredBackOff().getMaxElapsedTimeMillis(),
        is(equalTo(ExponentialBackOff.DEFAULT_MAX_ELAPSED_TIME_MILLIS)));
  }

  @Test
  public void configureBackOffCustomTest()
      throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException, IOException {

    int timeoutInMillis = 600000; // 10 minutes
    HttpEventPublisher publisherWithBackOff =
        HttpEventPublisher.newBuilder()
            .withUrl("http://example.com")
            .withToken("test-token")
            .withCustomerId("test-customer-id")
            .withLogTableName("test-logTableName")
            .withDisableCertificateValidation(false)
            .withMaxElapsedMillis(timeoutInMillis)
            .build();

    assertThat(
        publisherWithBackOff.getConfiguredBackOff().getMaxElapsedTimeMillis(),
        is(equalTo(timeoutInMillis)));
  }
}
