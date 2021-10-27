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
package com.google.cloud.teleport.splunk;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.util.ExponentialBackOff;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.List;
import javax.net.ssl.SSLHandshakeException;
import org.junit.Before;
import org.junit.Test;
import org.mockserver.configuration.ConfigurationProperties;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.verify.VerificationTimes;

/** Unit tests for {@link com.google.cloud.teleport.splunk.HttpEventPublisher} class. */
public class HttpEventPublisherTest {

  private static final SplunkEvent SPLUNK_TEST_EVENT_1 =
      SplunkEvent.newBuilder()
          .withEvent("test-event-1")
          .withHost("test-host-1")
          .withIndex("test-index-1")
          .withSource("test-source-1")
          .withSourceType("test-source-type-1")
          .withTime(12345L)
          .build();

  private static final SplunkEvent SPLUNK_TEST_EVENT_2 =
      SplunkEvent.newBuilder()
          .withEvent("test-event-2")
          .withHost("test-host-2")
          .withIndex("test-index-2")
          .withSource("test-source-2")
          .withSourceType("test-source-type-2")
          .withTime(12345L)
          .build();

  private static final List<SplunkEvent> SPLUNK_EVENTS =
      ImmutableList.of(SPLUNK_TEST_EVENT_1, SPLUNK_TEST_EVENT_2);
  private static final String RECOGNIZED_CERTIFICATE_PATH =
      "certificates/RecognizedSelfSignedCertificate.crt";
  private static final String UNRECOGNIZED_CERTIFICATE_PATH =
      "certificates/UnrecognizedSelfSignedCertificate.crt";
  private static final String KEY_PATH =
      Resources.getResource("certificates/privateKey.key").getPath();
  private static final String EXPECTED_PATH = "/" + HttpEventPublisher.HEC_URL_PATH;
  private ClientAndServer mockServer;

  @Before
  public void setUp() throws IOException {
    ConfigurationProperties.disableSystemOut(true);
    ConfigurationProperties.privateKeyPath(KEY_PATH);
    ConfigurationProperties.x509CertificatePath(RECOGNIZED_CERTIFICATE_PATH);
    ServerSocket socket = new ServerSocket(0);
    int port = socket.getLocalPort();
    socket.close();
    mockServer = startClientAndServer("localhost", port);
  }

  /** Test whether payload is stringified as expected. */
  @Test
  public void stringPayloadTest()
      throws UnsupportedEncodingException, NoSuchAlgorithmException, KeyStoreException,
          KeyManagementException, CertificateException {

    HttpEventPublisher publisher =
        HttpEventPublisher.newBuilder()
            .withUrl("http://example.com")
            .withToken("test-token")
            .withDisableCertificateValidation(false)
            .build();

    String actual = publisher.getStringPayload(SPLUNK_EVENTS);

    String expected =
        "{\"time\":12345,\"host\":\"test-host-1\",\"source\":\"test-source-1\","
            + "\"sourcetype\":\"test-source-type-1\",\"index\":\"test-index-1\","
            + "\"event\":\"test-event-1\"}{\"time\":12345,\"host\":\"test-host-2\","
            + "\"source\":\"test-source-2\",\"sourcetype\":\"test-source-type-2\","
            + "\"index\":\"test-index-2\",\"event\":\"test-event-2\"}";

    assertThat(expected, is(equalTo(actual)));
  }

  /** Test whether {@link HttpContent} is created from the list of {@link SplunkEvent}s. */
  @Test
  public void contentTest() throws NoSuchAlgorithmException,
      KeyStoreException, KeyManagementException, IOException, CertificateException {

    HttpEventPublisher publisher =
        HttpEventPublisher.newBuilder()
            .withUrl("http://example.com")
            .withToken("test-token")
            .withDisableCertificateValidation(false)
            .build();

    String expectedString =
        "{\"time\":12345,\"host\":\"test-host-1\",\"source\":\"test-source-1\","
            + "\"sourcetype\":\"test-source-type-1\",\"index\":\"test-index-1\","
            + "\"event\":\"test-event-1\"}{\"time\":12345,\"host\":\"test-host-2\","
            + "\"source\":\"test-source-2\",\"sourcetype\":\"test-source-type-2\","
            + "\"index\":\"test-index-2\",\"event\":\"test-event-2\"}";

    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      HttpContent actualContent = publisher.getContent(SPLUNK_EVENTS);
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
            .withDisableCertificateValidation(false);

    assertThat(
        builder.genericUrl(),
        is(equalTo(new GenericUrl(Joiner.on('/').join(baseURL, "services/collector/event")))));
  }

  @Test
  public void configureBackOffDefaultTest() throws NoSuchAlgorithmException,
      KeyStoreException, KeyManagementException, IOException, CertificateException {

    HttpEventPublisher publisherDefaultBackOff =
        HttpEventPublisher.newBuilder()
            .withUrl("http://example.com")
            .withToken("test-token")
            .withDisableCertificateValidation(false)
            .build();

    assertThat(
        publisherDefaultBackOff.getConfiguredBackOff().getMaxElapsedTimeMillis(),
        is(equalTo(ExponentialBackOff.DEFAULT_MAX_ELAPSED_TIME_MILLIS)));
  }

  @Test
  public void configureBackOffCustomTest() throws NoSuchAlgorithmException,
      KeyStoreException, KeyManagementException, IOException, CertificateException {

    int timeoutInMillis = 600000; // 10 minutes
    HttpEventPublisher publisherWithBackOff =
        HttpEventPublisher.newBuilder()
            .withUrl("http://example.com")
            .withToken("test-token")
            .withDisableCertificateValidation(false)
            .withMaxElapsedMillis(timeoutInMillis)
            .build();

    assertThat(
        publisherWithBackOff.getConfiguredBackOff().getMaxElapsedTimeMillis(),
        is(equalTo(timeoutInMillis)));
  }

  @Test(expected = CertificateException.class)
  public void invalidSelfSignedCertificateTest() throws Exception {
    HttpEventPublisher publisherWithInvalidCert =
        HttpEventPublisher.newBuilder()
            .withUrl("https://example.com")
            .withToken("test-token")
            .withDisableCertificateValidation(false)
            .withSelfSignedCertificate("invalid_cert".getBytes())
            .build();
  }

  @Test
  public void recognizedSelfSignedCertificateTest() throws Exception {
    mockServerListening(200);
    byte[] recognizedCert =
        Resources.toString(
                Resources.getResource(RECOGNIZED_CERTIFICATE_PATH), StandardCharsets.UTF_8)
            .getBytes();
    HttpEventPublisher publisher =
        HttpEventPublisher.newBuilder()
            .withUrl("https://localhost:" + String.valueOf(mockServer.getPort()))
            .withToken("test-token")
            .withDisableCertificateValidation(false)
            .withSelfSignedCertificate(recognizedCert)
            .build();
    publisher.execute(SPLUNK_EVENTS);

    // Server received exactly one POST request.
    mockServer.verify(HttpRequest.request(EXPECTED_PATH), VerificationTimes.once());
  }

  @Test(expected = SSLHandshakeException.class)
  public void unrecognizedSelfSignedCertificateTest() throws Exception {
    mockServerListening(200);
    byte[] unrecognizedCert =
        Resources.toString(
                Resources.getResource(UNRECOGNIZED_CERTIFICATE_PATH), StandardCharsets.UTF_8)
            .getBytes();
    HttpEventPublisher publisher =
        HttpEventPublisher.newBuilder()
            .withUrl("https://localhost:" + String.valueOf(mockServer.getPort()))
            .withToken("test-token")
            .withDisableCertificateValidation(false)
            .withSelfSignedCertificate(unrecognizedCert)
            .build();
    publisher.execute(SPLUNK_EVENTS);
  }

  private byte[] readFile(String path) throws FileNotFoundException, IOException {
    BufferedReader br = new BufferedReader(new FileReader(Resources.getResource(path).getFile()));
    StringBuilder sb = new StringBuilder();
    String line = br.readLine();
    while (line != null) {
      sb.append(line);
      sb.append(System.lineSeparator());
      line = br.readLine();
    }
    return sb.toString().getBytes();
  }

  private void mockServerListening(int statusCode) {
    mockServer
        .when(HttpRequest.request(EXPECTED_PATH))
        .respond(HttpResponse.response().withStatusCode(statusCode));
  }
}
