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
package com.google.cloud.teleport.datadog;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GZipEncoding;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler.BackOffRequired;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpIOExceptionHandler;
import com.google.api.client.http.HttpMediaType;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.apache.v2.ApacheHttpTransport;
import com.google.api.client.util.ExponentialBackOff;
import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import javax.annotation.Nullable;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link HttpEventPublisher} is a utility class that helps write {@link DatadogEvent}s to a Datadog
 * Logs API endpoint.
 */
@AutoValue
public abstract class HttpEventPublisher {

  private static final Logger LOG = LoggerFactory.getLogger(HttpEventPublisher.class);

  private static final int DEFAULT_MAX_CONNECTIONS = 1;

  private static final Gson GSON =
      new GsonBuilder().setFieldNamingStrategy(f -> f.getName().toLowerCase()).create();

  @VisibleForTesting
  protected static final String DD_URL_PATH = "api/v2/logs";

  private static final String DD_API_KEY_HEADER = "dd-api-key";

  private static final HttpMediaType MEDIA_TYPE =
      new HttpMediaType("application/json;charset=utf-8");

  private static final String CONTENT_TYPE =
      Joiner.on('/').join(MEDIA_TYPE.getType(), MEDIA_TYPE.getSubType());

  private static final String HTTPS_PROTOCOL_PREFIX = "https";

  public static Builder newBuilder() {
    return new AutoValue_HttpEventPublisher.Builder();
  }

  abstract ApacheHttpTransport transport();

  abstract HttpRequestFactory requestFactory();

  abstract GenericUrl genericUrl();

  abstract String apiKey();

  @Nullable
  abstract Integer maxElapsedMillis();

  /**
   * Executes a POST for the list of {@link DatadogEvent} objects into Datadog's Logs API.
   *
   * @param events List of {@link DatadogEvent}s
   * @return {@link HttpResponse} for the POST.
   */
  public HttpResponse execute(List<DatadogEvent> events) throws IOException {

    HttpContent content = getContent(events);
    HttpRequest request = requestFactory().buildPostRequest(genericUrl(), content);

    request.setEncoding(new GZipEncoding());

    HttpBackOffUnsuccessfulResponseHandler responseHandler =
        new HttpBackOffUnsuccessfulResponseHandler(getConfiguredBackOff());
    responseHandler.setBackOffRequired(BackOffRequired.ON_SERVER_ERROR);
    request.setUnsuccessfulResponseHandler(responseHandler);

    HttpIOExceptionHandler ioExceptionHandler =
        new HttpBackOffIOExceptionHandler(getConfiguredBackOff());
    request.setIOExceptionHandler(ioExceptionHandler);

    setHeaders(request, apiKey());

    return request.execute();
  }

  /**
   * Same as {@link HttpEventPublisher#execute(List)} but with a single {@link DatadogEvent}.
   *
   * @param event {@link DatadogEvent} object.
   */
  public HttpResponse execute(DatadogEvent event) throws IOException {
    return this.execute(ImmutableList.of(event));
  }

  /**
   * Return an {@link ExponentialBackOff} with the right settings.
   *
   * @return {@link ExponentialBackOff} object.
   */
  @VisibleForTesting
  protected ExponentialBackOff getConfiguredBackOff() {
    return new ExponentialBackOff.Builder().setMaxElapsedTimeMillis(maxElapsedMillis()).build();
  }

  /** Shutsdown connection manager and releases all resources. */
  public void close() throws IOException {
    if (transport() != null) {
      LOG.info("Closing publisher transport.");
      transport().shutdown();
    }
  }

  /**
   * Utility method to set http headers into the {@link HttpRequest}.
   *
   * @param request {@link HttpRequest} object to add headers to.
   * @param apiKey Datadog's Logs API key.
   */
  private void setHeaders(HttpRequest request, String apiKey) {
    request.getHeaders().set(DD_API_KEY_HEADER, apiKey);
    request.getHeaders().setContentEncoding("gzip");
  }

  /**
   * Utility method to marshall a list of {@link DatadogEvent}s into an {@link HttpContent} object
   * that can be used to create an {@link HttpRequest}.
   *
   * @param events List of {@link DatadogEvent}s
   * @return {@link HttpContent} that can be used to create an {@link HttpRequest}.
   */
  @VisibleForTesting
  protected HttpContent getContent(List<DatadogEvent> events) {
    String payload = getStringPayload(events);
    LOG.debug("Payload content: {}", payload);
    return ByteArrayContent.fromString(CONTENT_TYPE, payload);
  }

  /** Utility method to get payload string from a list of {@link DatadogEvent}s. */
  @VisibleForTesting
  String getStringPayload(List<DatadogEvent> events) {
    return GSON.toJson(events);
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setTransport(ApacheHttpTransport transport);

    abstract ApacheHttpTransport transport();

    abstract Builder setRequestFactory(HttpRequestFactory requestFactory);

    abstract Builder setGenericUrl(GenericUrl genericUrl);

    abstract GenericUrl genericUrl();

    abstract Builder setApiKey(String apiKey);

    abstract String apiKey();

    abstract Builder setMaxElapsedMillis(Integer maxElapsedMillis);

    abstract Integer maxElapsedMillis();

    abstract HttpEventPublisher autoBuild();

    /**
     * Method to set the Datadog Logs API URL.
     *
     * @param url Logs API URL
     * @return {@link Builder}
     */
    public Builder withUrl(String url) throws UnsupportedEncodingException {
      checkNotNull(url, "withUrl(url) called with null input.");
      return setGenericUrl(getGenericUrl(url));
    }

    /**
     * Method to set the Datadog Logs API key.
     *
     * @param apiKey Logs API key.
     * @return {@link Builder}
     */
    public Builder withApiKey(String apiKey) {
      checkNotNull(apiKey, "withApiKey(apiKey) called with null input.");
      return setApiKey(apiKey);
    }

    /**
     * Method to max timeout for {@link ExponentialBackOff}. Otherwise uses the default setting for
     * {@link ExponentialBackOff}.
     *
     * @param maxElapsedMillis max elapsed time in milliseconds for timeout.
     * @return {@link Builder}
     */
    public Builder withMaxElapsedMillis(Integer maxElapsedMillis) {
      checkNotNull(
          maxElapsedMillis, "withMaxElapsedMillis(maxElapsedMillis) called with null input.");
      return setMaxElapsedMillis(maxElapsedMillis);
    }

    /**
     * Validates and builds a {@link HttpEventPublisher} object.
     *
     * @return {@link HttpEventPublisher}
     */
    public HttpEventPublisher build()
        throws NoSuchAlgorithmException,
            KeyManagementException {

      checkNotNull(apiKey(), "API Key needs to be specified via withApiKey(apiKey).");
      checkNotNull(genericUrl(), "URL needs to be specified via withUrl(url).");

      if (maxElapsedMillis() == null) {
        LOG.info(
            "Defaulting max backoff time to: {} milliseconds ",
            ExponentialBackOff.DEFAULT_MAX_ELAPSED_TIME_MILLIS);
        setMaxElapsedMillis(ExponentialBackOff.DEFAULT_MAX_ELAPSED_TIME_MILLIS);
      }

      CloseableHttpClient httpClient =
          getHttpClient(
              DEFAULT_MAX_CONNECTIONS);

      setTransport(new ApacheHttpTransport(httpClient));
      setRequestFactory(transport().createRequestFactory());

      return autoBuild();
    }

    /**
     * Utility method to convert a baseUrl into a {@link GenericUrl}.
     *
     * @param baseUrl url pointing to the Logs API endpoint.
     * @return {@link GenericUrl}
     */
    private GenericUrl getGenericUrl(String baseUrl) {
      String url = Joiner.on('/').join(baseUrl, DD_URL_PATH);

      return new GenericUrl(url);
    }

    /**
     * Utility method to create a {@link CloseableHttpClient} to make http POSTs against Datadog's
     * Logs API.
     *
     */
    private CloseableHttpClient getHttpClient(
        int maxConnections)
        throws NoSuchAlgorithmException,
            KeyManagementException {

      HttpClientBuilder builder = ApacheHttpTransport.newDefaultHttpClientBuilder();

      if (genericUrl().getScheme().equalsIgnoreCase(HTTPS_PROTOCOL_PREFIX)) {
        LOG.info("SSL connection requested");

        HostnameVerifier hostnameVerifier = new DefaultHostnameVerifier();

        SSLContext sslContext = SSLContextBuilder.create().build();

        SSLConnectionSocketFactory connectionSocketFactory =
            new SSLConnectionSocketFactory(sslContext, hostnameVerifier);
        builder.setSSLSocketFactory(connectionSocketFactory);
      }

      builder.setMaxConnTotal(maxConnections);
      builder.setDefaultRequestConfig(
          RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build());

      return builder.build();
    }
  }
}
