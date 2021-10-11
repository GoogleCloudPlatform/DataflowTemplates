package com.google.cloud.teleport.sentinel;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.http.ByteArrayContent;
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
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.net.ssl.HostnameVerifier;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link HttpEventPublisher} is a utility class that helps write {@link SentinelEvent}s to a Sentinel
 * http endpoint.
 */
@AutoValue
public abstract class HttpEventPublisher {
    
    private static final Logger LOG = LoggerFactory.getLogger(HttpEventPublisher.class);

    private static final int DEFAULT_MAX_CONNECTIONS = 1;
  
    private static final boolean DEFAULT_DISABLE_CERTIFICATE_VALIDATION = false;
  
    private static final Gson GSON =
        new GsonBuilder().setFieldNamingStrategy(f -> f.getName().toLowerCase()).create();
  
    private static final HttpMediaType MEDIA_TYPE =
        new HttpMediaType("application/json");
  
    private static final String CONTENT_TYPE =
        Joiner.on('/').join(MEDIA_TYPE.getType(), MEDIA_TYPE.getSubType());
  
    private static final String HTTPS_PROTOCOL_PREFIX = "https";
  
    public static Builder newBuilder() {
      return new AutoValue_HttpEventPublisher.Builder();
    }
  
    abstract ApacheHttpTransport transport();
  
    abstract HttpRequestFactory requestFactory();
  
    abstract GenericUrl genericUrl();
  
    abstract String token();

    abstract String customerId();

    abstract String logTableName();    
  
    @Nullable
    abstract Integer maxElapsedMillis();
  
    abstract Boolean disableCertificateValidation();
  
    /**
     * Executes a POST for the list of {@link SentinelEvent} objects into Sentinel's Http endpoint.
     *
     * @param events List of {@link SentinelEvent}s
     * @return {@link HttpResponse} for the POST.
     */
    public HttpResponse execute(List<SentinelEvent> events) throws IOException {
  
      HttpContent content = getContent(events);
      HttpRequest request = requestFactory().buildPostRequest(genericUrl(), content);
  
      HttpBackOffUnsuccessfulResponseHandler responseHandler =
          new HttpBackOffUnsuccessfulResponseHandler(getConfiguredBackOff());
      responseHandler.setBackOffRequired(BackOffRequired.ON_SERVER_ERROR);
      request.setUnsuccessfulResponseHandler(responseHandler);
  
      HttpIOExceptionHandler ioExceptionHandler =
          new HttpBackOffIOExceptionHandler(getConfiguredBackOff());
      request.setIOExceptionHandler(ioExceptionHandler);
  
      try {
        setHeaders(request, customerId(), logTableName(), token(), content.getLength());
      } catch (Exception e) {
        throw new IOException("Error setting headers: " + e.getMessage(), e);
      }
      
  
      return request.execute();
    }
  
    /**
     * Same as {@link HttpEventPublisher#execute(List)} but with a single {@link SentinelEvent}.
     *
     * @param event {@link SentinelEvent} object.
     */
    public HttpResponse execute(SentinelEvent event) throws IOException {
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
     * Utility method to set Authorization and other relevant http headers into the {@link
     * HttpRequest}.
     *
     * @param request {@link HttpRequest} object to add headers to.
     * @param customerId Sentinel's customer Id.     
     * @param logTableName Sentinel's logTableName.     
     * @param token Sentinel's authorization token.
     */
    private void setHeaders(HttpRequest request, String customerId, String logTableName, String token, long length) throws InvalidKeyException, NoSuchAlgorithmException {

      String dateString = SentinelUtils.getServerTime();

      String contentType = "application/json";
      String xmsDate = "x-ms-date:" + dateString;
      String resource = "/api/logs";
      String httpMethod = "POST";

      String stringToHash = String
        .join("\n", httpMethod, String.valueOf(length), contentType,
            xmsDate , resource);
      String hashedString = SentinelUtils.getHMAC256(stringToHash, token);
      String signature = "SharedKey " + customerId + ":" + hashedString;
      request.getHeaders().setAuthorization(signature);
      request.getHeaders().set("Log-Type", logTableName); 
      request.getHeaders().set("x-ms-date", dateString);
    }
  
    /**
     * Utility method to marshall a list of {@link SentinelEvent}s into an {@link HttpContent} object
     * that can be used to create an {@link HttpRequest}.
     *
     * @param events List of {@link SentinelEvent}s
     * @return {@link HttpContent} that can be used to create an {@link HttpRequest}.
     */
    @VisibleForTesting
    protected HttpContent getContent(List<SentinelEvent> events) {
      String payload = getStringPayload(events);
      LOG.debug("Payload content: {}", payload);
      return ByteArrayContent.fromString(CONTENT_TYPE, payload);
    }
  
    /** Utility method to get payload string from a list of {@link SentinelEvent}s. */
    @VisibleForTesting
    String getStringPayload(List<SentinelEvent> events) {
      return "[" + events.stream().map(event -> GSON.toJson(event)).collect(Collectors.joining(",")) +"]";
    }
  
    @AutoValue.Builder
    abstract static class Builder {
  
      abstract Builder setTransport(ApacheHttpTransport transport);
  
      abstract ApacheHttpTransport transport();
  
      abstract Builder setRequestFactory(HttpRequestFactory requestFactory);
  
      abstract Builder setGenericUrl(GenericUrl genericUrl);
  
      abstract GenericUrl genericUrl();
  
      abstract Builder setToken(String token);
  
      abstract String token();

      abstract Builder setCustomerId(String customerId);

      abstract String customerId();
  
      abstract Builder setLogTableName(String logTableName);

      abstract String logTableName();

      abstract Builder setDisableCertificateValidation(Boolean disableCertificateValidation);
  
      abstract Boolean disableCertificateValidation();
  
      abstract Builder setMaxElapsedMillis(Integer maxElapsedMillis);
  
      abstract Integer maxElapsedMillis();
  
      abstract HttpEventPublisher autoBuild();
  
      /**
       * Method to set the Sentinel Http URL.
       *
       * @param url Event URL
       * @return {@link Builder}
       */
      public Builder withUrl(String url) throws UnsupportedEncodingException {
        checkNotNull(url, "withUrl(url) called with null input.");
        return setGenericUrl(getGenericUrl(url));
      }
  
      /**
       * Method to set the Sentinel Http authentication token.
       *
       * @param token authentication token.
       * @return {@link Builder}
       */
      public Builder withToken(String token) {
        checkNotNull(token, "withToken(token) called with null input.");
        return setToken(token);
      }
  
      /**
       * Method to set the Sentinel customer id.
       *
       * @param customerId customer Id.
       * @return {@link Builder}
       */
      public Builder withCustomerId(String customerId) {
        checkNotNull(customerId, "withCustomerId(customerId) called with null input.");
        return setCustomerId(customerId);
      }

      /**
       * Method to set the Sentinel log table name.
       *
       * @param logTableName Sentinel log table name.
       * @return {@link Builder}
       */
      public Builder withLogTableName(String logTableName) {
        checkNotNull(logTableName, "withToken(logTableName) called with null input.");
        return setLogTableName(logTableName);
      }      

      /**
       * Method to disable SSL certificate validation. Defaults to {@value
       * DEFAULT_DISABLE_CERTIFICATE_VALIDATION}.
       *
       * @param disableCertificateValidation whether to disable SSL certificate validation.
       * @return {@link Builder}
       */
      public Builder withDisableCertificateValidation(Boolean disableCertificateValidation) {
        checkNotNull(
            disableCertificateValidation,
            "withDisableCertificateValidation(disableCertificateValidation) called with null input.");
        return setDisableCertificateValidation(disableCertificateValidation);
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
          throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
  
        checkNotNull(token(), "Authentication token needs to be specified via withToken(token).");
        checkNotNull(customerId(), "Customer Id needs to be specified via withCustomerId(token).");
        checkNotNull(logTableName(), "Log table name needs to be specified via withLogTableName(token).");
        checkNotNull(genericUrl(), "URL needs to be specified via withUrl(url).");
  
        if (disableCertificateValidation() == null) {
          LOG.info("Certificate validation disabled: {}", DEFAULT_DISABLE_CERTIFICATE_VALIDATION);
          setDisableCertificateValidation(DEFAULT_DISABLE_CERTIFICATE_VALIDATION);
        }
  
        if (maxElapsedMillis() == null) {
          LOG.info(
              "Defaulting max backoff time to: {} milliseconds ",
              ExponentialBackOff.DEFAULT_MAX_ELAPSED_TIME_MILLIS);
          setMaxElapsedMillis(ExponentialBackOff.DEFAULT_MAX_ELAPSED_TIME_MILLIS);
        }
  
        CloseableHttpClient httpClient =
            getHttpClient(DEFAULT_MAX_CONNECTIONS, disableCertificateValidation());
  
        setTransport(new ApacheHttpTransport(httpClient));
        setRequestFactory(transport().createRequestFactory());
  
        return autoBuild();
      }
  
      /**
       * Utility method to convert a baseUrl into a {@link GenericUrl}.
       *
       * @param baseUrl url pointing to the hec endpoint.
       * @return {@link GenericUrl}
       */
      private GenericUrl getGenericUrl(String baseUrl) {
  
        return new GenericUrl(baseUrl);
      }
  
      /**
       * Utility method to create a {@link CloseableHttpClient} to make http POSTs against Sentinel's
       * HEC.
       *
       * @param maxConnections max number of parallel connections.
       * @param disableCertificateValidation should disable certificate validation.
       */
      private CloseableHttpClient getHttpClient(
          int maxConnections, boolean disableCertificateValidation)
          throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
  
        HttpClientBuilder builder = ApacheHttpTransport.newDefaultHttpClientBuilder();
  
        if (genericUrl().getScheme().equalsIgnoreCase(HTTPS_PROTOCOL_PREFIX)) {
          LOG.info("SSL connection requested");
  
          HostnameVerifier hostnameVerifier =
              disableCertificateValidation
                  ? NoopHostnameVerifier.INSTANCE
                  : new DefaultHostnameVerifier();
  
          SSLContextBuilder sslContextBuilder = SSLContextBuilder.create();
          if (disableCertificateValidation) {
            LOG.info("Certificate validation is disabled");
            sslContextBuilder.loadTrustMaterial((TrustStrategy) (chain, authType) -> true);
          }
  
          SSLConnectionSocketFactory connectionSocketFactory =
              new SSLConnectionSocketFactory(sslContextBuilder.build(), hostnameVerifier);
          builder.setSSLSocketFactory(connectionSocketFactory);
        }
  
        builder.setMaxConnTotal(maxConnections);
        builder.setDefaultRequestConfig(
            RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build());
  
        return builder.build();
      }
    }
  }
  
