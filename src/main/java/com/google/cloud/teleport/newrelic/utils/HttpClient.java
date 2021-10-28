package com.google.cloud.teleport.newrelic.utils;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GZipEncoding;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.apache.v2.ApacheHttpTransport;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.StringUtils;
import com.google.cloud.teleport.newrelic.dtos.NewRelicLogRecord;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
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

import javax.net.ssl.HostnameVerifier;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link HttpClient} is a utility class that helps write {@link NewRelicLogRecord}s to the NewRelic Logs API endpoint.
 */
public class HttpClient {
    private static final Logger LOG = LoggerFactory.getLogger(HttpClient.class);
    private static final int DEFAULT_MAX_CONNECTIONS = 1;
    private static final Set<Integer> RETRYABLE_STATUS_CODES = ImmutableSet.of(408, 429, 500, 502, 503, 504, 599);
    private static final String HTTPS_PROTOCOL_PREFIX = "https";
    private static final Gson GSON = new GsonBuilder().create();
    private static final Integer MAX_ELAPSED_MILLIS = ExponentialBackOff.DEFAULT_MAX_ELAPSED_TIME_MILLIS;

    // If a POST request fails, the following response handler will determine whether the request should be reattempted
    // and will sleep for some time before retrying, by following an exponential backoff mechanism.
    private static final HttpBackOffUnsuccessfulResponseHandler RESPONSE_HANDLER;
    public static final GZipEncoding GZIP_ENCODING = new GZipEncoding();
    public static final String APPLICATION_GZIP = "application/gzip";
    public static final String APPLICATION_JSON = "application/json";
    public static final String API_KEY = "Api-Key";

    static {
        RESPONSE_HANDLER = new HttpBackOffUnsuccessfulResponseHandler(
                new ExponentialBackOff.Builder().setMaxElapsedTimeMillis(MAX_ELAPSED_MILLIS).build()
        );
        RESPONSE_HANDLER.setBackOffRequired((HttpResponse response) -> RETRYABLE_STATUS_CODES.contains(response.getStatusCode()));
    }

    private final GenericUrl logsApiUrl;
    private final String licenseKey;
    private final boolean useCompression;
    private final ApacheHttpTransport transport;
    private final HttpRequestFactory requestFactory;

    private HttpClient(final GenericUrl logsApiUrl,
                       final String licenseKey,
                       final Boolean useCompression,
                       final ApacheHttpTransport transport,
                       final HttpRequestFactory requestFactory) {
        this.logsApiUrl = logsApiUrl;
        this.licenseKey = licenseKey;
        this.useCompression = useCompression;
        this.transport = transport;
        this.requestFactory = requestFactory;
    }

    /**
     * Initializes a {@link HttpClient} object.
     *
     * @return {@link HttpClient}
     */
    public static HttpClient init(
            final GenericUrl logsApiUrl,
            final String licenseKey,
            final Boolean disableCertificateValidation,
            final Boolean useCompression) throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {

        checkNotNull(licenseKey, "The New Relic License Key needs to be specified.");
        checkNotNull(logsApiUrl, "The New Relic Logs URL needs to be specified.");

        LOG.info("Certificate validation disabled: {}", disableCertificateValidation);
        LOG.info("Defaulting max backoff time to: {} milliseconds ", MAX_ELAPSED_MILLIS);

        CloseableHttpClient httpClient = getHttpClient(
                logsApiUrl.getScheme().equalsIgnoreCase(HTTPS_PROTOCOL_PREFIX),
                DEFAULT_MAX_CONNECTIONS,
                disableCertificateValidation);

        final ApacheHttpTransport transport = new ApacheHttpTransport(httpClient);

        return new HttpClient(
                logsApiUrl,
                licenseKey,
                useCompression,
                transport,
                transport.createRequestFactory());
    }

    /**
     * Utility method to create a {@link CloseableHttpClient} to make http POST requests against the New Relic API.
     *
     * @param useSsl                       use SSL in the established connection
     * @param maxConnections               max number of parallel connections.
     * @param disableCertificateValidation should disable certificate validation (only relevant when using SSL).
     */
    private static CloseableHttpClient getHttpClient(final boolean useSsl, int maxConnections, boolean disableCertificateValidation)
            throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {

        final HttpClientBuilder builder = ApacheHttpTransport.newDefaultHttpClientBuilder();

        if (useSsl) {
            LOG.info("SSL connection requested");

            final HostnameVerifier hostnameVerifier = disableCertificateValidation ? NoopHostnameVerifier.INSTANCE : new DefaultHostnameVerifier();

            final SSLContextBuilder sslContextBuilder = SSLContextBuilder.create();
            if (disableCertificateValidation) {
                LOG.info("Certificate validation is disabled");
                sslContextBuilder.loadTrustMaterial((TrustStrategy) (chain, authType) -> true);
            }

            final SSLConnectionSocketFactory connectionSocketFactory = new SSLConnectionSocketFactory(sslContextBuilder.build(), hostnameVerifier);
            builder.setSSLSocketFactory(connectionSocketFactory);
        }

        builder.setMaxConnTotal(maxConnections);
        builder.setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build());

        return builder.build();
    }

    /**
     * Sends a list of {@link NewRelicLogRecord} objects to New Relic Logs
     *
     * @param logRecords List of {@link NewRelicLogRecord}s
     * @return {@link HttpResponse} Response for the performed HTTP POST .
     */
    public HttpResponse send(final List<NewRelicLogRecord> logRecords) throws IOException {
        final byte[] bodyBytes = StringUtils.getBytesUtf8(toJsonString(logRecords));
        final String contentType = useCompression ? APPLICATION_GZIP : APPLICATION_JSON;
        final HttpContent content = new ByteArrayContent(contentType, bodyBytes);

        final HttpRequest request = requestFactory.buildPostRequest(logsApiUrl, content);
        request.setUnsuccessfulResponseHandler(RESPONSE_HANDLER);
        request.getHeaders().set(API_KEY, licenseKey);
        if(useCompression) {
            request.setEncoding(GZIP_ENCODING);
        }
        return request.execute();
    }

    /**
     * Utility method to transform a list of {@link NewRelicLogRecord}s into a JSON array string.
     */
    private String toJsonString(List<NewRelicLogRecord> logRecords) {
        return GSON.toJsonTree(logRecords, new TypeToken<List<NewRelicLogRecord>>() {
        }.getType()).toString();
    }

    /**
     * Shuts down the HTTP client
     */
    public void close() throws IOException {
        if (transport != null) {
            LOG.info("Closing HTTP client transport.");
            transport.shutdown();
        }
    }
}