package com.google.cloud.teleport.newrelic.config;

import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.junit.Test;

import static com.google.cloud.teleport.newrelic.config.NewRelicConfig.*;
import static com.google.common.truth.Truth.assertThat;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NewRelicConfigTest {

    private static final String SOME_LOGS_API_URL = "https://force-logs.grogu.sw";
    private static final String SOME_LICENSE_KEY = "a-license-key";
    private static final int SOME_BATCH_COUNT = 1;
    private static final boolean SOME_DISABLE_CERTIFICATE_VALIDATION = true;
    private static final boolean SOME_USE_COMPRESSION = false;
    private static final Integer SOME_PARALLELISM = 13;

    @Test
    public void shouldParseConfigurationCorrectly() {
        // Given
        final NewRelicPipelineOptions pipelineOptions = getPipelineOptions(SOME_BATCH_COUNT, SOME_PARALLELISM, SOME_USE_COMPRESSION, SOME_DISABLE_CERTIFICATE_VALIDATION);

        // When
        final NewRelicConfig newRelicConfig = NewRelicConfig.fromPipelineOptions(pipelineOptions);

        // Then
        assertThat(newRelicConfig.getLogsApiUrl().get()).isEqualTo(SOME_LOGS_API_URL);
        assertThat(newRelicConfig.getLicenseKey().get()).isEqualTo(SOME_LICENSE_KEY);
        assertThat(newRelicConfig.getBatchCount().get()).isEqualTo(SOME_BATCH_COUNT);
        assertThat(newRelicConfig.getDisableCertificateValidation().get()).isEqualTo(SOME_DISABLE_CERTIFICATE_VALIDATION);
        assertThat(newRelicConfig.getUseCompression().get()).isEqualTo(SOME_USE_COMPRESSION);
        assertThat(newRelicConfig.getParallelism().get()).isEqualTo(SOME_PARALLELISM);
    }

    @Test
    public void shouldUseCorrectDefaultValues() {
        // Given
        final NewRelicPipelineOptions pipelineOptions = getPipelineOptions(null, null, null, null);

        // When
        final NewRelicConfig newRelicConfig = NewRelicConfig.fromPipelineOptions(pipelineOptions);

        // Then
        assertThat(newRelicConfig.getLogsApiUrl().get()).isEqualTo(SOME_LOGS_API_URL);
        assertThat(newRelicConfig.getLicenseKey().get()).isEqualTo(SOME_LICENSE_KEY);
        assertThat(newRelicConfig.getBatchCount().get()).isEqualTo(DEFAULT_BATCH_COUNT);
        assertThat(newRelicConfig.getDisableCertificateValidation().get()).isEqualTo(DEFAULT_DISABLE_CERTIFICATE_VALIDATION);
        assertThat(newRelicConfig.getUseCompression().get()).isEqualTo(DEFAULT_USE_COMPRESSION);
        assertThat(newRelicConfig.getParallelism().get()).isEqualTo(DEFAULT_PARALLELISM);
    }

    private static NewRelicPipelineOptions getPipelineOptions(final Integer batchCount, final Integer parallelism, final Boolean useCompression, final Boolean disableCertificateValidation) {
        final NewRelicPipelineOptions pipelineOptions = mock(NewRelicPipelineOptions.class);

        when(pipelineOptions.getLogsApiUrl()).thenReturn(StaticValueProvider.of(SOME_LOGS_API_URL));
        when(pipelineOptions.getLicenseKey()).thenReturn(StaticValueProvider.of(SOME_LICENSE_KEY));
        when(pipelineOptions.getTokenKMSEncryptionKey()).thenReturn(StaticValueProvider.of(null));
        when(pipelineOptions.getBatchCount()).thenReturn(StaticValueProvider.of(batchCount));
        when(pipelineOptions.getParallelism()).thenReturn(StaticValueProvider.of(parallelism));
        when(pipelineOptions.getUseCompression()).thenReturn(StaticValueProvider.of(useCompression));
        when(pipelineOptions.getDisableCertificateValidation()).thenReturn(StaticValueProvider.of(disableCertificateValidation));

        return pipelineOptions;
    }
}