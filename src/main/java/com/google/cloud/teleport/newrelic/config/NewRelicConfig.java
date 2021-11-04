package com.google.cloud.teleport.newrelic.config;

import com.google.cloud.teleport.util.KMSEncryptedNestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider;

import static com.google.cloud.teleport.newrelic.utils.ConfigHelper.valueOrDefault;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

/**
 * The NewRelicConfig contains the {@link NewRelicPipelineOptions} that were supplied
 * when starting the Apache Beam job, and which will be used by the {@link com.google.cloud.teleport.newrelic.ptransforms.NewRelicIO}
 * transform to conveniently batch and send the processed logs to New Relic.
 */
public class NewRelicConfig {
    protected static final String DEFAULT_LOGS_API_URL = "https://log-api.newrelic.com/log/v1";
    protected static final int DEFAULT_BATCH_COUNT = 100;
    protected static final boolean DEFAULT_DISABLE_CERTIFICATE_VALIDATION = false;
    protected static final boolean DEFAULT_USE_COMPRESSION = true;
    protected static final int DEFAULT_FLUSH_DELAY = 2;
    protected static final Integer DEFAULT_PARALLELISM = 1;

    private final ValueProvider<String> logsApiUrl;
    private final ValueProvider<String> licenseKey;
    private final ValueProvider<Integer> batchCount;
    private final ValueProvider<Integer> parallelism;
    private final ValueProvider<Boolean> disableCertificateValidation;
    private final ValueProvider<Boolean> useCompression;
    private final ValueProvider<Integer> flushDelay;

    private NewRelicConfig(final ValueProvider<String> logsApiUrl,
                           final ValueProvider<String> licenseKey,
                           final ValueProvider<Integer> batchCount,
                           final ValueProvider<Integer> flushDelay,
                           final ValueProvider<Integer> parallelism,
                           final ValueProvider<Boolean> disableCertificateValidation,
                           final ValueProvider<Boolean> useCompression) {
        this.logsApiUrl = logsApiUrl;
        this.licenseKey = licenseKey;
        this.batchCount = batchCount;
        this.flushDelay = flushDelay;
        this.parallelism = parallelism;
        this.disableCertificateValidation = disableCertificateValidation;
        this.useCompression = useCompression;
    }

    public static NewRelicConfig fromPipelineOptions(final NewRelicPipelineOptions newRelicOptions) {
        checkArgument(newRelicOptions.getLicenseKey() != null && newRelicOptions.getLicenseKey().isAccessible() && newRelicOptions.getLicenseKey().get() != null, "New Relic License Key is required for writing events.");

        return new NewRelicConfig(
                valueOrDefault(newRelicOptions.getLogsApiUrl(), DEFAULT_LOGS_API_URL),
                newRelicOptions.getTokenKMSEncryptionKey().isAccessible()
                        ? maybeDecrypt(newRelicOptions.getLicenseKey(), newRelicOptions.getTokenKMSEncryptionKey())
                        : newRelicOptions.getLicenseKey(),
                valueOrDefault(newRelicOptions.getBatchCount(), DEFAULT_BATCH_COUNT),
                valueOrDefault(newRelicOptions.getFlushDelay(), DEFAULT_FLUSH_DELAY),
                valueOrDefault(newRelicOptions.getParallelism(), DEFAULT_PARALLELISM),
                valueOrDefault(newRelicOptions.getDisableCertificateValidation(), DEFAULT_DISABLE_CERTIFICATE_VALIDATION),
                valueOrDefault(newRelicOptions.getUseCompression(), DEFAULT_USE_COMPRESSION));
    }

    /**
     * Utility method to decrypt a NewRelic API token.
     *
     * @param unencryptedToken The NewRelic API token as a Base64 encoded {@link String} encrypted with a Cloud KMS Key.
     * @param kmsKey           The Cloud KMS Encryption Key to decrypt the NewRelic API token.
     * @return Decrypted NewRelic API token.
     */
    private static ValueProvider<String> maybeDecrypt(
            ValueProvider<String> unencryptedToken, ValueProvider<String> kmsKey) {
        return new KMSEncryptedNestedValueProvider(unencryptedToken, kmsKey);
    }

    public ValueProvider<String> getLogsApiUrl() {
        return logsApiUrl;
    }

    public ValueProvider<String> getLicenseKey() {
        return licenseKey;
    }

    public ValueProvider<Integer> getBatchCount() {
        return batchCount;
    }

    public ValueProvider<Integer> getFlushDelay() {
        return flushDelay;
    }

    public ValueProvider<Integer> getParallelism() {
        return parallelism;
    }

    public ValueProvider<Boolean> getDisableCertificateValidation() {
        return disableCertificateValidation;
    }

    public ValueProvider<Boolean> getUseCompression() {
        return useCompression;
    }
}
