package com.google.cloud.teleport.newrelic.config;

import com.google.cloud.teleport.util.KMSEncryptedNestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * The NewRelicConfig contains the {@link NewRelicPipelineOptions} that were supplied
 * when starting the Apache Beam job, and which will be used by the {@link com.google.cloud.teleport.newrelic.ptransforms.NewRelicIO}
 * transform to conveniently batch and send the processed logs to New Relic.
 */
public class NewRelicConfig {
    private final ValueProvider<String> logsApiUrl;
    private final ValueProvider<String> licenseKey;
    private final ValueProvider<Integer> batchCount;
    private final ValueProvider<Integer> parallelism;
    private final ValueProvider<Boolean> disableCertificateValidation;
    private final ValueProvider<Boolean> useCompression;

    private NewRelicConfig(final ValueProvider<String> logsApiUrl,
                           final ValueProvider<String> licenseKey,
                           final ValueProvider<Integer> batchCount,
                           final ValueProvider<Integer> parallelism,
                           final ValueProvider<Boolean> disableCertificateValidation,
                           final ValueProvider<Boolean> useCompression) {
        this.logsApiUrl = logsApiUrl;
        this.licenseKey = licenseKey;
        this.batchCount = batchCount;
        this.parallelism = parallelism;
        this.disableCertificateValidation = disableCertificateValidation;
        this.useCompression = useCompression;
    }

    public static NewRelicConfig fromPipelineOptions(final NewRelicPipelineOptions newRelicOptions) {
        return new NewRelicConfig(
                newRelicOptions.getLogsApiUrl(),
                newRelicOptions.getTokenKMSEncryptionKey().isAccessible()
                        ? maybeDecrypt(newRelicOptions.getLicenseKey(), newRelicOptions.getTokenKMSEncryptionKey())
                        : newRelicOptions.getLicenseKey(),
                newRelicOptions.getBatchCount(),
                newRelicOptions.getParallelism(),
                newRelicOptions.getDisableCertificateValidation(),
                newRelicOptions.getUseCompression());
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
