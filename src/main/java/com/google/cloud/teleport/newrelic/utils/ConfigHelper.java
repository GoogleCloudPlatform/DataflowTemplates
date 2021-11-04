package com.google.cloud.teleport.newrelic.utils;

import com.google.cloud.teleport.newrelic.config.NewRelicConfig;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigHelper {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigHelper.class);

    private ConfigHelper() {
        throw new AssertionError("This class should not be instantiated");
    }

    /**
     * Returns the value included in the provided ValueProvider, if it's available and non-null. In any other case,
     * it returns the default value provided in the second argument.
     * @param value The value to use, if it's non-null.
     * @param defaultValue Fallback value to use if the provided ValueProvider is null or holds a null value.
     * @param <T> The type of the value being read
     * @return The value included in the provided ValueProvider, if it's available and non-null, otherwise the default value.
     */
    public static <T> ValueProvider<T> valueOrDefault(ValueProvider<T> value, T defaultValue ) {
        return (value != null && value.isAccessible()) && value.get() != null
                ? value
                : StaticValueProvider.of(defaultValue);
    }

    public static void logConfiguration(final NewRelicConfig newRelicConfig) {
        LOG.info("Batch count set to: {}", newRelicConfig.getBatchCount().get());
        LOG.info("Disable certificate validation set to: {}", newRelicConfig.getDisableCertificateValidation().get());
        LOG.info("Use Compression set to: {}", newRelicConfig.getUseCompression().get());
        LOG.info("Parallelism set to: {}", newRelicConfig.getParallelism().get());
    }
}
