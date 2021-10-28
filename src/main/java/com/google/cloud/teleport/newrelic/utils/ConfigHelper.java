package com.google.cloud.teleport.newrelic.utils;

import org.apache.beam.sdk.options.ValueProvider;

public class ConfigHelper {
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
    public static <T> T valueOrDefault(ValueProvider<T> value, T defaultValue ) {
        return (value != null && value.isAccessible()) && value.get() != null
                ? value.get()
                : defaultValue;
    }
}
