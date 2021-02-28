package com.mw.pipeline.util;

import org.apache.beam.sdk.options.ValueProvider;

public class OptionsUtil {

    public static <T extends ValueProvider>String getStringValue(T t){
        if (t.isAccessible()) {
            return (String)t.get();
        }
        return "";
    }

}
