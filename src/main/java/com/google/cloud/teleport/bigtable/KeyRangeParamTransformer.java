package com.google.cloud.teleport.bigtable;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transform keyRange from string to ByteKeyRange object.
 */
public class KeyRangeParamTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(KeyRangeParamTransformer.class);

    public static List<ByteKeyRange> getByteKeyRanges(String keyRange) {
        ByteKeyRange kr = ByteKeyRange.ALL_KEYS;
        if (keyRange != null && keyRange.contains("|")) {
            String[] split = keyRange.split("\\|");
            String start = split[0];
            String end = split[1];

            ByteKey startKey = ByteKey.copyFrom(start.getBytes(StandardCharsets.UTF_8));
            ByteKey endKey = ByteKey.copyFrom(end.getBytes(StandardCharsets.UTF_8));
            kr = ByteKeyRange.of(startKey, endKey);
        }
        LOG.info("ByteKeyRange  " + kr + " from " + keyRange);
        return Collections.singletonList(kr);
    }
}
