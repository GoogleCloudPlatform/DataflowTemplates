package com.google.cloud.teleport.v2.spanner.migrations.utils;

import com.google.cloud.teleport.v2.spanner.migrations.shard.ShardingContext;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.GsonBuilder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;

public class ShardingContextReader {
    private static final Logger LOG = LoggerFactory.getLogger(ShardingContextReader.class);

    /** Path of the session file on GCS. */
    public static ShardingContext getShardingContext(
            String shardingContextFilePath) {
        if (shardingContextFilePath == null || shardingContextFilePath.isBlank()) {
            return new ShardingContext();
        }
        return readFileIntoMemory(shardingContextFilePath);
    }

    private static ShardingContext readFileIntoMemory(String filePath) {
        try (InputStream stream =
                     Channels.newInputStream(FileSystems.open(FileSystems.matchNewResource(filePath, false)))) {
            String result = IOUtils.toString(stream, StandardCharsets.UTF_8);

            ShardingContext shardingContext =
                    new GsonBuilder()
                            .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
                            .create()
                            .fromJson(result, ShardingContext.class);
            LOG.info("Sharding context obj: " + shardingContext.toString());
            return shardingContext;
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to read sharding context file. Make sure it is ASCII or UTF-8 encoded and"
                            + " contains a well-formed JSON string.",
                    e);
        }
    }
}
