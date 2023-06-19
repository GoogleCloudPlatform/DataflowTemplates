package com.google.cloud.teleport.v2.spanner.migrations.utils;

import com.google.cloud.teleport.v2.spanner.migrations.transformation.TransformationContext;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransformationContextReader {
    private static final Logger LOG = LoggerFactory.getLogger(TransformationContextReader.class);

    /** Path of the session file on GCS. */

    public static TransformationContext getTransformationContext(String transformationContextFilePath) {
        if (transformationContextFilePath == null || transformationContextFilePath.isBlank()) {
            return new TransformationContext();
        }
        return readFileIntoMemory(transformationContextFilePath);
    }

    private static TransformationContext readFileIntoMemory(String filePath) {
        try (InputStream stream =
                     Channels.newInputStream(FileSystems.open(FileSystems.matchNewResource(filePath, false)))) {
            String result = IOUtils.toString(stream, StandardCharsets.UTF_8);

            TransformationContext transformationContext =
                    new GsonBuilder()
                            .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
                            .create()
                            .fromJson(result, TransformationContext.class);
            LOG.info("Transformation context obj: " + transformationContext.toString());
            return transformationContext;
        } catch (IOException e) {
            LOG.error(
                    "Failed to read transformation context file. Make sure it is ASCII or UTF-8 encoded and contains a"
                            + " well-formed JSON string.",
                    e);
            throw new RuntimeException(
                    "Failed to read transformation context file. Make sure it is ASCII or UTF-8 encoded and contains a"
                            + " well-formed JSON string.",
                    e);
        }
    }
}

