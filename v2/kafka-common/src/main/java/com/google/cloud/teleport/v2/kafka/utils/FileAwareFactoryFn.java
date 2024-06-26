/*
 * Copyright (C) 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.kafka.utils;

import com.google.cloud.teleport.v2.utils.SecretManagerUtils;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Kafka consumer/producer factory implementation that pre-processes the consumer config and
 * replaces all references to GCS paths with local files and all references to Secret Manager
 * secrets with their actual values.
 */
public abstract class FileAwareFactoryFn<T>
    implements SerializableFunction<Map<String, Object>, T> {
  private static final String LOCAL_FILE_PREFIX = "/tmp/kafka";

  public static final String GCS_PATH_PREFIX = "gs://";
  public static final String SECRET_MANAGER_VALUE_PREFIX = "secretValue:";
  public static final String SECRET_MANAGER_FILE_PREFIX = "secretFile:";
  private static final Map<String, String> secretCache = new ConcurrentHashMap<>();

  private final String factoryType;
  private final String filePrefix;

  /* Logger for class.*/
  private static final Logger LOG = LoggerFactory.getLogger(FileAwareFactoryFn.class);

  protected FileAwareFactoryFn(String factoryType, String filePrefix) {
    Preconditions.checkNotNull(factoryType);
    this.factoryType = factoryType;
    this.filePrefix = filePrefix;
  }

  protected abstract T createObject(Map<String, Object> config);

  @Override
  public T apply(Map<String, Object> config) {
    if (config == null) {
      return createObject(config);
    }

    Map<String, Object> processedConfig = new HashMap<>();

    for (Map.Entry<String, Object> e : config.entrySet()) {
      String key = e.getKey();
      String value = e.getValue() == null ? "" : e.getValue().toString();
      String processedValue = null;

      try {
        if (value.startsWith(GCS_PATH_PREFIX)) {
          processedValue = getGcsFileAsLocal(value, generateLocalFileName(key));
        } else if (value.startsWith(SECRET_MANAGER_VALUE_PREFIX)) {
          String secretId = value.substring(SECRET_MANAGER_VALUE_PREFIX.length());
          processedValue = getSecretWithCache(secretId);
        } else if (value.startsWith(SECRET_MANAGER_FILE_PREFIX)) {
          throw new UnsupportedOperationException("Not yet implemented");
        }
      } catch (IOException ex) {
        throw new RuntimeException(
            "Couldn't load Kafka consumer property " + key + " = " + value, ex);
      }

      processedConfig.put(key, processedValue != null ? processedValue : e.getValue());
    }

    return createObject(processedConfig);
  }

  private String generateLocalFileName(String key) {
    return LOCAL_FILE_PREFIX
        + "-"
        + factoryType
        + (filePrefix == null ? "" : "-" + filePrefix)
        + "-"
        + key;
  }

  private String getSecretWithCache(String secretId) {
    return secretCache.computeIfAbsent(secretId, id -> SecretManagerUtils.getSecret(id));
  }

  public static synchronized String getGcsFileAsLocal(String gcsFilePath, String outputFilePath)
      throws IOException {
    // create the file only if it doesn't exist
    if (!new File(outputFilePath).exists()) {
      LOG.info("Staging GCS file [{}] to [{}]", gcsFilePath, outputFilePath);
      Set<StandardOpenOption> options = new HashSet<>(2);
      options.add(StandardOpenOption.CREATE);
      options.add(StandardOpenOption.WRITE);
      // Copy the GCS file into a local file and will throw an I/O exception in case file not found.
      try (ReadableByteChannel readerChannel =
          FileSystems.open(FileSystems.matchSingleFileSpec(gcsFilePath).resourceId())) {
        try (FileChannel writeChannel = FileChannel.open(Paths.get(outputFilePath), options)) {
          writeChannel.transferFrom(readerChannel, 0, Long.MAX_VALUE);
        }
      }
    }
    return outputFilePath;
  }
}
