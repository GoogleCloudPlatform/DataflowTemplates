/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Utility for Shared instance of {@link com.github.nosan.embedded.cassandra.Cassandra Embedded
 * Cassandra}.
 */
public class SharedEmbeddedCassandra implements AutoCloseable {

  private static final ConcurrentHashMap<Configuration, RefCountedEmbeddedCassandra> instances =
      new ConcurrentHashMap<>();
  private static final Logger LOG = LoggerFactory.getLogger(SharedEmbeddedCassandra.class);

  private Configuration config;
  private EmbeddedCassandra embeddedCassandra;

  /**
   * Get or start an instance of embedded Cassandra for testing. Note: Call this as a part of {@link
   * org.junit.BeforeClass}
   *
   * @param config - config.yaml
   * @param cqlResource - cql script.
   * @param clientEncryption - set to true if Client side SSL is needed.
   */
  public SharedEmbeddedCassandra(
      String config, @Nullable String cqlResource, Boolean clientEncryption) {
    this.config = Configuration.create(config, cqlResource, clientEncryption);
    this.embeddedCassandra = getEmbeddedCassandra(this.config);
  }

  public SharedEmbeddedCassandra(String config, @Nullable String cqlResource) {
    this(config, cqlResource, Boolean.FALSE);
  }

  /**
   * Get a reference to {@link com.github.nosan.embedded.cassandra.Cassandra Embedded Cassandra}
   * managed by {@link SharedEmbeddedCassandra}.
   */
  public EmbeddedCassandra getInstance() {
    return embeddedCassandra;
  }

  /**
   * Close this instance of {@link SharedEmbeddedCassandra}. Note: Call this as a part of {@link
   * org.junit.AfterClass} The actual {@link com.github.nosan.embedded.cassandra.Cassandra Embedded
   * Cassandra} instance is closed only after all the tests sharing it have completed.
   */
  @Override
  public void close() {

    if (this.embeddedCassandra == null || this.config == null) {
      return;
    }
    putEmbeddedCassandra(this.config);
    this.embeddedCassandra = null;
    this.config = null;
  }

  private static EmbeddedCassandra getEmbeddedCassandra(Configuration configuration) {
    return instances
        .compute(
            configuration,
            (config, refCountedCassandra) -> {
              if (refCountedCassandra == null) {
                LOG.info("Starting Shared embedded Cassandra for configuration = {}", config);
                try {
                  EmbeddedCassandra newCassandra =
                      new EmbeddedCassandra(
                          config.configYaml(), config.cqlScript(), config.clientEncryption());
                  // Wait for cassandra to be healthy.
                  int maxRetries = 60;
                  int retries = 0;
                  boolean isHealthy = false;
                  while (retries < maxRetries && !isHealthy) {
                    isHealthy = newCassandra.isHealthy();
                    if (!isHealthy) {
                      try {
                        Thread.sleep(2000); // Wait for 2 seconds before retrying.
                      } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                      }
                    }
                    retries++;
                  }
                  if (!isHealthy) {
                    throw new RuntimeException("Embedded cassandra is not healthy after waiting");
                  }
                  refCountedCassandra = RefCountedEmbeddedCassandra.create(newCassandra);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              } else {
                LOG.info("Getting Shared embedded Cassandra for configuration = {}", config);
              }
              refCountedCassandra.refIncrementAndGet();
              return refCountedCassandra;
            })
        .embeddedCassandra();
  }

  private static void putEmbeddedCassandra(Configuration configuration) {
    instances.computeIfPresent(
        configuration,
        (config, refCountedCassandra) -> {
          if (refCountedCassandra.refDecrementAndGet() == 0) {
            LOG.info("Stopping Shared embedded Cassandra for configuration = {}", config);
            try {
              refCountedCassandra.embeddedCassandra().close();
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
            return null; // remove from map
          }
          return refCountedCassandra; // keep in map
        });
  }

  @AutoValue
  abstract static class Configuration {
    public AtomicInteger refCount = new AtomicInteger();

    public static Configuration create(
        String configYaml, String cqlScript, Boolean clientEncryption) {
      return new AutoValue_SharedEmbeddedCassandra_Configuration(
          configYaml, cqlScript, clientEncryption);
    }

    @Nullable
    public abstract String configYaml();

    @Nullable
    public abstract String cqlScript();

    @Nullable
    public abstract Boolean clientEncryption();
  }

  // This is a private class, and it must be ensured that refcounting is synchronized.
  @AutoValue
  abstract static class RefCountedEmbeddedCassandra {
    private AtomicInteger refCount = new AtomicInteger();

    public static RefCountedEmbeddedCassandra create(EmbeddedCassandra embeddedCassandra) {
      return new AutoValue_SharedEmbeddedCassandra_RefCountedEmbeddedCassandra(embeddedCassandra);
    }

    public abstract EmbeddedCassandra embeddedCassandra();

    public int getRef() {
      return refCount.get();
    }

    public int refIncrementAndGet() {
      return refCount.incrementAndGet();
    }

    public int refDecrementAndGet() {
      return refCount.decrementAndGet();
    }
  }
}
