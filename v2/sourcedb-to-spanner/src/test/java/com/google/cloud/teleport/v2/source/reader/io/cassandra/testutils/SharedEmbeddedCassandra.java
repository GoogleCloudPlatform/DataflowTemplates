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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import org.jline.utils.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Utility for Shared instance of {@link com.github.nosan.embedded.cassandra.Cassandra Embedded
 * Cassandra}.
 */
public class SharedEmbeddedCassandra implements AutoCloseable {

  private static ConcurrentHashMap<Configuration, RefCountedEmbeddedCassandra> instances =
      new ConcurrentHashMap();
  private static Lock lock = new ReentrantLock();
  private static final Logger LOG = LoggerFactory.getLogger(SharedEmbeddedCassandra.class);

  private Configuration config;
  private EmbeddedCassandra embeddedCassandra;

  /**
   * Get or start an instance of embedded Cassandra for testing. Note: Call this as a part of {@link
   * org.junit.BeforeClass}
   *
   * @param config - config.yaml
   * @param cqlResource - cql script.
   * @throws IOException
   */
  public SharedEmbeddedCassandra(String config, @Nullable String cqlResource) throws IOException {
    this.config = Configuration.create(config, cqlResource);
    this.embeddedCassandra = getEmbeddedCassandra(this.config);
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
  public void close() throws Exception {

    if (this.embeddedCassandra == null || this.config == null) {
      return;
    }
    putEmbeddedCassandra(this.config);
    this.embeddedCassandra = null;
    this.config = null;
  }

  private static EmbeddedCassandra getEmbeddedCassandra(Configuration configuration)
      throws IOException {
    EmbeddedCassandra embeddedCassandra = null;
    lock.lock();
    try {
      Log.info("Getting Shared embedded Cassandra for configuration = {}", configuration);
      if (instances.containsKey(configuration)) {
        RefCountedEmbeddedCassandra refCountedEmbeddedCassandra = instances.get(configuration);
        refCountedEmbeddedCassandra.refIncrementAndGet();
        embeddedCassandra = refCountedEmbeddedCassandra.embeddedCassandra();
      } else {
        Log.info("Starting Shared embedded Cassandra for configuration = {}", configuration);
        embeddedCassandra =
            new EmbeddedCassandra(configuration.configYaml(), configuration.cqlScript());
        RefCountedEmbeddedCassandra refCountedEmbeddedCassandra =
            RefCountedEmbeddedCassandra.create(embeddedCassandra);
        refCountedEmbeddedCassandra.refIncrementAndGet();
        instances.put(configuration, refCountedEmbeddedCassandra);
      }
    } finally {
      lock.unlock();
    }

    return embeddedCassandra;
  }

  private static void putEmbeddedCassandra(Configuration configuration) throws Exception {
    lock.lock();
    try {
      if (instances.containsKey(configuration)) {
        RefCountedEmbeddedCassandra refCountedEmbeddedCassandra = instances.get(configuration);
        if (refCountedEmbeddedCassandra.refDecrementAndGet() == 0) {
          Log.info("Stopping Shared embedded Cassandra for configuration = {}", configuration);
          instances.remove(configuration);
          refCountedEmbeddedCassandra.embeddedCassandra().close();
        }
      }
    } finally {
      lock.unlock();
    }
  }

  @AutoValue
  abstract static class Configuration {
    public AtomicInteger refCount = new AtomicInteger();

    public static Configuration create(String configYaml, String cqlScript) {
      return new AutoValue_SharedEmbeddedCassandra_Configuration(configYaml, cqlScript);
    }

    @Nullable
    public abstract String configYaml();

    @Nullable
    public abstract String cqlScript();
  }

  // This is a private class, and it must be ensured that refcounting is synchronized.
  @AutoValue
  abstract static class RefCountedEmbeddedCassandra {
    private AtomicInteger refCount = new AtomicInteger();

    public static RefCountedEmbeddedCassandra create(EmbeddedCassandra embeddedCassandra) {
      AutoValue_SharedEmbeddedCassandra_RefCountedEmbeddedCassandra ret =
          new AutoValue_SharedEmbeddedCassandra_RefCountedEmbeddedCassandra(embeddedCassandra);
      ret.refIncrementAndGet();
      return ret;
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
