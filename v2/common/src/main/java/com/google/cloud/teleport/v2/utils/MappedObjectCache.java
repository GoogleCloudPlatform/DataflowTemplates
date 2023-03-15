/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.v2.utils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link MappedObjectCache} allows you to easily create a Map&lt;Key,Value&gt; cache where each
 * element expires and is re-acquired on a configurable basis.
 *
 * <p>The key factors addressed are ensuring expiration of cached objects, consistent update
 * behavior to ensure reliability, and easy cache reloads. Open Question: Does the class require
 * thread-safe behaviors? Currently, it does not since there is no iteration and get/set are not
 * continuous.
 */
public abstract class MappedObjectCache<KeyT, ValueT> implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(MappedObjectCache.class);

  public Cache<KeyT, ValueT> cachedObjects =
      CacheBuilder.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).<KeyT, ValueT>build();
  private int maxNumRetries = 0;
  // private Integer cacheResetTimeUnitValue = 5;
  // private TimeUnit cacheResetTimeUnit = TimeUnit.MINUTES;

  /**
   * Create an instance of a {@link MappedObjectCache} to track table schemas.
   *
   * @param client A Client which is required to pull the ValueT.
   */
  public MappedObjectCache() {}

  /**
   * Set the cache life for the {@code MappedObjectCache} instance.
   *
   * @param value The number of minutes before reseting a cached value.
   */
  public MappedObjectCache withCacheResetTimeUnitValue(Integer value) {
    this.cachedObjects =
        CacheBuilder.newBuilder().expireAfterWrite(value, TimeUnit.MINUTES).<KeyT, ValueT>build();

    return this;
  }

  /**
   * Set the number of retries {@code MappedObjectCache} will use each time it attempt to reset the
   * cache.
   *
   * @param value The number of minutes before reseting a cached value.
   */
  public MappedObjectCache withCacheNumRetries(int numRetries) {
    this.maxNumRetries = numRetries;

    return this;
  }

  /**
   * Return a {@code ValueT} representing the value requested to be stored.
   *
   * @param key A key used to lookup the value in the set.
   */
  public ValueT get(KeyT key) {
    // Reset cache if the object DNE in the map.
    ValueT value = cachedObjects.getIfPresent(key);
    if (value == null) {
      return this.reset(key, false, null);
    }
    return value;
  }

  public abstract ValueT getObjectValue(KeyT key);

  private ValueT getObjectValueWithRetries(KeyT key, int retriesRemaining) {
    try {
      return getObjectValue(key);
    } catch (Exception e) {
      if (retriesRemaining > 0) {
        int sleepSecs = (this.maxNumRetries - retriesRemaining + 1) * 10;
        LOG.info("Cache Exception, will retry after {} seconds: {}", sleepSecs, e.toString());
        try {
          Thread.sleep(sleepSecs * 1000);
          return getObjectValueWithRetries(key, retriesRemaining - 1);
        } catch (InterruptedException i) {
        }
      }
      throw e;
    }
  }

  /**
   * Returns a {@code ValueT} extracted from abstract getObjectValue(key) and sets the value in the
   * local cache.
   *
   * @param key a key used to lookup the value in the set.
   */
  public ValueT reset(KeyT key) {
    return this.reset(key, true, null);
  }

  /**
   * Returns a {@code ValueT} extracted from abstract getObjectValue(key) and sets the value in the
   * local cache.
   *
   * @param key a key used to lookup the value in the set.
   * @param currentValue is the current ValueT which a thread is using and if the stored value is
   *     already different than supply that.
   */
  public ValueT reset(KeyT key, ValueT currentValue) {
    return this.reset(key, false, currentValue);
  }

  /**
   * Returns a {@code ValueT} extracted from abstract getObjectValue(key) and sets the value in the
   * local cache.
   *
   * @param key a key used to lookup the value in the set.
   * @param force a boolean which tells the cache to ignore existing cache values.
   * @param currentValue is the current ValueT which a thread is using and if the stored value is
   *     already different than supply that.
   */
  private synchronized ValueT reset(KeyT key, Boolean force, ValueT currentValue) {
    ValueT value;
    if (!force) {
      value = cachedObjects.getIfPresent(key);
      if (value != null && value != currentValue) {
        return value;
      }
    }
    value = getObjectValueWithRetries(key, this.maxNumRetries);
    if (value != null) {
      cachedObjects.put(key, value);
    }
    return value;
  }
}
