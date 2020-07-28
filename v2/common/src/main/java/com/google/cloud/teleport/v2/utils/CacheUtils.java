/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.v2.utils;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.common.base.Supplier;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CacheUtils supplies a generic class which allows you to create a Map of values to be used
 * as a cache.  These values are stored in suppliers and will automatically reset after a
 * given amount of time.  The value can be forace reset using the CacheUtils.reset()
 * functionality as well.
 */
public class CacheUtils {

  private static final Logger LOG = LoggerFactory.getLogger(CacheUtils.class);

  /**
   * The {@link BigQueryTableCache} manages safely getting and setting BigQuery Table objects from a
   * local cache for each worker thread.
   *
   * <p>The key factors addressed are ensuring expiration of cached tables, consistent update
   * behavior to ensure reliabillity, and easy cache reloads. Open Question: Does the class require
   * thread-safe behaviors? Currently it does not since there is no iteration and get/set are not
   * continuous.
   */
  public static class BigQueryTableCache
      extends MappedObjectCache<TableId, Table> {

    private BigQuery bigquery;

    /**
     * Create an instance of a {@link BigQueryTableCache} to track table schemas.
     *
     * @param bigquery A BigQuery instance used to extract Table objects.
     */
    public BigQueryTableCache(BigQuery bigquery) {
      this.bigquery = bigquery;
    }

    @Override
    public Table getObjectValue(TableId key) {
      Table table = this.bigquery.getTable(key);
      return table;
    }
  }

  /**
   * The {@link DataStreamPkCache} stores an expiring cached list of PKs for each
   * stream, schema, and table combination.
   */
  public static class DataStreamPkCache
      extends MappedObjectCache<List<String>, List<String>> {

    public DataStreamClient client;

    public DataStreamPkCache(DataStreamClient client) {
      this.client = client;
    }

    @Override
    public List<String> getObjectValue(List<String> key) {
      try {
        return this.client.getPrimaryKeys(key.get(0), key.get(1), key.get(2));
      } catch (IOException e) {
        LOG.error("IOException: DataStream Discovery on Primary Keys Failed.");
        return null;
      }
    }
  }

  /**
   * The {@link MappedObjectCache} allows you to easily create a Map<Key,Value> cache
   * where each element expires and is re-acquied on a configurable basis.
   *
   * <p>The key factors addressed are ensuring expiration of cached objects, consistent update
   * behavior to ensure reliability, and easy cache reloads.
   * Open Question: Does the class require thread-safe behaviors? Currently it does 
   * not since there is no iteration and get/set are not continuous.
   */
  public abstract static class MappedObjectCache<KeyT, ValueT> {

    private Map<KeyT, ExpiringSupplier<ValueT>> cachedObjects = new HashMap<KeyT, ExpiringSupplier<ValueT>>();

    /**
     * Create an instance of a {@link MappedObjectCache} to track table schemas.
     *
     * @param client A Client which is required to pull the ValueT.
     */
    public MappedObjectCache() {}

    /**
     * Return a {@code ValueT} representing the value requested to be stored.
     *
     * @param key A key used to lookup the value in the set.
     */
    public ValueT get(KeyT key) {
      ExpiringSupplier<ValueT> valueSupplier = cachedObjects.get(key);

      // Reset cache if the object DNE in the map
      if (valueSupplier == null) {
        return this.reset(key);
      }

      // Reset cache if the object expired or return the object.
      ValueT value = valueSupplier.get();
      if (value == null) {
        return this.reset(key);
      } else {
        return value;
      }
    }

    public abstract ValueT getObjectValue(KeyT key);

    /**
     * Returns a {@code ValueT} extracted from abstract getObjectValue(key)
     * and sets the value in the local cache.
     *
     * @param key a key used to lookup the value in the set.
     */
    public ValueT reset(KeyT key) {
      ValueT value = getObjectValue(key);

      ExpiringSupplier<ValueT> valueSupplier = new ExpiringSupplier<ValueT>(value, 5, TimeUnit.MINUTES);
      cachedObjects.put(key, valueSupplier);
      return value;
    }
  }

  /**
   * The {@link ExpiringSupplier} is a Supplier to help manage Objects which must expire.
   */
  public static class ExpiringSupplier<InputT> implements Supplier<InputT> {
    InputT value;
    long expiryTimeNano;

    public ExpiringSupplier(InputT value, long duration, TimeUnit unit) {
      this.value = value;
      this.expiryTimeNano = System.nanoTime() + unit.toNanos(duration);
    }

    @Override
    public InputT get() {
      if (this.expiryTimeNano < System.nanoTime()) {
        return null;
      }
      return this.value;
    }
  }
}
