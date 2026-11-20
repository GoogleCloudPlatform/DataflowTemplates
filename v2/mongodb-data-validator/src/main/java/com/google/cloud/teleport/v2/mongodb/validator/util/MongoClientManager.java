/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.mongodb.validator.util;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.bson.UuidRepresentation;

/**
 * Per-JVM shared {@link MongoClient} cache so concurrent Dataflow worker harness threads reuse a
 * bounded connection pool per endpoint rather than multiplying socket pools per {@code DoFn}
 * instance.
 */
public final class MongoClientManager {

  private static final ConcurrentHashMap<String, MongoClient> CLIENTS = new ConcurrentHashMap<>();

  private MongoClientManager() {}

  public static MongoClient getOrCreate(String uri, ReadPreference defaultReadPreference) {
    String prefName =
        defaultReadPreference == null ? "default" : defaultReadPreference.getName().toLowerCase();
    String cacheKey = uri + "|" + prefName;
    return CLIENTS.computeIfAbsent(cacheKey, k -> createNewClient(uri, defaultReadPreference));
  }

  public static MongoClient createDedicated(String uri) {
    return createNewClient(uri, null);
  }

  public static ReadPreference parseReadPreference(String name) {
    if (name == null || name.isBlank()) {
      return ReadPreference.secondaryPreferred();
    }
    switch (name.trim()) {
      case "primary":
        return ReadPreference.primary();
      case "primaryPreferred":
        return ReadPreference.primaryPreferred();
      case "secondary":
        return ReadPreference.secondary();
      case "secondaryPreferred":
      default:
        return ReadPreference.secondaryPreferred();
    }
  }

  private static MongoClient createNewClient(String uri, ReadPreference readPreference) {
    ConnectionString connString = new ConnectionString(uri);
    MongoClientSettings.Builder builder =
        MongoClientSettings.builder()
            .applyConnectionString(connString)
            .uuidRepresentation(UuidRepresentation.STANDARD)
            .applyToConnectionPoolSettings(
                pool -> pool.maxSize(128).maxWaitTime(60, TimeUnit.SECONDS))
            .applyToSocketSettings(
                sock ->
                    sock.connectTimeout(30, TimeUnit.SECONDS).readTimeout(120, TimeUnit.SECONDS));
    if (readPreference != null && connString.getReadPreference() == null) {
      builder.readPreference(readPreference);
    }
    return MongoClients.create(builder.build());
  }
}
