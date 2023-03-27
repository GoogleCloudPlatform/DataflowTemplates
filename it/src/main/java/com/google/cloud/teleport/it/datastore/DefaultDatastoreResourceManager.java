/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.it.datastore;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auth.Credentials;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.GqlQuery;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query.ResultType;
import com.google.cloud.datastore.QueryResults;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefaultDatastoreResourceManager implements DatastoreResourceManager {

  private final String namespace;

  private final Datastore datastore;
  private final Set<Key> keys;

  public DefaultDatastoreResourceManager(Builder builder) {
    this.namespace = builder.namespace;

    this.datastore =
        DatastoreOptions.newBuilder()
            .setProjectId(builder.project)
            .setCredentials(builder.credentials)
            .build()
            .getService();
    this.keys = new HashSet<>();
  }

  @VisibleForTesting
  DefaultDatastoreResourceManager(String namespace, Datastore datastore) {
    this.namespace = namespace;
    this.datastore = datastore;
    this.keys = new HashSet<>();
  }

  @Override
  public List<Entity> insert(String kind, Map<Long, FullEntity<?>> entities) {
    List<Entity> created = new ArrayList<>();

    try {
      for (Map.Entry<Long, FullEntity<?>> entry : entities.entrySet()) {
        Key entityKey =
            datastore.newKeyFactory().setKind(kind).setNamespace(namespace).newKey(entry.getKey());
        Entity entity = Entity.newBuilder(entityKey, entry.getValue()).build();
        created.add(datastore.put(entity));
        keys.add(entityKey);
      }
    } catch (Exception e) {
      throw new DatastoreResourceManagerException("Error inserting Datastore entity", e);
    }

    return created;
  }

  @Override
  public List<Entity> query(String gqlQuery) {
    try {
      QueryResults<Entity> queryResults =
          datastore.run(
              GqlQuery.newGqlQueryBuilder(ResultType.ENTITY, gqlQuery)
                  .setNamespace(namespace)
                  .build());

      List<Entity> entities = new ArrayList<>();

      while (queryResults.hasNext()) {
        Entity entity = queryResults.next();
        entities.add(entity);

        // Mark for deletion if namespace matches the test
        if (entity.getKey().getNamespace().equals(namespace)) {
          keys.add(entity.getKey());
        }
      }
      return entities;
    } catch (Exception e) {
      throw new DatastoreResourceManagerException("Error running Datastore query", e);
    }
  }

  @Override
  public void cleanupAll() {
    try {
      datastore.delete(keys.toArray(new Key[0]));
    } catch (Exception e) {
      throw new DatastoreResourceManagerException("Error cleaning up resources", e);
    }
    keys.clear();
  }

  public static Builder builder(String project, String namespace) {
    checkArgument(!Strings.isNullOrEmpty(project), "project can not be empty");
    checkArgument(!Strings.isNullOrEmpty(namespace), "namespace can not be empty");
    return new Builder(project, namespace);
  }

  public static final class Builder {

    private final String project;
    private final String namespace;
    private Credentials credentials;

    private Builder(String project, String namespace) {
      this.project = project;
      this.namespace = namespace;
    }

    public Builder credentials(Credentials credentials) {
      this.credentials = credentials;
      return this;
    }

    public DefaultDatastoreResourceManager build() {
      if (credentials == null) {
        throw new IllegalArgumentException(
            "Unable to find credentials. Please provide credentials to authenticate to GCP");
      }
      return new DefaultDatastoreResourceManager(this);
    }
  }
}
