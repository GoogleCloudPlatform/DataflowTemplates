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
package com.google.cloud.teleport.it.gcp.datastore;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.teleport.it.common.ResourceManager;
import java.util.List;
import java.util.Map;

/** Interface for managing Datastore resources in integration tests. */
public interface DatastoreResourceManager extends ResourceManager {

  /**
   * Insert entities to Datastore.
   *
   * @param kind Kind of document to insert.
   * @param entities Entities to insert to Datastore.
   * @return Entities.
   */
  List<Entity> insert(String kind, Map<Long, FullEntity<?>> entities);

  /**
   * Run a Gql Query and return the results in entity format.
   *
   * @param gqlQuery Gql Query to run.
   * @return Entities returned from the query.
   */
  List<Entity> query(String gqlQuery);

  /**
   * Deletes all created entities and cleans up the Datastore client.
   *
   * @throws DatastoreResourceManagerException if there is an error deleting the tables or dataset
   *     in BigQuery.
   */
  void cleanupAll();
}
