/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.values;

/**
 * Entity views for {@link
 * com.google.api.services.dataplex.v1.CloudDataplex.Projects.Locations.Lakes.Zones.Entities.Get#getView()}.
 */
public enum GetEntityRequestEntityView {
  /** The API will default to the BASIC view. */
  ENTITY_VIEW_UNSPECIFIED,

  /** Minimal view that does not include the schema. */
  BASIC,

  /** Includes basic information and schema. */
  SCHEMA,

  /** Includes basic information and statistics. */
  STATS,

  /** Includes everything. */
  FULL
}
