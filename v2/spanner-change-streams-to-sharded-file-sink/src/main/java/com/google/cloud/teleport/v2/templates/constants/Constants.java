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
package com.google.cloud.teleport.v2.templates.constants;

/** A single class to store all constants. */
public class Constants {

  // Transaction tag prefix used in the forward migration job.
  public static final String FWD_MIGRATION_TRANSACTION_TAG_PREFIX = "txBy=";

  // Filtration Mode - none
  public static final String FILTRATION_MODE_NONE = "none";

  // Filtration Mode - forward_migration
  public static final String FILTRATION_MODE_FORWARD_MIGRATION = "forward_migration";

  // Sharding Mode - single_shard
  public static final String SHARDING_MODE_SINGLE_SHARD = "single_shard";

  // Sharding Mode - multi_shard
  public static final String SHARDING_MODE_MULTI_SHARD = "multi_shard";
}
