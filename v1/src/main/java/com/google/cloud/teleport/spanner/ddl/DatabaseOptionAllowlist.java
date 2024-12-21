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
package com.google.cloud.teleport.spanner.ddl;

import com.google.common.collect.ImmutableList;

/** Cloud Spanner database option allowlist for Export/Import. */
public class DatabaseOptionAllowlist {

  // Private constructor to prevent initializing instance, because this class is only served as
  // allow list.
  private DatabaseOptionAllowlist() {}

  // Only those database options whose name are included in the allowlist will be processed in
  // export/import pipelines.
  public static final ImmutableList<String> DATABASE_OPTION_ALLOWLIST =
      ImmutableList.of(
          "version_retention_period", "opt_in_dataplacement_preview", "default_sequence_kind");
}
