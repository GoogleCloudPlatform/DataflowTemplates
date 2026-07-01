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
package com.google.cloud.teleport.spanner.ddl;

import com.google.common.collect.ImmutableSet;

/**
 * A list of table options that are supported by the import/export process. See <a
 * href="https://cloud.google.com/spanner/docs/reference/standard-sql/data-definition-language#create_table_statement">CREATE
 * TABLE</a> for the full list of options.
 */
final class TableOptionAllowlist {

  private TableOptionAllowlist() {}

  static final ImmutableSet<String> TABLE_OPTION_ALLOWLIST =
      ImmutableSet.of("fulltext_dictionary_table");
}
