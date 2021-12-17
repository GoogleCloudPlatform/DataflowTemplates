/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.dataflow.cdc.applier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Test;

/** Tests for Dynamic Destinations class for CDC template. */
public class ChangelogTableDynamicDestinationsTest {

  // TODO(pabloem): Add tests that rely on BigQueryFakeServices coming from Beam 2.15.0 OR 2.16.0.

  @Test
  void testTableNameFormatting() {
    String sourceTable = "mainstance.cdcForDataflow.team_metadata";

    assertThat(
        ChangelogTableDynamicDestinations.getBigQueryTableName(sourceTable, false),
        equalTo("team_metadata"));
    assertThat(
        ChangelogTableDynamicDestinations.getBigQueryTableName(sourceTable, true),
        equalTo("team_metadata_changelog"));
  }
}
