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
package com.google.cloud.dataflow.cdc.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;

import org.junit.Test;

/** Tests for KnowledgeCatalogSchemaUtils class. */
public class KnowledgeCatalogSchemaUtilsTest {

  @Test
  public void testEntryGroupNameForTopic() {
    assertThat(
        KnowledgeCatalogSchemaUtils.entryGroupNameForTopic("my_topic"), equalTo("cdc-my-topic"));
    assertThat(
        KnowledgeCatalogSchemaUtils.entryGroupNameForTopic("cdc-already-prefixed"),
        equalTo("cdc-already-prefixed"));
    assertThat(
        KnowledgeCatalogSchemaUtils.entryGroupNameForTopic("prefix_with_dots.and_more"),
        equalTo("cdc-prefix-with-dots-and-more"));
    assertThat(
        KnowledgeCatalogSchemaUtils.entryGroupNameForTopic("trailing_dash_"),
        equalTo("cdc-trailing-dash"));
  }

  @Test
  public void testEntryGroupNameForLongTopic() {
    String longTopic =
        "very_long_topic_name_that_exceeds_sixty_three_characters_limit_in_dataplex_entry_group";
    String entryGroup = KnowledgeCatalogSchemaUtils.entryGroupNameForTopic(longTopic);

    assertThat(entryGroup.length(), lessThanOrEqualTo(63));
    assertThat(entryGroup, startsWith("cdc-"));
  }
}
