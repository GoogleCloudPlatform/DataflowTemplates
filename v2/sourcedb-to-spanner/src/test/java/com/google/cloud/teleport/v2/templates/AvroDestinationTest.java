/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link AvroDestination}. */
@RunWith(JUnit4.class)
public class AvroDestinationTest {

  /** Tests the equality and inequality of {@link AvroDestination} objects. */
  @Test
  public void testAvroDestinationEquals() {
    AvroDestination dest1 = AvroDestination.of("shard1", "table1", "schema1");
    AvroDestination dest2 = AvroDestination.of("shard1", "table1", "schema1");
    AvroDestination dest3 = AvroDestination.of("shard2", "table1", "schema1");
    AvroDestination dest4 = AvroDestination.of("shard1", "table2", "schema1");
    AvroDestination dest5 = AvroDestination.of("shard1", "table1", "schema2");

    assertThat(dest1).isEqualTo(dest1);
    assertThat(dest1).isEqualTo(dest2);
    assertThat(dest1).isNotEqualTo(dest3);
    assertThat(dest1).isNotEqualTo(dest4);
    assertThat(dest1).isNotEqualTo(dest5);
    assertThat(dest1).isNotEqualTo(null);
    assertThat(dest1).isNotEqualTo("not an AvroDestination");
  }

  /** Tests the hash code generation of {@link AvroDestination} objects. */
  @Test
  public void testAvroDestinationHashCode() {
    AvroDestination dest1 = AvroDestination.of("shard1", "table1", "schema1");
    AvroDestination dest2 = AvroDestination.of("shard1", "table1", "schema1");
    AvroDestination dest3 = AvroDestination.of("shard2", "table1", "schema1");

    assertThat(dest1.hashCode()).isEqualTo(dest2.hashCode());
    // Note: hashCode in AvroDestination currently only uses name and jsonSchema, not shardId
    assertThat(dest1.hashCode()).isEqualTo(dest3.hashCode());
  }
}
