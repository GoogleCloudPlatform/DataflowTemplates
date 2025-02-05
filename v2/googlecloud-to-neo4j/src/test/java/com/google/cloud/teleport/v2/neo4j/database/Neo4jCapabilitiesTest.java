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
package com.google.cloud.teleport.v2.neo4j.database;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.neo4j.database.Neo4jCapabilities.Neo4jVersion;
import org.junit.Test;

public class Neo4jCapabilitiesTest {

  @Test
  public void parses_kernel_version() {
    assertThat(Neo4jVersion.of("4.4")).isEqualTo(new Neo4jVersion(4, 4));
    assertThat(Neo4jVersion.of("4.4-aura")).isEqualTo(new Neo4jVersion(4, 4));
    assertThat(Neo4jVersion.of("4.4.13")).isEqualTo(new Neo4jVersion(4, 4, 13));
    assertThat(Neo4jVersion.of("2025.01")).isEqualTo(new Neo4jVersion(2025, 1));
    assertThat(Neo4jVersion.of("2025.01.0")).isEqualTo(new Neo4jVersion(2025, 1, 0));
    assertThat(Neo4jVersion.of("2025.01-aura")).isEqualTo(new Neo4jVersion(2025, 1));
    assertThat(Neo4jVersion.of("2025.01.0-21379")).isEqualTo(new Neo4jVersion(2025, 1, 0));
  }

  @Test
  public void rejects_invalid_kernel_version() {
    assertThrows(IllegalArgumentException.class, () -> Neo4jVersion.of(""));
    assertThrows(IllegalArgumentException.class, () -> Neo4jVersion.of("5"));
    assertThrows(IllegalArgumentException.class, () -> Neo4jVersion.of("5."));
    assertThrows(IllegalArgumentException.class, () -> Neo4jVersion.of("2025.1."));
    assertThrows(IllegalArgumentException.class, () -> Neo4jVersion.of("5.5.3.1"));
    assertThrows(NumberFormatException.class, () -> Neo4jVersion.of(".."));
    assertThrows(NumberFormatException.class, () -> Neo4jVersion.of("5..3"));
    assertThrows(NumberFormatException.class, () -> Neo4jVersion.of(".2025.6"));
  }
}
