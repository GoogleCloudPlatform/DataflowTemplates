/*
 * Copyright (C) 2026 Google LLC
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

import static com.google.common.truth.Truth.assertThat;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link RandomDdlGenerator}. */
@RunWith(JUnit4.class)
public final class RandomDdlGeneratorTest {

  @Test(timeout = 10_000)
  public void testGenerateDoesNotHangWhenShortIdentifiersExhausted() {
    // With maxIdLength=1 and >26 total tables/columns/indexes, all 26 case-insensitive
    // single-letter identifiers ("a".."z") are guaranteed to be exhausted. Verify that
    // generateIdentifier widens the identifier length instead of spinning forever.
    Ddl ddl =
        RandomDdlGenerator.builder()
            .setRandom(new Random(0L))
            .setMaxIdLength(1)
            .setMaxPkComponents(2)
            .setMaxBranchPerLevel(new int[] {5, 4, 3, 2, 2, 3, 3})
            .setMaxUdfs(3)
            .setMaxViews(3)
            .build()
            .generate();

    assertThat(ddl.allTables()).isNotEmpty();
    Set<String> tableNames = new HashSet<>();
    int totalColumns = 0;
    for (Table table : ddl.allTables()) {
      assertThat(tableNames.add(table.name().toLowerCase())).isTrue();
      totalColumns += table.columns().size();
    }
    assertThat(totalColumns).isGreaterThan(26);
  }

  @Test
  public void testGenerateIsDeterministicWithSeededRandom() {
    Ddl first =
        RandomDdlGenerator.builder()
            .setRandom(new Random(12345L))
            .setMaxBranchPerLevel(new int[] {5, 4, 3, 2, 2, 3, 3})
            .build()
            .generate();
    Ddl second =
        RandomDdlGenerator.builder()
            .setRandom(new Random(12345L))
            .setMaxBranchPerLevel(new int[] {5, 4, 3, 2, 2, 3, 3})
            .build()
            .generate();

    assertThat(first.prettyPrint()).isEqualTo(second.prettyPrint());
  }
}
