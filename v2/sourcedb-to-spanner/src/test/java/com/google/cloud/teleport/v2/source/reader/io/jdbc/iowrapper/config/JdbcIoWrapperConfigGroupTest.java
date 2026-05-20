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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link JdbcIoWrapperConfigGroup}. */
@RunWith(JUnit4.class)
public class JdbcIoWrapperConfigGroupTest {

  /**
   * Tests that the builder correctly initializes the group and that equality and hashing work as
   * expected.
   */
  @Test
  public void testBuilderAndEquality() {
    JdbcIOWrapperConfig mockConfig1 = mock(JdbcIOWrapperConfig.class);
    JdbcIOWrapperConfig mockConfig2 = mock(JdbcIOWrapperConfig.class);
    when(mockConfig1.sourceDbDialect()).thenReturn(SQLDialect.MYSQL);
    when(mockConfig2.sourceDbDialect()).thenReturn(SQLDialect.MYSQL);

    JdbcIoWrapperConfigGroup group1 =
        JdbcIoWrapperConfigGroup.builder()
            .setSourceDbDialect(SQLDialect.MYSQL)
            .setShardConfigs(ImmutableList.of(mockConfig1, mockConfig2))
            .build();

    JdbcIoWrapperConfigGroup group2 =
        JdbcIoWrapperConfigGroup.builder()
            .setSourceDbDialect(SQLDialect.MYSQL)
            .setShardConfigs(ImmutableList.of(mockConfig1, mockConfig2))
            .build();

    assertThat(group1).isEqualTo(group2);
    assertThat(group1.hashCode()).isEqualTo(group2.hashCode());
    assertThat(group1.shardConfigs()).hasSize(2);
    assertThat(group1.sourceDbDialect()).isEqualTo(SQLDialect.MYSQL);
  }

  /**
   * Tests that attempting to create a group with mixed SQL dialects throws an {@link
   * IllegalArgumentException}.
   */
  @Test
  public void testMixedDialectMigrationsNotSupported() {
    JdbcIOWrapperConfig mockConfig1 = mock(JdbcIOWrapperConfig.class);
    JdbcIOWrapperConfig mockConfig2 = mock(JdbcIOWrapperConfig.class);
    when(mockConfig1.sourceDbDialect()).thenReturn(SQLDialect.MYSQL);
    when(mockConfig2.sourceDbDialect()).thenReturn(SQLDialect.POSTGRESQL);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            JdbcIoWrapperConfigGroup.builder()
                .addShardConfig(mockConfig1)
                .addShardConfig(mockConfig2)
                .build());
  }

  /**
   * Tests that the dialect is automatically set based on the first shard configuration added to the
   * builder.
   */
  @Test
  public void testAddShardConfig_dialectAutoSet() {
    JdbcIOWrapperConfig mockConfig = mock(JdbcIOWrapperConfig.class);
    JdbcIoWrapperConfigGroup.Builder builder = JdbcIoWrapperConfigGroup.builder();

    when(mockConfig.sourceDbDialect()).thenReturn(SQLDialect.POSTGRESQL);
    builder.setSourceDbDialect(SQLDialect.POSTGRESQL).addShardConfig(mockConfig);

    JdbcIoWrapperConfigGroup group = builder.build();
    assertThat(group.shardConfigs()).containsExactly(mockConfig);
    assertThat(
            JdbcIoWrapperConfigGroup.builder().addShardConfig(mockConfig).build().sourceDbDialect())
        .isEqualTo(SQLDialect.POSTGRESQL);
  }

  /** Tests that the group can be built with an empty list of shard configurations. */
  @Test
  public void testEmptyConfigs() {
    JdbcIoWrapperConfigGroup group =
        JdbcIoWrapperConfigGroup.builder()
            .setShardConfigs(ImmutableList.of())
            .setSourceDbDialect(SQLDialect.MYSQL)
            .build();

    assertThat(group.shardConfigs()).isEmpty();
  }
}
