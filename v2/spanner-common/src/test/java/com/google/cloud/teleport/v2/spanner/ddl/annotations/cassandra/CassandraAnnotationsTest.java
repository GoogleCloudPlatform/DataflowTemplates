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
package com.google.cloud.teleport.v2.spanner.ddl.annotations.cassandra;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.spanner.ddl.annotations.cassandra.CassandraType.Kind;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CassandraAnnotationsTest {

  @Test
  public void testCassandraAnnotationsBasic() {
    assertThat(
            CassandraAnnotations.fromColumnOptions(ImmutableList.of(), "testCol")
                .cassandraType()
                .getKind())
        .isEqualTo(Kind.NONE);

    assertThat(
            CassandraAnnotations.fromColumnOptions(
                    ImmutableList.of("cassandra_type='ascii'"), "testCol")
                .cassandraType()
                .getKind())
        .isEqualTo(Kind.PRIMITIVE);

    assertThat(
            CassandraAnnotations.fromColumnOptions(
                    ImmutableList.of("cassandra_type=\"list<time>\""), "testCol")
                .cassandraType()
                .getKind())
        .isEqualTo(Kind.LIST);

    assertThat(
            CassandraAnnotations.fromColumnOptions(
                    ImmutableList.of("cassandra_type='set<time>'"), "testCol")
                .cassandraType()
                .getKind())
        .isEqualTo(Kind.SET);

    assertThat(
            CassandraAnnotations.fromColumnOptions(
                    ImmutableList.of("cassandra_type='map<time, time>'"), "testCol")
                .cassandraType()
                .getKind())
        .isEqualTo(Kind.MAP);
  }
}
