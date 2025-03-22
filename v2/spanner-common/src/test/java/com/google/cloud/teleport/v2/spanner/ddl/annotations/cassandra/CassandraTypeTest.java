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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CassandraTypeTest {
  @Test
  public void testCassandraTypeBasic() {
    CassandraType primitiveType = CassandraType.fromAnnotation("int");
    CassandraType listType = CassandraType.fromAnnotation("list<int>");
    CassandraType setType = CassandraType.fromAnnotation("set<int>");
    CassandraType mapType = CassandraType.fromAnnotation("map<int, time>");
    CassandraType nonType = CassandraType.fromAnnotation("");

    assertThat(primitiveType.getKind()).isEqualTo(Kind.PRIMITIVE);
    assertThat(primitiveType.primitive()).isEqualTo("INT");
    assertThat(listType.getKind()).isEqualTo(Kind.LIST);
    assertThat(listType.list().elementType()).isEqualTo("INT");
    assertThat(setType.getKind()).isEqualTo(Kind.SET);
    assertThat(setType.set().elementType()).isEqualTo("INT");
    assertThat(mapType.getKind()).isEqualTo(Kind.MAP);
    assertThat(mapType.map().keyType()).isEqualTo("INT");
    assertThat(mapType.map().valueType()).isEqualTo("TIME");
    assertThat(nonType.getKind()).isEqualTo(Kind.NONE);
    // Check that void method does not cause an exception, there's nothing more to assert.
    nonType.none();
  }
}
