/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.cassandra.schema;

import static com.google.common.truth.Truth.assertThat;

import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link CassandraSchemaReference}. */
@RunWith(MockitoJUnitRunner.class)
public class CassandraSchemaReferenceTest extends TestCase {

  @Test
  public void testCassandraSchemaReferenceBasic() {
    String testKeySpace = "testKeySpace";
    CassandraSchemaReference ref =
        CassandraSchemaReference.builder().setKeyspaceName(testKeySpace).build();
    assertThat(ref.keyspaceName()).isEqualTo(testKeySpace);
    assertThat(ref.getName()).isEqualTo("KeySpace." + testKeySpace);
  }

  @Test
  public void testCassandraSchemaReferenceNullKeyspace() {
    CassandraSchemaReference ref = CassandraSchemaReference.builder().build();
    assertThat(ref.keyspaceName()).isNull();
    assertThat(ref.getName()).isEqualTo("KeySpace." + null);
  }
}
