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
package com.google.cloud.teleport.v2.source.reader.io.schema;

import static com.google.common.truth.Truth.assertThat;

import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link SourceSchemaReference}. */
@RunWith(MockitoJUnitRunner.class)
public class SourceSchemaReferenceTest extends TestCase {

  @Test
  public void testDbNameWithNullNamespaceBuilds() {
    final String testDB = "testDb";
    SourceSchemaReference ref = SourceSchemaReference.builder().setDbName(testDB).build();
    assertThat(ref.namespace()).isNull();
    assertThat(ref.dbName()).isEqualTo(testDB);
    assertThat(ref.getName()).isEqualTo("Db." + testDB);
  }

  @Test
  public void testDbNameWithNamespaceBuilds() {
    final String testDB = "testDb";
    final String testNamespace = "testNamespace";
    SourceSchemaReference ref =
        SourceSchemaReference.builder().setDbName(testDB).setNamespace(testNamespace).build();
    assertThat(ref.dbName()).isEqualTo(testDB);
    assertThat(ref.namespace()).isEqualTo(testNamespace);
    assertThat(ref.getName()).isEqualTo("Db." + testDB + ".Namespace." + testNamespace);
  }

  @Test
  public void testNullDbNameThrowsIllegalStateException() {
    // As dbName is a required property, we expect "java.lang.IllegalStateException"
    Assert.assertThrows(
        java.lang.IllegalStateException.class, () -> SourceSchemaReference.builder().build());
  }
}
