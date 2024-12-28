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

import com.google.cloud.teleport.v2.source.reader.io.jdbc.JdbcSchemaReference;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link SourceTableReference}. */
@RunWith(MockitoJUnitRunner.class)
public class SourceTableReferenceTest {
  @Test
  public void testSourceTableReferenceBuilds() {
    final String testDB = "testDb";
    final String testTable = "testTable";
    final String testTableUUID = "10a03145-47bd-4d6c-8168-3eab0f9f847b";
    SourceTableReference ref =
        SourceTableReference.builder()
            .setSourceSchemaReference(
                SourceSchemaReference.ofJdbc(
                    JdbcSchemaReference.builder().setDbName(testDB).build()))
            .setSourceTableName(testTable)
            .setSourceTableSchemaUUID(testTableUUID)
            .build();
    assertThat(ref.sourceSchemaReference().jdbc().namespace()).isNull();
    assertThat(ref.sourceSchemaReference().jdbc().dbName()).isEqualTo(testDB);
    assertThat(ref.sourceTableName()).isEqualTo(testTable);
    assertThat(ref.sourceTableSchemaUUID()).isEqualTo(testTableUUID);
    assertThat(ref.getName()).isEqualTo("Db.testDb.Table.testTable");
  }
}
