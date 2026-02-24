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
package com.google.cloud.teleport.v2.source.reader.io.transform;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableReference;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link SourceTableReferenceWithCount}. */
@RunWith(JUnit4.class)
public class SourceTableReferenceWithCountTest {

  @Test
  public void testApply() {
    SourceSchemaReference schemaReference =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("testDB").build());
    String tableName = "testTable";
    String tableUUID = UUID.randomUUID().toString();
    SourceTableReference tableReference =
        SourceTableReference.builder()
            .setSourceSchemaReference(schemaReference)
            .setSourceTableName(tableName)
            .setSourceTableSchemaUUID(tableUUID)
            .setRecordCount(0L)
            .build();

    SourceTableReferenceWithCount function = new SourceTableReferenceWithCount(tableReference);
    long recordCount = 42L;
    SourceTableReference result = function.apply(recordCount);

    assertThat(result.sourceSchemaReference()).isEqualTo(schemaReference);
    assertThat(result.sourceTableName()).isEqualTo(tableName);
    assertThat(result.sourceTableSchemaUUID()).isEqualTo(tableUUID);
    assertThat(result.recordCount()).isEqualTo(recordCount);
  }
}
