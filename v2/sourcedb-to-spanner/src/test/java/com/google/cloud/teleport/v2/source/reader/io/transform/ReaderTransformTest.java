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
package com.google.cloud.teleport.v2.source.reader.io.transform;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link ReaderTransform}. */
@RunWith(MockitoJUnitRunner.class)
public class ReaderTransformTest {

  @Test
  public void testReaderTransformBuilds() {
    final String dbName = "testDB";
    final SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName(dbName).build());
    final long rowCountPerTable = 10;
    final long tableCount = 1;
    ReaderTransformTestUtils readerTransformTestUtils =
        new ReaderTransformTestUtils(rowCountPerTable, tableCount, sourceSchemaReference);
    var readerTransform = readerTransformTestUtils.getTestReaderTransform();
    assertThat(readerTransform.sourceRowTag()).isNotNull();
    assertThat(readerTransform.sourceTableReferenceTag()).isNotNull();
    assertThat(readerTransform.readTransform()).isInstanceOf(AccumulatingTableReader.class);
  }
}
