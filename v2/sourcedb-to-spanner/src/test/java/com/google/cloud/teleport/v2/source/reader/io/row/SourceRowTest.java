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
package com.google.cloud.teleport.v2.source.reader.io.row;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.source.reader.io.schema.SchemaTestUtils;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link SourceRow}. */
@RunWith(MockitoJUnitRunner.class)
public class SourceRowTest extends TestCase {
  @Test
  public void testSourceRowBuilds() {
    final String testTable = "testTable";
    final String shardId = "id1";
    final long testReadTime = 1712751118L;
    var schema = SchemaTestUtils.generateTestTableSchema(testTable);
    SourceRow sourceRow =
        SourceRow.builder(schema, shardId, testReadTime)
            .setField("firstName", "abc")
            .setField("lastName", "def")
            .build();

    assertThat(sourceRow.tableSchemaUUID()).isEqualTo(schema.tableSchemaUUID());
    assertThat(sourceRow.tableName()).isEqualTo(schema.tableName());
    assertThat(sourceRow.shardId()).isEqualTo(shardId);
    assertThat(sourceRow.getReadTimeMicros()).isEqualTo(testReadTime);
    assertThat(sourceRow.getPayload().get("firstName")).isEqualTo("abc");
    assertThat(sourceRow.getPayload().get("lastName")).isEqualTo("def");
  }

  @Test
  public void testSourceRowBuildWithInvalidFieldThrowsNPE() {
    final String testTable = "testTable";
    final long testReadTime = 1712751118L;
    var schema = SchemaTestUtils.generateTestTableSchema(testTable);

    Assert.assertThrows(
        java.lang.NullPointerException.class,
        () ->
            SourceRow.builder(schema, null, testReadTime)
                /* Invalid Field */
                .setField("middleName", "abc")
                .setField("lastName", "def")
                .build());
  }
}
