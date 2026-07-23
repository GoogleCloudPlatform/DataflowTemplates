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
package com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import java.sql.SQLException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class verifying default behaviors of {@link UniformSplitterDBAdapter}. */
@RunWith(MockitoJUnitRunner.class)
public class UniformSplitterDBAdapterTest {

  @Test
  public void testApproximateCountDefaultMethods() {
    UniformSplitterDBAdapter adapter =
        new UniformSplitterDBAdapter() {
          @Override
          public String getReadQuery(String tableName, ImmutableList<String> partitionColumns) {
            return "SELECT *";
          }

          @Override
          public String getCountQuery(
              String tableName, ImmutableList<String> partitionColumns, long timeoutMillis) {
            return "SELECT COUNT(*)";
          }

          @Override
          public String getBoundaryQuery(
              String tableName, ImmutableList<String> partitionColumns, String colName) {
            return "SELECT MIN, MAX";
          }

          @Override
          public boolean checkForTimeout(SQLException exception) {
            return false;
          }

          @Override
          public String getCollationsOrderQuery(
              String dbCharset, String dbCollation, boolean padSpace) {
            return "SELECT CHARS";
          }
        };

    assertThat(adapter.supportsApproximateCounts()).isFalse();
    assertThrows(
        UnsupportedOperationException.class,
        () -> adapter.getApproximateCountQuery("testTable", ImmutableList.of("id")));
    assertThrows(UnsupportedOperationException.class, () -> adapter.parseApproximateCount(null));
  }
}
