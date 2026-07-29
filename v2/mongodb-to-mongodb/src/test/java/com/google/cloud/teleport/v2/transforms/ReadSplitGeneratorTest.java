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
package com.google.cloud.teleport.v2.transforms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.bson.BsonDocument;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link ReadSplitGenerator}. */
@RunWith(JUnit4.class)
public class ReadSplitGeneratorTest {

  @Test
  public void testGenerateIndexSliceFilters_singleSplit() {
    List<BsonDocument> filters = ReadSplitGenerator.generateIndexSliceFilters(1);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0).isEmpty());
  }

  @Test
  public void testGenerateIndexSliceFilters_zeroSplit() {
    List<BsonDocument> filters = ReadSplitGenerator.generateIndexSliceFilters(0);
    assertEquals(1, filters.size());
    assertTrue(filters.get(0).isEmpty());
  }

  @Test
  public void testGenerateIndexSliceFilters_multipleSplits() {
    List<BsonDocument> filters = ReadSplitGenerator.generateIndexSliceFilters(16);
    // 4 Integer/Long mod slices + 1 Double/Decimal + 4 String slices + 8 ObjectId slices + 1 BinData + 1 Catch-All = 19 slices
    assertNotNull(filters);
    assertFalse(filters.isEmpty());
    assertEquals(19, filters.size());

    // Verify all generated filters parse cleanly and contain expected BSON type selectors
    int numberModCount = 0;
    int numberFloatCount = 0;
    int stringCount = 0;
    int objectIdCount = 0;
    int binDataCount = 0;
    int catchAllCount = 0;

    for (BsonDocument filter : filters) {
      assertNotNull(filter);
      String json = filter.toJson();
      if (json.contains("\"$not\"")) {
        catchAllCount++;
      } else if (json.contains("\"$mod\"")) {
        numberModCount++;
      } else if (json.contains("\"double\"") && json.contains("\"decimal\"")) {
        numberFloatCount++;
      } else if (json.contains("\"$type\": \"string\"")) {
        stringCount++;
      } else if (json.contains("\"$oid\"")) {
        objectIdCount++;
      } else if (json.contains("\"binData\"")) {
        binDataCount++;
      }
    }

    assertEquals(4, numberModCount);
    assertEquals(1, numberFloatCount);
    assertEquals(4, stringCount);
    assertEquals(8, objectIdCount);
    assertEquals(1, binDataCount);
    assertEquals(1, catchAllCount);
  }
}
