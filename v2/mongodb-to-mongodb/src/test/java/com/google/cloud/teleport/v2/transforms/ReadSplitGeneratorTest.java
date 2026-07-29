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

import java.util.EnumSet;
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
    assertNotNull(filters);
    assertFalse(filters.isEmpty());
    assertEquals(16, filters.size());

    int numberModCount = 0;
    int stringCount = 0;
    int objectIdCount = 0;
    int catchAllCount = 0;

    for (BsonDocument filter : filters) {
      assertNotNull(filter);
      String json = filter.toJson();
      if (json.contains("\"$not\"")) {
        catchAllCount++;
      }
      if (json.contains("\"$mod\"")) {
        numberModCount++;
      }
      if (json.contains("\"$type\": \"string\"")) {
        stringCount++;
      }
      if (json.contains("\"$oid\"")) {
        objectIdCount++;
      }
    }

    assertEquals(16, numberModCount);
    assertEquals(16, stringCount);
    assertEquals(16, objectIdCount);
    assertEquals(1, catchAllCount);
  }

  @Test
  public void testGenerateIndexSliceFilters_stringOnly_noOrWrapper() {
    List<BsonDocument> filters =
        ReadSplitGenerator.generateIndexSliceFilters(
            4, EnumSet.of(ReadSplitGenerator.IdType.STRING));
    assertEquals(4, filters.size());
    for (BsonDocument filter : filters) {
      String json = filter.toJson();
      assertFalse("Single type filter should not contain $or", json.contains("\"$or\""));
      assertTrue("Should contain string type check", json.contains("\"$type\": \"string\""));
    }
  }

  @Test
  public void testGenerateIndexSliceFilters_objectIdOnly_noOrWrapper() {
    List<BsonDocument> filters =
        ReadSplitGenerator.generateIndexSliceFilters(
            4, EnumSet.of(ReadSplitGenerator.IdType.OBJECT_ID));
    assertEquals(4, filters.size());
    for (BsonDocument filter : filters) {
      String json = filter.toJson();
      assertFalse("Single type filter should not contain $or", json.contains("\"$or\""));
      assertTrue("Should contain $oid check", json.contains("\"$oid\""));
    }
  }

  @Test
  public void testGenerateIndexSliceFilters_numberOnly_noOrWrapper() {
    List<BsonDocument> filters =
        ReadSplitGenerator.generateIndexSliceFilters(
            4, EnumSet.of(ReadSplitGenerator.IdType.NUMBER));
    assertEquals(4, filters.size());
    for (BsonDocument filter : filters) {
      String json = filter.toJson();
      assertFalse("Single type filter should not contain $or", json.contains("\"$or\""));
      assertTrue("Should contain $mod check", json.contains("\"$mod\""));
    }
  }

  @Test
  public void testGenerateIndexSliceFilters_multipleTypes_usesOrWrapper() {
    List<BsonDocument> filters =
        ReadSplitGenerator.generateIndexSliceFilters(
            4, EnumSet.of(ReadSplitGenerator.IdType.STRING, ReadSplitGenerator.IdType.OBJECT_ID));
    assertEquals(4, filters.size());
    for (BsonDocument filter : filters) {
      String json = filter.toJson();
      assertTrue("Multiple type filter should contain $or", json.contains("\"$or\""));
    }
  }

  @Test
  public void testGenerateIndexSliceFilters_otherType_includedInSliceZeroOnly() {
    List<BsonDocument> filters =
        ReadSplitGenerator.generateIndexSliceFilters(
            4, EnumSet.of(ReadSplitGenerator.IdType.STRING, ReadSplitGenerator.IdType.OTHER));
    assertEquals(4, filters.size());
    assertTrue(filters.get(0).toJson().contains("\"$not\""));
    assertFalse(filters.get(1).toJson().contains("\"$not\""));
    assertFalse(filters.get(2).toJson().contains("\"$not\""));
    assertFalse(filters.get(3).toJson().contains("\"$not\""));
  }
}
