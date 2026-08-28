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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonString;
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

  @Test
  public void testDataDrivenSplits_mixedTypesAreIsolated() {
    MongoClient mockClient = mock(MongoClient.class);
    MongoDatabase mockDb = mock(MongoDatabase.class);
    @SuppressWarnings("unchecked")
    MongoCollection<BsonDocument> mockCol = mock(MongoCollection.class);

    when(mockClient.getDatabase(anyString())).thenReturn(mockDb);
    when(mockDb.getCollection(anyString(), eq(BsonDocument.class))).thenReturn(mockCol);

    // Mock detectIdTypes to return multiple types
    @SuppressWarnings("unchecked")
    FindIterable<BsonDocument> mockFind = mock(FindIterable.class);
    when(mockCol.find(any(BsonDocument.class))).thenReturn(mockFind);
    when(mockFind.limit(1)).thenReturn(mockFind);
    when(mockFind.first()).thenReturn(new BsonDocument()); // Meaning we detect active types

    // Mock $sample aggregation
    @SuppressWarnings("unchecked")
    AggregateIterable<BsonDocument> mockAgg =
        (AggregateIterable<BsonDocument>)
            Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {AggregateIterable.class},
                (proxy, method, args) -> {
                  if (method.getName().equals("iterator")) {
                    return new MongoCursor<BsonDocument>() {
                      Iterator<BsonDocument> iter =
                          Arrays.asList(
                                  new BsonDocument("_id", new BsonString("min")),
                                  new BsonDocument("_id", new BsonString("mid")),
                                  new BsonDocument("_id", new BsonString("max")))
                              .iterator();

                      @Override
                      public void close() {}

                      @Override
                      public boolean hasNext() {
                        return iter.hasNext();
                      }

                      @Override
                      public BsonDocument next() {
                        return iter.next();
                      }

                      @Override
                      public BsonDocument tryNext() {
                        return null;
                      }

                      @Override
                      public ServerCursor getServerCursor() {
                        return null;
                      }

                      @Override
                      public ServerAddress getServerAddress() {
                        return null;
                      }

                      @Override
                      public int available() {
                        return 0;
                      }
                    };
                  }
                  return null;
                });
    when(mockCol.aggregate(any())).thenReturn(mockAgg);

    List<BsonDocument> filters =
        ReadSplitGenerator.generateIndexSliceFilters(mockClient, "db", "col", 2);

    assertEquals(2, filters.size());
    String slice0 = filters.get(0).toJson();
    String slice1 = filters.get(1).toJson();

    // Validate we use the $or wrapper
    assertTrue(slice0.contains("\"$or\""));

    // Validate that the bounds are nested within specific type bounds!
    assertTrue(slice0.contains("\"$type\": \"string\""));
    assertTrue(slice0.contains("\"$type\": \"objectId\""));
    assertTrue(slice0.contains("\"$type\": [\"int\""));
  }
}
