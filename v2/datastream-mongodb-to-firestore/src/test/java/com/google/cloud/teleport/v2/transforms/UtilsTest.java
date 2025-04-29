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
package com.google.cloud.teleport.v2.transforms;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext;
import java.util.HashSet;
import java.util.Set;
import org.bson.Document;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link Utils}. */
@RunWith(JUnit4.class)
public class UtilsTest {

  @Test
  public void testRemoveTableRowFields() {
    Document doc = new Document("field1", "value1").append("field2", 123).append("field3", true);
    Set<String> ignoreFields = new HashSet<>();
    ignoreFields.add("field1");
    ignoreFields.add("field3");

    Utils.removeTableRowFields(doc, ignoreFields);

    assertFalse(doc.containsKey("field1"));
    assertTrue(doc.containsKey("field2"));
    assertFalse(doc.containsKey("field3"));
  }

  @Test
  public void testRemoveTableRowFieldsEmptyIgnoreSet() {
    Document doc = new Document("field1", "value1").append("field2", 123);
    Set<String> ignoreFields = new HashSet<>();

    Utils.removeTableRowFields(doc, ignoreFields);

    assertTrue(doc.containsKey("field1"));
    assertTrue(doc.containsKey("field2"));
  }

  @Test
  public void testRemoveTableRowFieldsAllFieldsIgnored() {
    Document doc = new Document("field1", "value1").append("field2", 123);
    Set<String> ignoreFields = new HashSet<>();
    ignoreFields.add("field1");
    ignoreFields.add("field2");

    Utils.removeTableRowFields(doc, ignoreFields);

    assertFalse(doc.containsKey("field1"));
    assertFalse(doc.containsKey("field2"));
  }

  @Test
  public void testIsNewerTimestamp_ts1Newer() {
    Document ts1 =
        new Document(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL, 2L)
            .append(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL, 100);
    Document ts2 =
        new Document(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL, 1L)
            .append(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL, 200);

    assertTrue(Utils.isNewerTimestamp(ts1, ts2));
  }

  @Test
  public void testIsNewerTimestamp_ts2Newer() {
    Document ts1 =
        new Document(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL, 1L)
            .append(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL, 200);
    Document ts2 =
        new Document(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL, 2L)
            .append(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL, 100);

    assertFalse(Utils.isNewerTimestamp(ts1, ts2));
  }

  @Test
  public void testIsNewerTimestamp_sameSeconds_ts1Newer() {
    Document ts1 =
        new Document(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL, 1L)
            .append(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL, 200);
    Document ts2 =
        new Document(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL, 1L)
            .append(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL, 100);

    assertTrue(Utils.isNewerTimestamp(ts1, ts2));
  }

  @Test
  public void testIsNewerTimestamp_sameSeconds_ts2Newer() {
    Document ts1 =
        new Document(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL, 1L)
            .append(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL, 100);
    Document ts2 =
        new Document(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL, 1L)
            .append(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL, 200);

    assertFalse(Utils.isNewerTimestamp(ts1, ts2));
  }

  @Test
  public void testIsNewerTimestamp_sameTimestamp() {
    Document ts1 =
        new Document(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL, 1L)
            .append(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL, 100);
    Document ts2 =
        new Document(MongoDbChangeEventContext.TIMESTAMP_SECONDS_COL, 1L)
            .append(MongoDbChangeEventContext.TIMESTAMP_NANOS_COL, 100);

    assertFalse(Utils.isNewerTimestamp(ts1, ts2));
  }
}
