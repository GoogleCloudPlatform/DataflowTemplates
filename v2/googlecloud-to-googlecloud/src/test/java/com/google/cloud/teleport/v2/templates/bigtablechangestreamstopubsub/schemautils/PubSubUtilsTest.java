/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.schemautils;

import com.google.cloud.teleport.bigtable.ChangelogEntry;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.PubSubDestination;
import com.google.cloud.teleport.v2.utils.BigtableSource;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link PubSubUtils}. */
@RunWith(JUnit4.class)
public class PubSubUtilsTest {
  static final Charset CHARSET = StandardCharsets.UTF_8;

  // Pipeline specific variables to be used throughout testing of PubSubUtils
  static final String FAKE_PROJECT_ID = "fakeproject";
  static final String FAKE_INSTANCE_ID = "fakeinstance";
  static final String FAKE_TABLE_ID = "faketableid";
  static final String FAKE_TOPIC = "faketopic";

  @Test
  public void testGetValidEntriesAllEntriesAreValid() {
    PubSubUtils utils = initPubSubUtils();

    List<ChangelogEntry> actualEntries = utils.mapChangeJsonStringToPubSubMessageAsJson("test");

    Assert.assertEquals(2, actualEntries.size());

    ChangelogEntry logEntry1 = actualEntries.get(0);
    ChangelogEntry logEntry2 = actualEntries.get(1);
    Assert.assertEquals(
        "rowkey", Charset.defaultCharset().decode(logEntry1.getRowKey()).toString());
    Assert.assertEquals("family1", logEntry1.getColumnFamily());
    Assert.assertEquals(
        "column1", Charset.defaultCharset().decode(logEntry1.getColumn()).toString());

    Assert.assertEquals(
        "rowkey", Charset.defaultCharset().decode(logEntry2.getRowKey()).toString());
    Assert.assertEquals("family2", logEntry2.getColumnFamily());
    Assert.assertEquals(
        "column2", Charset.defaultCharset().decode(logEntry2.getColumn()).toString());
  }

  private static ByteBuffer getByteBufferFromString(String s, Charset charset) {
    return ByteBuffer.wrap(s.getBytes(charset));
  }

  private PubSubUtils initPubSubUtils() {
    BigtableSource source =
        new BigtableSource(
            FAKE_INSTANCE_ID, FAKE_TABLE_ID, CHARSET.toString(), "", "", Instant.now());

    PubSubDestination destination =
        new PubSubDestination(
            FAKE_PROJECT_ID,
            FAKE_TOPIC,
            FAKE_TOPIC,
            "JSON",
            CHARSET.toString(),
            false,
            false,
            false,
            false);

    return new PubSubUtils(source, destination);
  }
}
