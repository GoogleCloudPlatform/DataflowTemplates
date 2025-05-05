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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.schemautils;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation.MutationType;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.InvalidModException;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.MessageEncoding;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.MessageFormat;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.Mod;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.PubSubDestination;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.TestChangeStreamMutation;
import com.google.cloud.teleport.v2.utils.BigtableSource;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test class for {@link PubSubUtils}. */
@RunWith(JUnit4.class)
public class PubSubUtilsTest {
  static final Charset CHARSET = StandardCharsets.UTF_8;

  private static final Logger LOG = LoggerFactory.getLogger(PubSubUtilsTest.class);

  // Pipeline specific variables to be used throughout testing of PubSubUtils
  static final String FAKE_PROJECT_ID = "fakeproject";
  static final String FAKE_INSTANCE_ID = "fakeinstance";
  static final String FAKE_TABLE_ID = "faketableid";
  static final String FAKE_TOPIC = "faketopic";

  @Test
  public void testMapChangeJsonStringToPubSubMessageAsJsonValid() throws Exception {
    PubSubUtils utils = initPubSubUtils();

    SetCell setCell =
        SetCell.create(
            "test_column_family",
            ByteString.copyFrom("test_column", CHARSET),
            1000L, // timestamp
            ByteString.copyFrom("test_value", CHARSET));

    TestChangeStreamMutation mutation =
        new TestChangeStreamMutation(
            "test_rowkey",
            MutationType.USER,
            "source_cluster",
            java.time.Instant.now(), // commit timestamp
            1, // tiebreaker
            "token",
            java.time.Instant.now(), // low watermark
            setCell);

    Mod mod = new Mod(utils.getSource(), mutation, setCell);
    String json = mod.getChangeJson();
    PubsubMessage message = utils.mapChangeJsonStringToPubSubMessageAsJson(json);

    assertNotNull(message.getData());
  }

  @Test
  public void testMapChangeJsonStringToPubSubMessageAsJsonInvalid() throws Exception {
    PubSubUtils utils = initPubSubUtils();

    SetCell setCell =
        SetCell.create(
            "test_column_family",
            ByteString.copyFrom("test_column", CHARSET),
            1000L, // timestamp
            ByteString.copyFrom("test_value", CHARSET));

    TestChangeStreamMutation mutation =
        new TestChangeStreamMutation(
            "test_rowkey",
            MutationType.USER,
            "source_cluster",
            java.time.Instant.now(), // commit timestamp
            1, // tiebreaker
            "token",
            java.time.Instant.now(), // low watermark
            setCell);

    Mod mod = new Mod(utils.getSource(), mutation, setCell);
    String json = mod.getChangeJson();

    // Removing the VALUE_BYTES from the json makes it invalid. It should fail appropriately.
    String base64Value = Base64.getEncoder().encodeToString("test_value".getBytes());
    String invalidJson = json.replace(",\"VALUE_BYTES\":\"" + base64Value + "\"", "");

    assertThrows(
        InvalidModException.class,
        () -> {
          utils.mapChangeJsonStringToPubSubMessageAsJson(invalidJson);
        });
  }

  private PubSubUtils initPubSubUtils() {
    BigtableSource source =
        new BigtableSource(
            FAKE_INSTANCE_ID, FAKE_TABLE_ID, CHARSET.toString(), "", "", Instant.now());

    PubSubDestination destination =
        new PubSubDestination(
            FAKE_PROJECT_ID,
            FAKE_TOPIC,
            null,
            MessageFormat.JSON,
            MessageEncoding.JSON,
            false,
            false,
            false,
            false);

    return new PubSubUtils(source, destination);
  }
}
