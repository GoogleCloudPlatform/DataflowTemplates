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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.schemautils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.OversizedValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Instant;

/** Tests for {@link OversizedValueDlqSanitizer}. */
@RunWith(JUnit4.class)
public class OversizedValueDlqSanitizerTest {

  @Test
  public void jsonSchemaMatchesExpectedKeysAndTypes() throws Exception {
    OversizedValue value =
        new OversizedValue(
            "row-1",
            Instant.ofEpochSecond(1_700_000_000L, 123_456_789L),
            "cf",
            "proto_col",
            100L,
            50_000L,
            10_000L,
            "inst-a",
            "cluster-b",
            "tbl-c");

    OversizedValueDlqSanitizer sanitizer = new OversizedValueDlqSanitizer();
    String envelopeJson = sanitizer.apply(value);

    ObjectMapper om = new ObjectMapper();
    JsonNode envelope = om.readTree(envelopeJson);
    JsonNode message = envelope.get("message");
    assertEquals("row-1", message.get("row_key").asText());
    // ISO-8601 UTC with nanosecond precision, e.g. 2023-11-14T22:13:20.123456789Z.
    String commitTs = message.get("commit_timestamp").asText();
    assertTrue(
        "unexpected commit_timestamp format: " + commitTs,
        commitTs.startsWith("2023-11-14T22:13:20") && commitTs.endsWith("Z"));
    assertTrue(commitTs.contains(".123456789"));
    assertEquals("cf", message.get("column_family").asText());
    assertEquals("proto_col", message.get("column").asText());
    assertEquals(100L, message.get("raw_bytes").asLong());
    assertEquals(50_000L, message.get("estimated_decoded_bytes").asLong());
    assertEquals(10_000L, message.get("max_bytes").asLong());
    assertEquals("decoded_value_exceeds_max_bytes", message.get("reason").asText());
    assertEquals("inst-a", message.get("source_instance").asText());
    assertEquals("cluster-b", message.get("source_cluster").asText());
    assertEquals("tbl-c", message.get("source_table").asText());

    assertEquals("decoded_value_exceeds_max_bytes", envelope.get("error_message").asText());
  }

  @Test
  public void nullCommitTimestampOmittedByDefaultGson() throws Exception {
    OversizedValue value =
        new OversizedValue("row-2", null, "cf", "col", 10L, 20L, 5L, "inst", "cluster", "tbl");
    String envelopeJson = new OversizedValueDlqSanitizer().apply(value);
    ObjectMapper om = new ObjectMapper();
    JsonNode message = om.readTree(envelopeJson).get("message");
    // Default Gson drops null fields — assert the key is absent rather than explicitly null.
    assertTrue(
        "commit_timestamp should not be present when the input instant is null",
        !message.has("commit_timestamp") || message.get("commit_timestamp").isNull());
    assertEquals("row-2", message.get("row_key").asText());
  }
}
