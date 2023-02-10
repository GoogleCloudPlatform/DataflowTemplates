/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants;
import com.google.cloud.teleport.v2.templates.session.NameAndCols;
import com.google.cloud.teleport.v2.templates.session.Session;
import com.google.cloud.teleport.v2.templates.session.SyntheticPKey;
import java.io.IOException;
import java.util.HashMap;
import org.apache.beam.sdk.io.gcp.spanner.ExposedSpannerConfig;
import org.json.JSONObject;
import org.junit.Test;

/** Unit tests SpannerTransactionWriterDoFn class. */
public class SpannerTransactionWriterDoFnTest {
  public static JsonNode parseChangeEvent(String json) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
      return mapper.readTree(json);
    } catch (IOException e) {
      // No action. Return null.
    }
    return null;
  }

  @Test
  public void transformChangeEventNamesTest() {
    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(ExposedSpannerConfig.create(), null, null, "", "");
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("first_name", "A");
    changeEvent.put("last_name", "B");
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    JsonNode ce = parseChangeEvent(changeEvent.toString());

    HashMap<String, NameAndCols> toSpanner = new HashMap<String, NameAndCols>();
    HashMap<String, String> cols = new HashMap<String, String>();
    cols.put("first_name", "first_name_new");
    cols.put("last_name", "last_name_new");
    toSpanner.put("Users", new NameAndCols("Users_new", cols));
    Session session = new Session(null, toSpanner);
    JsonNode actualEvent = spannerTransactionWriterDoFn.transformChangeEvent(ce, session);

    JSONObject changeEventNew = new JSONObject();
    changeEventNew.put("first_name_new", "A");
    changeEventNew.put("last_name_new", "B");
    changeEventNew.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users_new");
    JsonNode expectedEvent = parseChangeEvent(changeEventNew.toString());
    assertEquals(expectedEvent, actualEvent);
  }

  @Test
  public void transformChangeEventSynthPKTest() {
    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(ExposedSpannerConfig.create(), null, null, "", "");
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("first_name", "A");
    changeEvent.put("last_name", "B");
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    changeEvent.put(DatastreamConstants.EVENT_UUID_KEY, "abc-123");
    JsonNode ce = parseChangeEvent(changeEvent.toString());

    HashMap<String, SyntheticPKey> syntheticPKeys = new HashMap<String, SyntheticPKey>();
    syntheticPKeys.put("Users_new", new SyntheticPKey("synth_id", 0));
    HashMap<String, NameAndCols> toSpanner = new HashMap<String, NameAndCols>();
    HashMap<String, String> cols = new HashMap<String, String>();
    cols.put("first_name", "first_name_new");
    cols.put("last_name", "last_name_new");
    toSpanner.put("Users", new NameAndCols("Users_new", cols));
    Session session = new Session(syntheticPKeys, toSpanner);
    JsonNode actualEvent = spannerTransactionWriterDoFn.transformChangeEvent(ce, session);

    JSONObject changeEventNew = new JSONObject();
    changeEventNew.put("first_name_new", "A");
    changeEventNew.put("last_name_new", "B");
    changeEventNew.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users_new");
    changeEventNew.put(DatastreamConstants.EVENT_UUID_KEY, "abc-123");
    changeEventNew.put("synth_id", "abc-123");
    JsonNode expectedEvent = parseChangeEvent(changeEventNew.toString());
    assertEquals(expectedEvent, actualEvent);
  }

  @Test
  public void transformChangeEventEmptySessionTest() {
    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(ExposedSpannerConfig.create(), null, null, "", "");
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("first_name", "A");
    changeEvent.put("last_name", "B");
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    JsonNode ce = parseChangeEvent(changeEvent.toString());

    Session session = new Session();
    JsonNode actualEvent = spannerTransactionWriterDoFn.transformChangeEvent(ce, session);

    assertEquals(ce, actualEvent);
  }
}
