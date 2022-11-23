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
import com.google.cloud.teleport.v2.templates.session.ReadSessionFileTest;
import com.google.cloud.teleport.v2.templates.session.Session;
import java.io.IOException;
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
    changeEvent.put("product_id", "A");
    changeEvent.put("quantity", 1);
    changeEvent.put("user_id", "B");
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "cart");
    JsonNode ce = parseChangeEvent(changeEvent.toString());

    Session session = ReadSessionFileTest.getSessionObject();

    JsonNode actualEvent = spannerTransactionWriterDoFn.transformChangeEvent(ce, session);

    JSONObject changeEventNew = new JSONObject();
    changeEventNew.put("new_product_id", "A");
    changeEventNew.put("new_quantity", 1);
    changeEventNew.put("new_user_id", "B");
    changeEventNew.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "new_cart");
    JsonNode expectedEvent = parseChangeEvent(changeEventNew.toString());
    assertEquals(expectedEvent, actualEvent);
  }

  @Test
  public void transformChangeEventSynthPKTest() {
    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(ExposedSpannerConfig.create(), null, null, "", "");
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("name", "A");
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "people");
    changeEvent.put(DatastreamConstants.EVENT_UUID_KEY, "abc-123");
    JsonNode ce = parseChangeEvent(changeEvent.toString());

    Session session = ReadSessionFileTest.getSessionObject();
    JsonNode actualEvent = spannerTransactionWriterDoFn.transformChangeEvent(ce, session);

    JSONObject changeEventNew = new JSONObject();
    changeEventNew.put("new_name", "A");
    changeEventNew.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "new_people");
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
