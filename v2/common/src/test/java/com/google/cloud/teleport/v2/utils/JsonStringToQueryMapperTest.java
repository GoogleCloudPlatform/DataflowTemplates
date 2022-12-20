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
package com.google.cloud.teleport.v2.utils;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;

import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link JsonStringToQueryMapperTest}. */
@RunWith(JUnit4.class)
public class JsonStringToQueryMapperTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Mock private PreparedStatement query;

  @Test
  public void testMapJsonObjectToQuery() throws Exception {
    doNothing().when(query).setNull(1, Types.NULL);
    doNothing().when(query).setObject(isA(Integer.class), isA(String.class));

    JSONObject jsonObject = new JSONObject();
    jsonObject.put("null", JSONObject.NULL);
    jsonObject.put("name", "foo");
    jsonObject.put("num", 100);
    List<String> keys = new ArrayList<>();
    Collections.addAll(keys, "null", "name", "num");
    JsonStringToQueryMapper map = new JsonStringToQueryMapper(keys);
    map.setParameters(jsonObject.toString(), query);

    verify(query).setNull(1, Types.NULL);
    verify(query).setObject(2, "foo");
    verify(query).setObject(3, 100);
  }
}
