/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.templates.loadtesting;

import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance tests for {@link DataStreamToSpanner} DataStream to Spanner template. */
@Category(TemplateLoadTest.class)
@TemplateLoadTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpanner100GbLT extends DataStreamToSpannerLTBase {
  @Test
  public void backfill100Gb() throws IOException, ParseException, InterruptedException {
    setUpResourceManagers("DataStreamToSpanner100GbLT/spanner-schema.sql");
    HashMap<String, Integer> tables100GB = new HashMap<>();
    for (int i = 1; i <= 10; i++) {
      tables100GB.put("person" + i, 6500000);
    }
    runLoadTest(tables100GB);
  }
}
