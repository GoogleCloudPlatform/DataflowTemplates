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
package com.google.cloud.teleport.v2.templates;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class SpannerLoggingIT {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerLoggingIT.class);

  @Test
  public void testLoggingPerformance() {
    List<String> statements = new ArrayList<>();
    for (int i = 0; i < 5000; i++) {
      statements.add("CREATE TABLE table_" + i + " (id INT64) PRIMARY KEY (id)");
    }
    LOG.info("START: Simulating massive log statement...");
    long startTime = System.currentTimeMillis();
    LOG.info("Simulating log: Executing DDL statements '{}' on database db", statements);
    long endTime = System.currentTimeMillis();
    LOG.info("END: Simulating massive log statement. Time taken: {} ms", (endTime - startTime));
  }
}
