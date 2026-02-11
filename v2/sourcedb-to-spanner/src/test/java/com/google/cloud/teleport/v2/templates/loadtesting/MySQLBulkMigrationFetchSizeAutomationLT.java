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
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.templates.SourceDbToSpanner;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(TemplateLoadTest.class)
@TemplateLoadTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class MySQLBulkMigrationFetchSizeAutomationLT extends SourceDbToSpannerLTBase {

  private static final String WORKER_MACHINE_TYPE = "e2-highcpu-4";
  private static final String LAUNCHER_MACHINE_TYPE = "n1-highmem-8";
  // The automated pipeline uses "smart" defaults which we want to test against.
  // Explicitly setting fetchSize=0 disables cursor/streaming and tries to fetch
  // all rows.
  private static final String FETCH_SIZE_ALL = "0";

  private String host;
  private int port;
  private String username;
  private String password;
  private String database;

  @Before
  public void setupEnvironment() throws IOException {
    username = "test";
    password = "Mysql@123";
    host = "34.72.121.70";
    port = 3306;
    database = "test_db";

    setUp(SQLDialect.MYSQL, host, port, username, password, database);
    createSpannerDDL("SourceDbToSpannerLT/WideRow/spanner-schema-50kb-at-source-row.sql");
  }

  @Test
  public void testPassWithAutomation() throws IOException, ParseException, InterruptedException {
    Map<String, Integer> expectedCountPerTable = new HashMap<>();
    expectedCountPerTable.put("users_heavy_beam", 100000);

    Map<String, String> params = new HashMap<>();
    params.put("workerMachineType", WORKER_MACHINE_TYPE);

    Map<String, String> env = new HashMap<>();
    env.put("launcherMachineType", LAUNCHER_MACHINE_TYPE);

    runLoadTest(expectedCountPerTable, params, env);
  }
}
