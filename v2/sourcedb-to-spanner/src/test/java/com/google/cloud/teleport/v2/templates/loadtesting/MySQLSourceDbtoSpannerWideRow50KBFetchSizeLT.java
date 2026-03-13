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
package com.google.cloud.teleport.v2.templates.loadtesting;

import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.templates.SourceDbToSpanner;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(TemplateLoadTest.class)
@TemplateLoadTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
@Ignore("Waiting Dataflow release b/492014904")
public class MySQLSourceDbtoSpannerWideRow50KBFetchSizeLT extends SourceDbToSpannerLTBase {
  private static final String WORKER_MACHINE_TYPE = "n2-standard-4";
  private static final String LAUNCHER_MACHINE_TYPE = "n2-standard-4";

  @Test
  public void mySQLToSpannerWideRow50KBFetchSizeTest() throws Exception {

    String username =
        accessSecret("projects/269744978479/secrets/wide-row-50kb-username/versions/1");
    String password =
        accessSecret("projects/269744978479/secrets/wide-row-50kb-password/versions/1");
    String database = "50KBRow";
    String host = accessSecret("projects/269744978479/secrets/wide-row-50kb-host/versions/1");
    int port = 3306;

    setUp(SQLDialect.MYSQL, host, port, username, password, database);
    createSpannerDDL("SourceDbToSpannerLT/WideRow/spanner-schema-50kb-row.sql");

    Map<String, Integer> expectedCountPerTable =
        new HashMap<>() {
          {
            put("heavy_users", 1000000);
          }
        };

    Map<String, String> params =
        new HashMap<>() {
          {
            put("workerMachineType", WORKER_MACHINE_TYPE);
            put("numPartitions", "1");
          }
        };

    Map<String, String> env =
        new HashMap<>() {
          {
            put("launcherMachineType", LAUNCHER_MACHINE_TYPE);
          }
        };
    runLoadTest(expectedCountPerTable, params, env);
  }
}
