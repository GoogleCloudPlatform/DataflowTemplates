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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(TemplateLoadTest.class)
@TemplateLoadTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class PostgreSQLSourceDbToSpannerLT extends SourceDbToSpannerLTBase {

  @Test
  public void postgresToSpannerBulk100GBTest()
      throws IOException, ParseException, InterruptedException {
    String username =
        accessSecret(
            "projects/269744978479/secrets/nokill-sourcedb-postgresql-to-spanner-cloudsql-username/versions/1");
    String password =
        accessSecret(
            "projects/269744978479/secrets/nokill-sourcedb-postgresql-to-spanner-cloudsql-password/versions/1");
    String database =
        accessSecret(
            "projects/269744978479/secrets/nokill-sourcedb-postgresql-to-spanner-cloudsql-database/versions/1");
    String host =
        accessSecret(
            "projects/269744978479/secrets/nokill-sourcedb-postgresql-to-spanner-cloudsql-ip-address/versions/1");
    int port =
        Integer.parseInt(
            accessSecret(
                "projects/269744978479/secrets/nokill-sourcedb-postgresql-to-spanner-cloudsql-port/versions/1"));

    setUp(SQLDialect.POSTGRESQL, host, port, username, password, database);
    createSpannerDDL("SourceDbToSpannerLT/postgresql-spanner-schema.sql");

    Map<String, Integer> expectedCountPerTable =
        new HashMap<>() {
          {
            put("table1", 10489500);
            put("table2", 10497100);
            put("table3", 10510700);
            put("table4", 10454600);
            put("table5", 10493000);
            put("table6", 10449900);
            put("table7", 10515900);
            put("table8", 10437000);
            put("table9", 10491000);
            put("table10", 10511300);
          }
        };

    runLoadTest(expectedCountPerTable);
  }
}
