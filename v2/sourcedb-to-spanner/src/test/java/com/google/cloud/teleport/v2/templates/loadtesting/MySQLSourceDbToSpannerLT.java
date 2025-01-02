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
public class MySQLSourceDbToSpannerLT extends SourceDbToSpannerLTBase {

  @Test
  public void mySQLToSpannerBulk1TBTest() throws IOException, ParseException, InterruptedException {
    String username =
        accessSecret(
            "projects/269744978479/secrets/nokill-sourcedb-mysql-to-spanner-cloudsql-username/versions/1");
    String password =
        accessSecret(
            "projects/269744978479/secrets/nokill-sourcedb-mysql-to-spanner-cloudsql-password/versions/1");
    String database = "3tables10cols";
    String host =
        accessSecret(
            "projects/269744978479/secrets/nokill-sourcedb-mysql-to-spanner-cloudsql-ip-address/versions/1");
    int port = 3306;

    setUp(SQLDialect.MYSQL, host, port, username, password, database);
    createSpannerDDL("SourceDbToSpannerLT/mysql-spanner-schema.sql");

    Map<String, Integer> expectedCountPerTable =
        new HashMap<>() {
          {
            put("table1", 4255685);
            put("table2", 10489500);
          }
        };

    runLoadTest(expectedCountPerTable);
  }
}
