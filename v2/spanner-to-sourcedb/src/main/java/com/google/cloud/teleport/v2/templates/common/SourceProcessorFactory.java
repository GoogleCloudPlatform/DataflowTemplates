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
package com.google.cloud.teleport.v2.templates.common;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.mysql.MySQLConnectionHelper;
import com.google.cloud.teleport.v2.templates.mysql.MySQLDMLGenerator;
import com.google.cloud.teleport.v2.templates.mysql.MySqlDao;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SourceProcessorFactory {
  public static IDMLGenerator getDMLGenerator(String source) throws Exception {
    if (source.equalsIgnoreCase(Constants.SOURCE_MYSQL)) {
      return new MySQLDMLGenerator();
    }
    throw new Exception("Invalid source type: " + source);
  }

  public static Map<String, ISourceDao> getSourceDaoMap(
      String source, List<Shard> shards, int maxConnections) throws Exception {
    if (source.equalsIgnoreCase(Constants.SOURCE_MYSQL)) {
      Map<String, ISourceDao> sourceDaoMap = new HashMap<>();
      MySQLConnectionHelper.init(shards, null, maxConnections);
      for (Shard shard : shards) {
        String sourceConnectionUrl =
            "jdbc:mysql://" + shard.getHost() + ":" + shard.getPort() + "/" + shard.getDbName();
        ISourceDao mySqlDao =
            new MySqlDao(sourceConnectionUrl, shard.getUserName(), shard.getPassword());
        sourceDaoMap.put(shard.getLogicalShardId(), mySqlDao);
      }
      return sourceDaoMap;
    }
    throw new Exception("Invalid source type: " + source);
  }
}
