/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.utils;

import static com.google.cloud.teleport.v2.templates.constants.TestConstants.colFamily;
import static com.google.cloud.teleport.v2.templates.constants.TestConstants.colFamily2;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

/** Hbase-related convenience functions. */
public class HbaseUtils {

  public static String getCell(Table table, String rowKey, String colFamily, String colQualifier)
      throws IOException {

    return new String(
        getRowResult(table, rowKey).getValue(colFamily.getBytes(), colQualifier.getBytes()));
  }

  public static Result getRowResult(Table table, String rowKey) throws IOException {
    return table.get(new Get(rowKey.getBytes()));
  }

  public static Table createTable(HBaseTestingUtility hbaseTestingUtil) throws IOException {
    return createTable(hbaseTestingUtil, UUID.randomUUID().toString());
  }

  public static Table createTable(HBaseTestingUtility hbaseTestingUtil, String name)
      throws IOException {
    TableName tableName = TableName.valueOf(name);
    return hbaseTestingUtil.createTable(tableName, new String[] {colFamily, colFamily2});
  }
}
