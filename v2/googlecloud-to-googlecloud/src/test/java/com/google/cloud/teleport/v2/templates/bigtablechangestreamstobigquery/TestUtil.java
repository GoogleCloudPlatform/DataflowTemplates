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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery;

public class TestUtil {
  public static final String TEST_PROJECT = "test-project";
  public static final String TEST_CBT_TABLE = "cbt_table";
  public static final String TEST_CBT_INSTANCE = "cbt-instance";
  public static final String TEST_BIG_QUERY_DATESET = "bq-dataset";
  public static final String TEST_BIG_QUERY_PROJECT = TEST_PROJECT;
  public static final String TEST_BIG_QUERY_TABLENAME = "bq_table";
  public static final String TEST_GOOD_COLUMN_FAMILY = "goodf";
  public static final String TEST_GOOD_COLUMN = "goodcol";
  public static final String TEST_GOOD_VALUE = "goodval";
  public static final String TEST_CBT_CLUSTER = "goodcluster";
  public static final Integer TEST_TIEBREAKER = 34;
  public static final long TEST_COMMIT_TIMESTAMP = 2898787L;
  public static final long TEST_TIMESTAMP = 231243214;
  public static final String TEST_ROWKEY = "some rowkey";
}
