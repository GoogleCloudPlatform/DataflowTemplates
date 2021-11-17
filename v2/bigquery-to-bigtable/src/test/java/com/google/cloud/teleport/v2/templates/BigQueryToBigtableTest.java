// /*
//  * Copyright (C) 2019 Google LLC
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License"); you may not
//  * use this file except in compliance with the License. You may obtain a copy of
//  * the License at
//  *
//  *   http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//  * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//  * License for the specific language governing permissions and limitations under
//  * the License.
//  */
package com.google.cloud.teleport.v2.templates;
//
// import static org.junit.Assert.assertEquals;
// import static org.mockito.Mockito.mock;
//
// import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
// import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions;
// import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
// import org.junit.Test;
// import org.junit.runner.RunWith;
// import org.junit.runners.JUnit4;
//
// /** Unit tests for {@link BigQueryToBigtable}. */
// @RunWith(JUnit4.class)
// public class BigQueryToBigtableTest {
//   private static final String TABLE = "fantasmic-999999:great_data.table";
//   private BigQueryStorageClient client = mock(BigQueryStorageClient.class);
//
//   /** Test {@link ReadSessionFactory} throws exception when invalid table reference is provided. */
//   @Test(expected = IllegalArgumentException.class)
//   public void testReadSessionFactoryBadTable() {
//     // Test input
//     final String badTableRef = "fantasmic-999999;great_data.table";
//     final TableReadOptions tableReadOptions = TableReadOptions.newBuilder().build();
//     ReadSessionFactory trsf = new ReadSessionFactory();
//     ReadSession trs = trsf.create(client, badTableRef, tableReadOptions);
//   }
// }
