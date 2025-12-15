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
package com.google.cloud.teleport.v2.templates.dbutils.dao;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.teleport.v2.templates.dbutils.dao.spanner.SpannerDao;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public final class SpannerDaoTest {

  @Rule public final MockitoRule mocktio = MockitoJUnit.rule();
  @Mock private SpannerAccessor mockSpannerAccessor;

  @Mock private DatabaseClient mockDatabaseClient;

  @Mock private ReadOnlyTransaction mockReadOnlyTransaction;

  @Mock private TransactionContext mockReadWriteTransaction;

  @Before
  public void doBeforeEachTest() throws Exception {
    when(mockSpannerAccessor.getDatabaseClient()).thenReturn(mockDatabaseClient);
    when(mockDatabaseClient.singleUse()).thenReturn(mockReadOnlyTransaction);
    doNothing().when(mockSpannerAccessor).close();
  }

  @Test
  public void testUpdateShadowTable() {
    SpannerDao spannerDao = new SpannerDao(mockSpannerAccessor);
    Mutation mutation = Mutation.newInsertBuilder("T").set("C1").to("x").set("C2").to("y").build();

    spannerDao.updateShadowTable(mutation, mockReadWriteTransaction);
    verify(mockReadWriteTransaction).buffer(eq(mutation));
  }
}
