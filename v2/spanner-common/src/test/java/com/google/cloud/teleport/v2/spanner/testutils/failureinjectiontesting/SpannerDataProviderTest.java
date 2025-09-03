/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.spanner.Mutation;
import java.util.List;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class SpannerDataProviderTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private SpannerResourceManager spannerResourceManager;

  @Test
  public void testWriteRowsInSpanner() {
    int startId = 1;
    int endId = 10;

    boolean result = SpannerDataProvider.writeRowsInSpanner(startId, endId, spannerResourceManager);

    assertTrue(result);

    ArgumentCaptor<List<Mutation>> mutationCaptor = ArgumentCaptor.forClass(List.class);
    verify(spannerResourceManager, times(2)).write(mutationCaptor.capture());

    List<List<Mutation>> capturedMutations = mutationCaptor.getAllValues();
    assertEquals(2, capturedMutations.size());

    // Verify Authors mutations
    List<Mutation> authorMutations = capturedMutations.get(0);
    assertEquals(10, authorMutations.size());
    assertEquals(SpannerDataProvider.AUTHORS_TABLE, authorMutations.get(0).getTable());
    assertEquals("author_name_1", authorMutations.get(0).asMap().get("name").getString());

    // Verify Books mutations
    List<Mutation> bookMutations = capturedMutations.get(1);
    assertEquals(10, bookMutations.size());
    assertEquals(SpannerDataProvider.BOOKS_TABLE, bookMutations.get(0).getTable());
    assertEquals("book_name_1", bookMutations.get(0).asMap().get("name").getString());
  }
}
