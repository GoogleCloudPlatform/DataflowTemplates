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

import com.google.cloud.spanner.Mutation;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerDataProvider {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerDataProvider.class);
  public static final String AUTHORS_TABLE = "Authors";
  public static final String BOOKS_TABLE = "Books";

  public static boolean writeRowsInSpanner(
      Integer startId, Integer endId, SpannerResourceManager spannerResourceManager) {

    boolean success = true;
    List<Mutation> mutations = new ArrayList<>();
    // Insert Authors
    for (int i = startId; i <= endId; i++) {
      Mutation m =
          Mutation.newInsertOrUpdateBuilder(AUTHORS_TABLE)
              .set("author_id")
              .to(i)
              .set("name")
              .to("author_name_" + i)
              .build();
      mutations.add(m);
    }
    spannerResourceManager.write(mutations);
    LOG.info(String.format("Wrote %d rows to table %s", mutations.size(), AUTHORS_TABLE));

    mutations = new ArrayList<>();
    // Insert Books
    for (int i = startId; i <= endId; i++) {
      Mutation m =
          Mutation.newInsertOrUpdateBuilder(BOOKS_TABLE)
              .set("author_id")
              .to(i)
              .set("book_id")
              .to(i)
              .set("name")
              .to("book_name_" + i)
              .build();
      mutations.add(m);
    }
    spannerResourceManager.write(mutations);
    LOG.info(String.format("Wrote %d rows to table %s", mutations.size(), BOOKS_TABLE));

    return success;
  }
}
