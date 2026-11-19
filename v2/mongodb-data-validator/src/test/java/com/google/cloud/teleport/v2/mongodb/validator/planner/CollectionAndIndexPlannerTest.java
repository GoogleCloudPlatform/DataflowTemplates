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
package com.google.cloud.teleport.v2.mongodb.validator.planner;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.mongodb.validator.dto.MismatchType;
import com.google.cloud.teleport.v2.mongodb.validator.dto.MismatchedDocument;
import com.google.cloud.teleport.v2.mongodb.validator.dto.ValidationSlice.SliceMode;
import com.google.cloud.teleport.v2.mongodb.validator.planner.CollectionAndIndexPlanner.MetadataInspector;
import com.google.cloud.teleport.v2.mongodb.validator.planner.CollectionAndIndexPlanner.NormalizedIndexSpec;
import com.google.cloud.teleport.v2.mongodb.validator.planner.CollectionAndIndexPlanner.PlanResult;
import com.google.cloud.teleport.v2.mongodb.validator.planner.CollectionAndIndexPlanner.TypeBounds;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.bson.BsonInt64;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link CollectionAndIndexPlanner}. */
@RunWith(JUnit4.class)
public class CollectionAndIndexPlannerTest {

  @Test
  public void buildPlan_detectsMissingAndExtraCollections_indexDiffs_andBuildsGapFreeSlices() {
    MetadataInspector fakeInspector =
        new MetadataInspector() {
          @Override
          public Set<String> listUserCollections(boolean isTarget) {
            return isTarget
                ? ImmutableSet.of("users_v2", "orphan_target_col")
                : ImmutableSet.of("users", "missing_on_target_col");
          }

          @Override
          public List<NormalizedIndexSpec> listSecondaryIndexes(
              boolean isTarget, String collection) {
            if (!isTarget && "users".equals(collection)) {
              return ImmutableList.of(
                  new NormalizedIndexSpec("email_1", "{\"email\": 1}", true, false, null),
                  new NormalizedIndexSpec("ttl_1", "{\"createdAt\": 1}", false, false, 3600L));
            }
            if (isTarget && "users_v2".equals(collection)) {
              return ImmutableList.of(
                  // Unique flag lost on target -> INDEX_SPEC_MISMATCH
                  new NormalizedIndexSpec("email_idx", "{\"email\": 1}", false, false, null));
            }
            return Collections.emptyList();
          }

          @Override
          public long estimatedDocumentCount(boolean isTarget, String collection) {
            return 10_000L;
          }

          @Override
          @Nullable
          public TypeBounds probeTypeBounds(boolean isTarget, String collection, String typeName) {
            if ("number".equals(typeName)) {
              return new TypeBounds("number", new BsonInt64(1L), new BsonInt64(10_000L), 10_000L);
            }
            return null;
          }

          @Override
          public boolean hasOtherTypeDocuments(boolean isTarget, String collection) {
            return isTarget;
          }
        };

    PlanResult plan =
        CollectionAndIndexPlanner.buildPlan(
            fakeInspector,
            "src_db",
            "tgt_db",
            "",
            "users:users_v2",
            /* validateIndexes= */ true,
            /* targetChunkSize= */ 2500,
            /* maxSplitsPerCollection= */ 16,
            "run_01",
            Instant.parse("2026-11-16T00:00:00Z"));

    Set<MismatchType> mismatchTypes =
        plan.getCatalogAndIndexMismatches().stream()
            .map(MismatchedDocument::getMismatchType)
            .collect(Collectors.toSet());

    assertThat(mismatchTypes)
        .containsExactly(
            MismatchType.MISSING_COLLECTION_IN_TARGET,
            MismatchType.EXTRA_COLLECTION_IN_TARGET,
            MismatchType.INDEX_SPEC_MISMATCH,
            MismatchType.MISSING_INDEX_IN_TARGET);

    // 4 SORTED_MERGE slices for 10,000 number docs (@ chunk 2500) + 1 BATCH_LOOKUP slice for other
    assertThat(plan.getSlices()).hasSize(5);
    long sortedCount =
        plan.getSlices().stream().filter(s -> s.getSliceMode() == SliceMode.SORTED_MERGE).count();
    long lookupCount =
        plan.getSlices().stream().filter(s -> s.getSliceMode() == SliceMode.BATCH_LOOKUP).count();
    assertThat(sortedCount).isEqualTo(4);
    assertThat(lookupCount).isEqualTo(1);

    // Verify first slice has no $gte (covers -inf) and last sorted slice has no $lt (covers +inf)
    assertThat(plan.getSlices().get(0).getFilterJson()).doesNotContain("$gte");
    assertThat(plan.getSlices().get(0).getFilterJson()).contains("$lt");
    assertThat(plan.getSlices().get(3).getFilterJson()).contains("$gte");
    assertThat(plan.getSlices().get(3).getFilterJson()).doesNotContain("$lt");
  }
}
