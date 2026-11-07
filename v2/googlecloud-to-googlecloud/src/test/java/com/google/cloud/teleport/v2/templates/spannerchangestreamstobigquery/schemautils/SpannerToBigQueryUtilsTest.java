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
package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.spanner.Type;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.TrackedSpannerColumn;
import java.util.List;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SpannerToBigQueryUtils} null primary-key handling. */
@RunWith(JUnit4.class)
public final class SpannerToBigQueryUtilsTest {

  @Test
  public void testCompositeStringKeyWithExplicitJsonNull() {
    JSONObject keysJsonObject = new JSONObject("{\"Col1\":\"123\",\"Col2\":null}");
    List<TrackedSpannerColumn> spannerPkColumns =
        List.of(
            TrackedSpannerColumn.create("Col1", Type.string(), 1, 1),
            TrackedSpannerColumn.create("Col2", Type.string(), 2, 2));
    TableRow tableRow = new TableRow();

    SpannerToBigQueryUtils.addSpannerPkColumnsToTableRow(
        keysJsonObject, spannerPkColumns, tableRow);

    assertThat(tableRow.get("Col1")).isEqualTo("123");
    // JSONObject.NULL.equals(null) is true, so isEqualTo(null) would accept the sentinel.
    assertThat(tableRow.get("Col2")).isNull();
    assertThat(tableRow.get("_type_Col1")).isEqualTo("STRING");
    assertThat(tableRow.get("_type_Col2")).isEqualTo("STRING");
  }
}
