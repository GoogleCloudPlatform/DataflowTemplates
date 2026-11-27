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

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Type;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.TrackedSpannerColumn;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SpannerChangeStreamsUtils} null primary-key handling. */
@RunWith(JUnit4.class)
public final class SpannerChangeStreamsUtilsTest {

  @Test
  public void testAppendNullKeyComponentsMatchesTypedNullKey() {
    JSONObject keysJsonObject = new JSONObject("{\"Col1\":\"123\",\"Col2\":null}");
    Key.Builder keyBuilder = Key.newBuilder();
    SpannerChangeStreamsUtils.appendToSpannerKey(
        TrackedSpannerColumn.create("Col1", Type.string(), 1, 1), keysJsonObject, keyBuilder);
    SpannerChangeStreamsUtils.appendToSpannerKey(
        TrackedSpannerColumn.create("Col2", Type.string(), 2, 2), keysJsonObject, keyBuilder);

    assertThat(keyBuilder.build())
        .isEqualTo(Key.newBuilder().append("123").append((String) null).build());
  }
}
