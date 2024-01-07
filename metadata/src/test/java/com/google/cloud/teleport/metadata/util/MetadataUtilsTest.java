/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.metadata.util;

import static com.google.cloud.teleport.metadata.util.MetadataUtils.BIGQUERY_TABLE_PATTERN;
import static com.google.cloud.teleport.metadata.util.MetadataUtils.bucketNameOnly;
import static com.google.cloud.teleport.metadata.util.MetadataUtils.getParameterNameFromMethod;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.regex.Pattern;
import org.junit.Test;

/** Class to unit test {@link MetadataUtils} functionality. */
public class MetadataUtilsTest {

  @Test
  public void testBucketNameValid() {
    assertThat(bucketNameOnly("dataflow-templates")).isEqualTo("dataflow-templates");
    assertThat(bucketNameOnly("gs://dataflow-templates")).isEqualTo("dataflow-templates");
    assertThat(bucketNameOnly("gs://dataflow-templates/")).isEqualTo("dataflow-templates");
  }

  @Test
  public void testBucketNameInvalid() {
    assertThrows(IllegalArgumentException.class, () -> bucketNameOnly("gs://templates/path"));
    assertThrows(IllegalArgumentException.class, () -> bucketNameOnly("/tmp/templates"));
    assertThrows(IllegalArgumentException.class, () -> bucketNameOnly("https://www.google.com/"));
  }

  @Test
  public void testGetParameterNameFromMethod() {
    assertThat(getParameterNameFromMethod("getName")).isEqualTo("name");
    assertThat(getParameterNameFromMethod("getShouldKnowMyName")).isEqualTo("shouldKnowMyName");
    assertThat(getParameterNameFromMethod("name")).isEqualTo("name");
    assertThat(getParameterNameFromMethod("isClassic")).isEqualTo("classic");
  }

  @Test
  public void testRegexesBigQueryValid() {
    Pattern pattern = Pattern.compile(BIGQUERY_TABLE_PATTERN);
    assertThat(pattern.matcher("myproject:mydataset.mytable").matches()).isTrue();
    assertThat(pattern.matcher("myproject.mydataset.mytable").matches()).isTrue();
    assertThat(pattern.matcher("myproject.mydataset.mytable_whatever").matches()).isTrue();
  }

  @Test
  public void testRegexesBigQueryInvalid() {
    Pattern pattern = Pattern.compile(BIGQUERY_TABLE_PATTERN);
    assertThat(pattern.matcher("myproject:mydataset").matches()).isFalse();
    assertThat(pattern.matcher("myproject.mydataset").matches()).isFalse();
    assertThat(pattern.matcher("myproject.mydataset_whatever").matches()).isFalse();
  }
}
