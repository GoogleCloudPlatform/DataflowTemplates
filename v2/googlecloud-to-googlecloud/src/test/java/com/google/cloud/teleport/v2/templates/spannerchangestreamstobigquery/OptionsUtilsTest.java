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
package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.options.SpannerChangeStreamsToBigQueryOptions;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils.OptionsUtils;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class OptionsUtilsTest {

  @Test
  public void testValidFullDatasetId() {
    String[] args =
        new String[] {
          "--project=span-cloud-testing", "--bigQueryDataset=span-cloud-testing.dataset"
        };
    SpannerChangeStreamsToBigQueryOptions options =
        PipelineOptionsFactory.fromArgs(args).as(SpannerChangeStreamsToBigQueryOptions.class);
    List<String> results = OptionsUtils.processBigQueryProjectAndDataset(options);
    assertThat(results.get(0)).isEqualTo("span-cloud-testing");
    assertThat(results.get(1)).isEqualTo("dataset");
  }

  @Test
  public void testValidDatasetName() {
    String[] args = new String[] {"--project=span-cloud-testing", "--bigQueryDataset=dataset"};
    SpannerChangeStreamsToBigQueryOptions options =
        PipelineOptionsFactory.fromArgs(args).as(SpannerChangeStreamsToBigQueryOptions.class);
    List<String> results = OptionsUtils.processBigQueryProjectAndDataset(options);
    assertThat(results.get(0)).isEqualTo("span-cloud-testing");
    assertThat(results.get(1)).isEqualTo("dataset");
  }

  @Test
  public void testValidFullDatasetIdNoBigQueryProjectId() {
    String[] args =
        new String[] {
          "--project=span-cloud-testing", "--bigQueryDataset=cloud-spanner-backups-loadtest.dataset"
        };
    SpannerChangeStreamsToBigQueryOptions options =
        PipelineOptionsFactory.fromArgs(args).as(SpannerChangeStreamsToBigQueryOptions.class);
    List<String> results = OptionsUtils.processBigQueryProjectAndDataset(options);
    assertThat(results.get(0)).isEqualTo("cloud-spanner-backups-loadtest");
    assertThat(results.get(1)).isEqualTo("dataset");
  }

  @Test
  public void testInvalidFullDatasetIdWithDiffBigQueryProjectId() {
    String[] args =
        new String[] {
          "--project=span-cloud-testing",
          "--bigQueryProjectId=span-cloud-testing",
          "--bigQueryDataset=cloud-spanner-backups-loadtest.dataset"
        };
    SpannerChangeStreamsToBigQueryOptions options =
        PipelineOptionsFactory.fromArgs(args).as(SpannerChangeStreamsToBigQueryOptions.class);
    assertThrows(
        IllegalArgumentException.class,
        () -> OptionsUtils.processBigQueryProjectAndDataset(options));
  }
}
