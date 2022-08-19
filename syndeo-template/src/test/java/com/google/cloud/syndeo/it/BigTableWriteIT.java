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
package com.google.cloud.syndeo.it;

import com.google.api.gax.rpc.ServerStream;
import com.google.api.services.bigquery.Bigquery;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.syndeo.SyndeoTemplate;
import com.google.cloud.syndeo.common.ProviderUtil;
import com.google.cloud.syndeo.transforms.BigTableSchemaTransformTest;
import com.google.cloud.syndeo.transforms.bigtable.BigTableWriteSchemaTransformConfiguration;
import com.google.cloud.syndeo.v1.SyndeoV1;
import com.google.cloud.teleport.it.TestProperties;
import java.io.IOException;
import java.util.Arrays;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigTableWriteIT {

  @Rule public final TestName testName = new TestName();

  private static final String PROJECT = TestProperties.project();
  private static final String TEMP_LOCATION = TestProperties.artifactBucket();

  @Test
  public void testBigQueryToBigTableSmallNonTemplateJob() throws IOException {
    String bigTableName = "syndeo-test-" + BigTableSchemaTransformTest.randomString(6);
    String bigQueryName = PROJECT + ":syndeo_dataset." + bigTableName;

    BigTableSchemaTransformTest.setUpBigQueryResources(null, bigQueryName);

    SyndeoV1.PipelineDescription description =
        SyndeoV1.PipelineDescription.newBuilder()
            .addTransforms(
                new ProviderUtil.TransformSpec(
                        "bigquery:read", Arrays.asList(bigQueryName, null, null, null))
                    .toProto())
            .addTransforms(
                new ProviderUtil.TransformSpec(
                        "bigtable:write",
                        BigTableWriteSchemaTransformConfiguration.builder()
                            .setProjectId(PROJECT)
                            .setInstanceId("teleport")
                            .setTableId(bigTableName)
                            .setKeyColumns(Arrays.asList("name", "birthday"))
                            .setEndpoint("")
                            .build()
                            .toBeamRow()
                            .getValues())
                    .toProto())
            .build();
    try {
      PipelineOptions options = PipelineOptionsFactory.create();
      options.setTempLocation(TEMP_LOCATION);

      SyndeoTemplate.run(options, description);
      try (BigtableDataClient btClient =
          BigtableDataClient.create(
              BigtableDataSettings.newBuilder()
                  .setProjectId(PROJECT)
                  .setInstanceId("teleport")
                  .build())) {
        ServerStream<Row> rowstream = btClient.readRows(Query.create(bigTableName));
        assert Lists.newArrayList(rowstream.iterator()).size() == 6;
      }

    } finally {
      Bigquery client = BigqueryClient.getNewBigqueryClient("SyndeoTests");
      client.tables().delete(PROJECT, "syndeo_dataset", bigTableName);
      try (BigtableTableAdminClient btClient =
          BigtableTableAdminClient.create(
              BigtableTableAdminSettings.newBuilder()
                  .setProjectId(PROJECT)
                  .setInstanceId("teleport")
                  .build())) {
        btClient.deleteTable(bigTableName);
      }
    }
  }
}
