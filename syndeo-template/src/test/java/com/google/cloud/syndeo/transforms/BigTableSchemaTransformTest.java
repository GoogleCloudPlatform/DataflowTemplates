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
package com.google.cloud.syndeo.transforms;

import static org.junit.Assert.assertEquals;

import com.google.cloud.syndeo.SyndeoTemplate;
import com.google.cloud.syndeo.common.ProviderUtil;
import com.google.cloud.syndeo.transforms.bigquery.BigQuerySyndeoServices;
import com.google.cloud.syndeo.transforms.bigtable.BigTableWriteSchemaTransformConfiguration;
import com.google.cloud.syndeo.v1.SyndeoV1;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Random;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.BigtableEmulatorContainer;
import org.testcontainers.utility.DockerImageName;

@RunWith(JUnit4.class)
public class BigTableSchemaTransformTest {

  private static final Random RND = new Random();

  private static final Schema INPUT_SCHEMA =
      Schema.builder()
          .addStringField("name")
          .addBooleanField("vaccinated")
          .addDoubleField("height")
          .addFloatField("temperature")
          .addInt32Field("age")
          .addInt64Field("networth")
          .addDateTimeField("birthday")
          .build();

  public static String randomString(Integer length) {
    byte[] byteStr = new byte[length];
    RND.nextBytes(byteStr);
    return Base64.getEncoder().encodeToString(byteStr);
  }

  private static Row generateRow() {
    return Row.withSchema(INPUT_SCHEMA)
        .addValue(randomString(10))
        .addValue(RND.nextBoolean())
        .addValue(RND.nextDouble())
        .addValue(RND.nextFloat())
        .addValue(RND.nextInt())
        .addValue(RND.nextLong())
        .addValue(new DateTime(Math.abs(RND.nextLong()) % Instant.now().toEpochMilli()))
        .build();
  }

  public static void setUpBigQueryResources(BigQueryServices bqs, String bigQueryName) {
    PipelineOptions setupOptions = PipelineOptionsFactory.create();
    Pipeline setupP = Pipeline.create(setupOptions);

    BigQueryIO.Write<Row> bqWrite =
        BigQueryIO.<Row>write()
            .to(bigQueryName)
            .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .useBeamSchema();

    setupP
        .apply(
            Create.of(
                generateRow(),
                generateRow(),
                generateRow(),
                generateRow(),
                generateRow(),
                generateRow()))
        .setRowSchema(INPUT_SCHEMA)
        .apply(bqs == null ? bqWrite : bqWrite.withTestServices(bqs));

    setupP.run().waitUntilFinish();
  }

  @Rule
  public BigtableEmulatorContainer bigTableContainer =
      new BigtableEmulatorContainer(
          DockerImageName.parse("gcr.io/google.com/cloudsdktool/cloud-sdk:367.0.0-emulators"));

  @Test
  public void testBigTableSchemaTransformIsFound() {
    Collection<SchemaTransformProvider> providers = ProviderUtil.getProviders();
    assertEquals(
        1,
        providers.stream()
            .filter((provider) -> provider.identifier().equals("bigtable:write"))
            .count());
  }

  @Ignore(
      "Unable to run this test due to BigQuery exports from FakeJobService are not supporting AVRO schema")
  @Test
  public void testBigQueryToBigTableLocallyWithEmulators() throws Exception {
    String bigTableName = "anytable";
    String bigQueryName = "anyproject:anyds.anytable";

    FakeDatasetService.setUp();
    FakeJobService.setUp();
    FakeBigQueryServices bqs =
        new FakeBigQueryServices()
            .withDatasetService(new FakeDatasetService())
            .withJobService(new FakeJobService());
    bqs.getDatasetService(null).createDataset("anyproject", "anyds", "anyloc", null, null);
    setUpBigQueryResources(bqs, bigQueryName);
    // HACK: We use this global variable to override the BQ Services in the expanded BigQuery
    // SchemaTransform.

    BigQuerySyndeoServices.servicesVariable = bqs;

    try {
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
                              .setProjectId("anyproject")
                              .setInstanceId("anyinstance")
                              .setTableId(bigTableName)
                              .setKeyColumns(Arrays.asList("name", "birthday"))
                              .setEndpoint(bigTableContainer.getEmulatorEndpoint())
                              .build()
                              .toBeamRow()
                              .getValues())
                      .toProto())
              .build();
      PipelineOptions options = PipelineOptionsFactory.create();
      options.setTempLocation("any");
      SyndeoTemplate.run(options, description);
    } finally {
      BigQuerySyndeoServices.servicesVariable = null;
    }
  }
}
