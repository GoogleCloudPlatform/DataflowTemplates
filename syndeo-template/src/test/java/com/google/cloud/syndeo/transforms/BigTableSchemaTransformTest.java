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

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.syndeo.SyndeoTemplate;
import com.google.cloud.syndeo.common.ProviderUtil;
import com.google.cloud.syndeo.transforms.bigquery.BigQuerySyndeoServices;
import com.google.cloud.syndeo.transforms.bigtable.BigTableWriteSchemaTransformConfiguration;
import com.google.cloud.syndeo.transforms.bigtable.BigTableWriteSchemaTransformProvider;
import com.google.cloud.syndeo.v1.SyndeoV1;
import java.time.Instant;
import java.util.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
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

  private static final Schema INTEGRATION_TEST_SCHEMA =
      Schema.builder()
          .addStringField("name")
          // The following fields cannot be included in local tests
          // due to limitations of the testing utilities.
          .addBooleanField("vaccinated")
          .addDoubleField("temperature")
          .addInt32Field("age")
          .addInt64Field("networth")
          .addDateTimeField("birthday")
          .build();

  // We need a different input schema for local tests because not all types are well supported for
  // the local FakeBigQueryServices utilities.
  private static final Schema LOCAL_TEST_INPUT_SCHEMA =
      Schema.builder()
          .addStringField("name")
          // .addBooleanField("vaccinated")
          // .addDoubleField("temperature")
          .build();

  public static String randomString(Integer length) {
    byte[] byteStr = new byte[length];
    RND.nextBytes(byteStr);
    return Base64.getEncoder().encodeToString(byteStr);
  }

  private static Row generateRow(boolean local) {
    if (local) {
      return Row.withSchema(LOCAL_TEST_INPUT_SCHEMA)
          .addValue(randomString(10))
          // .addValue(RND.nextBoolean())
          // .addValue(RND.nextDouble())
          .build();
    } else {
      return Row.withSchema(INTEGRATION_TEST_SCHEMA)
          .addValue(randomString(10))
          .addValue(RND.nextBoolean())
          .addValue(RND.nextDouble())
          .addValue(RND.nextInt())
          .addValue(RND.nextLong())
          .addValue(new DateTime(Math.abs(RND.nextLong()) % Instant.now().toEpochMilli()))
          .build();
    }
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
                generateRow(bqs != null),
                generateRow(bqs != null),
                generateRow(bqs != null),
                generateRow(bqs != null),
                generateRow(bqs != null),
                generateRow(bqs != null)))
        .setRowSchema(bqs != null ? LOCAL_TEST_INPUT_SCHEMA : INTEGRATION_TEST_SCHEMA)
        .apply(bqs == null ? bqWrite : bqWrite.withTestServices(bqs));

    setupP.run().waitUntilFinish();
  }

  @Rule
  public BigtableEmulatorContainer bigTableContainer =
      new BigtableEmulatorContainer(
          DockerImageName.parse("gcr.io/google.com/cloudsdktool/cloud-sdk:367.0.0-emulators"));

  @Test(expected = UnsupportedOperationException.class)
  public void testBigTableSchemaUnsupported() {
    PipelineOptions setupOptions = PipelineOptionsFactory.create();
    Pipeline setupP = Pipeline.create(setupOptions);

    PCollectionRowTuple.of(
            "input",
            setupP
                .apply(Create.of(generateRow(true)))
                .setRowSchema(
                    Schema.builder()
                        .addRowField(
                            "someRowField", Schema.builder().addInt64Field("someint64").build())
                        .build()))
        .apply(
            new BigTableWriteSchemaTransformProvider()
                .from(
                    BigTableWriteSchemaTransformConfiguration.builder()
                        .setProjectId("anyproject")
                        .setInstanceId("anyinstance")
                        .setTableId("anytable")
                        .setKeyColumns(Arrays.asList("name"))
                        .setEndpoint(bigTableContainer.getEmulatorEndpoint())
                        .build())
                .buildTransform());

    setupP.run().waitUntilFinish();
  }

  @Test
  public void testBigTableSchemaTransformIsFound() {
    Collection<SchemaTransformProvider> providers = ProviderUtil.getProviders();
    assertEquals(
        1,
        providers.stream()
            .filter(
                (provider) ->
                    provider
                        .identifier()
                        .equals("syndeo:schematransform:com.google.cloud:bigtable_write:v1"))
            .count());
  }

  @Ignore
  @Test
  public void testBigQueryToBigTableLocallyWithEmulators() throws Exception {
    String bigTableName = "anytable";
    String bigQueryName = "anyproject:anyds.anytable";

    FakeDatasetService.setUp();
    FakeJobService.setUp();
    FakeDatasetService dss = new FakeDatasetService();
    dss.createDataset("anyproject", "anyds", "anyloc", null, null);
    TableReference anytableRef =
        new TableReference()
            .setProjectId("anyproject")
            .setDatasetId("anyds")
            .setTableId("anytable");
    dss.createTable(
        new Table()
            .setTableReference(anytableRef)
            .setSchema(BigQueryUtils.toTableSchema(LOCAL_TEST_INPUT_SCHEMA)));
    List<TableRow> records =
        Lists.newArrayList(
            BigQueryUtils.toTableRow(generateRow(true)),
            BigQueryUtils.toTableRow(generateRow(true)),
            BigQueryUtils.toTableRow(generateRow(true)),
            BigQueryUtils.toTableRow(generateRow(true)),
            BigQueryUtils.toTableRow(generateRow(true)),
            BigQueryUtils.toTableRow(generateRow(true)),
            BigQueryUtils.toTableRow(generateRow(true)));
    dss.insertAll(anytableRef, records, null);

    FakeJobService fjs = new FakeJobService();
    FakeBigQueryServices bqs =
        new FakeBigQueryServices().withDatasetService(dss).withJobService(fjs);

    // HACK: We use this global variable to override the BQ Services in the expanded BigQuery
    // SchemaTransform.
    BigQuerySyndeoServices.servicesVariable = bqs;

    try {
      SyndeoV1.PipelineDescription description =
          SyndeoV1.PipelineDescription.newBuilder()
              .addTransforms(
                  new ProviderUtil.TransformSpec(
                          //   "org.apache.beam:schematransform:bigquery_storage_read:v1",
                          "beam:schematransform:org.apache.beam:bigquery_storage_read:v1",
                          Arrays.asList(bigQueryName, null, "DEFAULT", null, null))
                      .toProto())
              .addTransforms(
                  new ProviderUtil.TransformSpec(
                          "syndeo:schematransform:com.google.cloud:bigtable_write:v1",
                          BigTableWriteSchemaTransformConfiguration.builder()
                              .setProjectId("anyproject")
                              .setInstanceId("anyinstance")
                              .setTableId(bigTableName)
                              .setKeyColumns(Arrays.asList("name"))
                              .setEndpoint(bigTableContainer.getEmulatorEndpoint())
                              .build()
                              .toBeamRow()
                              .getValues())
                      .toProto())
              .build();
      PipelineOptions options = PipelineOptionsFactory.create();
      options.setTempLocation("any");
      options.as(BigQueryOptions.class).setProject("anyproject");
      SyndeoTemplate.run(options, description);
    } finally {
      BigQuerySyndeoServices.servicesVariable = null;
    }
  }
}
