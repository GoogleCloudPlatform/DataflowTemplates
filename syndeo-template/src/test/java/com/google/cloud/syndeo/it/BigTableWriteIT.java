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

import com.google.cloud.syndeo.common.ProviderUtil;
import com.google.cloud.syndeo.transforms.bigtable.BigTableWriteSchemaTransformConfiguration;
import com.google.cloud.teleport.it.TestProperties;
import com.google.cloud.teleport.it.dataflow.DataflowOperator;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient;
import com.google.cloud.teleport.it.dataflow.DataflowUtils;
import com.google.cloud.teleport.it.dataflow.FlexTemplateClient;
import java.time.Instant;
import java.util.Base64;
import java.util.Random;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigTableWriteIT {

  @Rule public final TestName testName = new TestName();

  //    private static final Credentials CREDENTIALS = TestProperties.googleCredentials();
  private static final String PROJECT = TestProperties.project();
  private static final String REGION = TestProperties.region();
  private static final String SPEC_PATH = TestProperties.specPath();
  private static final String TEMP_LOCATION = TestProperties.artifactBucket();

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

  private static String randomString(Integer length) {
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

  @Test
  public void testCreateToBigTableSmallNonTemplateJob() {
    Pipeline p = Pipeline.create();
    // TODO(pabloem): Add this to test table creation
    //        String bigTableName = "syndeo-test-" + randomString(10);
    String bigTableName = "syndeo-test-2022-08";

    SchemaTransformProvider provider = ProviderUtil.getProvider("bigtable:write");
    SchemaTransform transform =
        provider.from(
            BigTableWriteSchemaTransformConfiguration.builder()
                .setProjectId(PROJECT)
                .setInstanceId("teleport")
                .setTableId(bigTableName)
                .setKeyColumns(Lists.newArrayList("name", "birthday"))
                .build()
                .toBeamRow());

    PCollectionRowTuple pCollectionRowTuple =
        PCollectionRowTuple.of(
            "INPUT",
            p.apply(
                    Create.of(
                        generateRow(), generateRow(), generateRow(), generateRow(), generateRow()))
                .setRowSchema(INPUT_SCHEMA));
    pCollectionRowTuple.apply(transform.buildTransform());

    p.run().waitUntilFinish(Duration.standardSeconds(100));
  }

  @Test
  public void testBigQueryToBigTableSmallNonTemplateJob() {
    PipelineOptions setupOptions = PipelineOptionsFactory.create();
    setupOptions.setTempLocation(TEMP_LOCATION);
    Pipeline setupP = Pipeline.create(setupOptions);
    // TODO(pabloem): Add this to test table creation
    //        String bigTableName = "syndeo-test-" + randomString(10);
    String bigTableName = "syndeo-test-2022-08";
    String bigQueryName = PROJECT + ":syndeo_dataset.syndeo-test-2022-08-bq";
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
        .apply(
            BigQueryIO.<Row>write()
                .to(bigQueryName)
                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .useBeamSchema());

    setupP.run().waitUntilFinish();

    PipelineOptions options = PipelineOptionsFactory.create();
    options.setTempLocation(TEMP_LOCATION);
    Pipeline p = Pipeline.create(options);

    SchemaTransformProvider bigqueryProvider = ProviderUtil.getProvider("schemaIO:bigquery:read");
    SchemaTransform bigqueryReader =
        bigqueryProvider.from(
            Row.withSchema(bigqueryProvider.configurationSchema())
                .addValue("us-central1") // Location
                .addValue(null) // Schema - it's already known
                .addValue(bigQueryName) // Table
                .addValue(null) // Query is not necessary
                .addValue(null) // Query location is not necessary.
                .addValue(null) // Create disposition is not necessary
                .build());

    SchemaTransformProvider bigtableProvider = ProviderUtil.getProvider("bigtable:write");
    SchemaTransform bigtableWriter =
        bigtableProvider.from(
            BigTableWriteSchemaTransformConfiguration.builder()
                .setProjectId(PROJECT)
                .setInstanceId("teleport")
                .setTableId(bigTableName)
                .setKeyColumns(Lists.newArrayList("name", "birthday"))
                .build()
                .toBeamRow());

    PCollectionRowTuple readPcoll =
        PCollectionRowTuple.empty(p).apply(bigqueryReader.buildTransform());

    PCollectionRowTuple.of("INPUT", readPcoll.getAll().values().iterator().next())
        .apply(bigtableWriter.buildTransform());

    p.run().waitUntilFinish(Duration.standardSeconds(100));
  }

  @Ignore
  @Test
  public void testBigQueryToBigTableSmallTemplateJob() throws Exception {
    String name = testName.getMethodName();
    String jobName = DataflowUtils.createJobName(name);

    DataflowTemplateClient.LaunchConfig options =
        DataflowTemplateClient.LaunchConfig.builder(jobName, SPEC_PATH)
            // TODO(pabloem): Add configuration parameters here.
            .build();
    DataflowTemplateClient dataflow =
        FlexTemplateClient.builder()
            //                        .setCredentials(CREDENTIALS)
            .build();

    // Act
    DataflowTemplateClient.JobInfo info = dataflow.launchTemplate(PROJECT, REGION, options);

    DataflowOperator.Result result =
        new DataflowOperator(dataflow)
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {
                  // TODO(pabloem): What condition do we use to validate DF job?
                  return false;
                });
  }

  private static DataflowOperator.Config createConfig(DataflowTemplateClient.JobInfo info) {
    return DataflowOperator.Config.builder()
        .setJobId(info.jobId())
        .setProject(PROJECT)
        .setRegion(REGION)
        .build();
  }
}
