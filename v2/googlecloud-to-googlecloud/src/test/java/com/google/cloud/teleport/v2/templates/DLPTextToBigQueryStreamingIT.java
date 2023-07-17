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
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.truthmatchers.PipelineAsserts.assertThatResult;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.common.PipelineOperator.Result;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.TemplateTestBase;
import com.google.cloud.teleport.it.gcp.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.gcp.bigquery.conditions.BigQueryRowsCheck;
import com.google.cloud.teleport.it.gcp.bigquery.matchers.BigQueryAsserts;
import com.google.cloud.teleport.it.gcp.dlp.DlpResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.privacy.dlp.v2.CryptoHashConfig;
import com.google.privacy.dlp.v2.CryptoKey;
import com.google.privacy.dlp.v2.DeidentifyConfig;
import com.google.privacy.dlp.v2.DeidentifyTemplate;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.FieldTransformation;
import com.google.privacy.dlp.v2.PrimitiveTransformation;
import com.google.privacy.dlp.v2.RecordTransformations;
import com.google.privacy.dlp.v2.TransientCryptoKey;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for {@link DLPTextToBigQueryStreaming} (Stream_DLP_GCS_Text_to_BigQuery_Flex).
 */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(DLPTextToBigQueryStreaming.class)
@RunWith(JUnit4.class)
public class DLPTextToBigQueryStreamingIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(DLPTextToBigQueryStreamingIT.class);

  private BigQueryResourceManager bigQueryClient;
  private DlpResourceManager dlpResourceManager;

  @Before
  public void setup() throws IOException {
    bigQueryClient =
        BigQueryResourceManager.builder(testName, PROJECT).setCredentials(credentials).build();
    dlpResourceManager =
        DlpResourceManager.builder(PROJECT).setCredentialsProvider(credentialsProvider).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(bigQueryClient, dlpResourceManager);
  }

  @Test
  public void testDLPTextToBigQuery() throws IOException {
    testDLPTextToBigQuery(Function.identity()); // no extra parameters to set
  }

  @Test
  public void testDLPTextToBigQueryWithStorageApi() throws IOException {
    testDLPTextToBigQuery(
        b ->
            b.addParameter("useStorageWriteApi", "true")
                .addParameter("numStorageWriteApiStreams", "1")
                .addParameter("storageWriteApiTriggeringFrequencySec", "5"));
  }

  private void testDLPTextToBigQuery(
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder) throws IOException {
    // Arrange
    String dataset = bigQueryClient.createDataset(REGION);

    // Create a template to hash card number + CVV
    DeidentifyTemplate deidentifyTemplate =
        dlpResourceManager.createDeidentifyTemplate(
            DeidentifyTemplate.newBuilder()
                .setName(String.format("projects/%s/deidentifyTemplates/%s", PROJECT, testId))
                .setDescription("Template for test " + testName)
                .setDeidentifyConfig(
                    DeidentifyConfig.newBuilder()
                        .setRecordTransformations(
                            RecordTransformations.newBuilder()
                                .addFieldTransformations(
                                    FieldTransformation.newBuilder()
                                        .addFields(FieldId.newBuilder().setName("Card Num").build())
                                        .addFields(FieldId.newBuilder().setName("CVV/CVV2").build())
                                        .setPrimitiveTransformation(
                                            PrimitiveTransformation.newBuilder()
                                                .setCryptoHashConfig(
                                                    CryptoHashConfig.newBuilder()
                                                        .setCryptoKey(
                                                            CryptoKey.newBuilder()
                                                                .setTransient(
                                                                    TransientCryptoKey.newBuilder()
                                                                        .setName(testId)
                                                                        .build())
                                                                .build())
                                                        .build())
                                                .build())
                                        .build())
                                .build()))
                .build());
    LOG.info("Created deidentify template: {}", deidentifyTemplate.getName());

    // Act
    LaunchInfo info =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("inputFilePattern", getGcsPath("*.csv"))
                    .addParameter("datasetName", dataset)
                    .addParameter("batchSize", "1000")
                    .addParameter("dlpProjectId", PROJECT)
                    .addParameter("deidentifyTemplateName", deidentifyTemplate.getName())));
    assertThatPipeline(info).isRunning();

    gcsClient.createArtifact(
        "input.csv",
        "Card Type Full Name,Issuing Bank,Card Num,Card Holder's Name,CVV/CVV2,Issue Date,Expiry Date,Billing Date,Card PIN,Credit Limit,Comments\n"
            + "Visa,Chase,4111111111111111,Frank Q Ortiz,360,09/2019,09/2024,7,1247,103700,Please change my number to 604-406-9050. Thanks\n");

    TableId targetTableId = TableId.of(PROJECT, dataset, "input");
    Result result =
        pipelineOperator()
            // drain doesn't seem to work with the TextIO GCS files watching that the template uses
            .waitForConditionAndCancel(
                createConfig(info),
                BigQueryRowsCheck.builder(bigQueryClient, targetTableId).setMinRows(1).build());

    // Assert
    assertThatResult(result).meetsConditions();
    List<Map<String, Object>> records =
        BigQueryAsserts.tableResultToRecords(bigQueryClient.readTable(targetTableId));
    assertThat(records).hasSize(1);

    Map<String, Object> record = records.get(0);
    assertThat((String) record.get("Card_Num")).isNotEqualTo("4111111111111111");
    assertThat((String) record.get("CVVCVV2")).isNotEqualTo("360");
    assertThat((String) record.get("Comments"))
        .isEqualTo("Please change my number to 604-406-9050. Thanks");
  }
}
