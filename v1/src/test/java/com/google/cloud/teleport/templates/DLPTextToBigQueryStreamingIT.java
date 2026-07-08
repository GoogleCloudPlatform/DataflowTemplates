/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.templates;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.metadata.SkipRunnerV2Test;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.privacy.dlp.v2.CharacterMaskConfig;
import com.google.privacy.dlp.v2.CryptoHashConfig;
import com.google.privacy.dlp.v2.CryptoKey;
import com.google.privacy.dlp.v2.DeidentifyConfig;
import com.google.privacy.dlp.v2.DeidentifyTemplate;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.FieldTransformation;
import com.google.privacy.dlp.v2.InfoType;
import com.google.privacy.dlp.v2.InfoTypeTransformations;
import com.google.privacy.dlp.v2.InspectConfig;
import com.google.privacy.dlp.v2.InspectTemplate;
import com.google.privacy.dlp.v2.PrimitiveTransformation;
import com.google.privacy.dlp.v2.RecordTransformations;
import com.google.privacy.dlp.v2.TransientCryptoKey;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigquery.conditions.BigQueryRowsCheck;
import org.apache.beam.it.gcp.bigquery.matchers.BigQueryAsserts;
import org.apache.beam.it.gcp.dlp.DlpResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link DLPTextToBigQueryStreaming} (Stream_DLP_GCS_Text_to_BigQuery). */
@Category({TemplateIntegrationTest.class, SkipRunnerV2Test.class})
@TemplateIntegrationTest(DLPTextToBigQueryStreaming.class)
@RunWith(JUnit4.class)
public class DLPTextToBigQueryStreamingIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(DLPTextToBigQueryStreamingIT.class);

  private BigQueryResourceManager bigQueryClient;
  private DlpResourceManager dlpResourceManager;

  @Before
  public void setup() throws IOException {
    bigQueryClient = BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
    dlpResourceManager = DlpResourceManager.builder(PROJECT, credentialsProvider).build();

    gcsClient.createArtifact(
        "input.csv",
        "Card Type Full Name,Issuing Bank,Card Num,Card Holder's Name,CVV/CVV2,Issue Date,Expiry Date,Billing Date,Card PIN,Credit Limit,Comments\n"
            + "Visa,Chase,4111111111111111,Frank Q Ortiz,360,09/2019,09/2024,7,1247,103700,Please change my number to 604-406-9050. Thanks\n");
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(bigQueryClient, dlpResourceManager);
  }

  @Test
  public void testDLPTextToBigQueryInspect() throws IOException {
    // Create a template to hash card number + CVV
    DeidentifyTemplate deidentifyTemplate = createInfoTypeTemplate();

    // Create inspect template
    InspectTemplate inspectTemplate = createInspectTemplate();

    testDLPTextToBigQueryBase(
        params -> params.addParameter("inspectTemplateName", inspectTemplate.getName()),
        (record) -> {
          assertThat((String) record.get("Card_Num")).isEqualTo("4111111111111111");
          assertThat((String) record.get("CVVCVV2")).isEqualTo("360");
          assertThat((String) record.get("Comments"))
              .isNotEqualTo("Please change my number to 604-406-9050. Thanks");
        },
        deidentifyTemplate);
  }

  @Test
  public void testDLPTextToBigQuery() throws IOException {
    // Create a template to hash card number + CVV
    DeidentifyTemplate deidentifyTemplate = createRecordTypeTemplate();

    testDLPTextToBigQueryBase(
        Function.identity(),
        (record) -> {
          assertThat((String) record.get("Card_Num")).isNotEqualTo("4111111111111111");
          assertThat((String) record.get("CVVCVV2")).isNotEqualTo("360");
          assertThat((String) record.get("Comments"))
              .isEqualTo("Please change my number to 604-406-9050. Thanks");
        },
        deidentifyTemplate);
  }

  public void testDLPTextToBigQueryBase(
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder,
      Consumer<Map<String, Object>> recordAsserts,
      DeidentifyTemplate deidentifyTemplate)
      throws IOException {
    // Arrange
    String dataset = bigQueryClient.createDataset(REGION);

    // Act
    PipelineLauncher.LaunchInfo info =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("inputFilePattern", getGcsPath("*.csv"))
                    .addParameter("datasetName", dataset)
                    .addParameter("batchSize", "1000")
                    .addParameter("dlpProjectId", PROJECT)
                    .addParameter("deidentifyTemplateName", deidentifyTemplate.getName())));
    assertThatPipeline(info).isRunning();

    TableId targetTableId = TableId.of(PROJECT, dataset, "input");
    PipelineOperator.Result result =
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

    recordAsserts.accept(records.get(0));
  }

  private InspectTemplate createInspectTemplate() throws IOException {
    InspectTemplate inspectTemplate =
        dlpResourceManager.createInspectTemplate(
            InspectTemplate.newBuilder()
                .setName(String.format("projects/%s/inspectTemplates/%s", PROJECT, testId))
                .setDescription("Template for test " + testName)
                .setInspectConfig(
                    InspectConfig.newBuilder()
                        .addAllInfoTypes(
                            Stream.of("PHONE_NUMBER")
                                .map(it -> InfoType.newBuilder().setName(it).build())
                                .collect(Collectors.toList()))
                        .build())
                .build());
    LOG.info("Created inspect template: {}", inspectTemplate.getName());

    return inspectTemplate;
  }

  private DeidentifyTemplate createInfoTypeTemplate() throws IOException {
    DeidentifyTemplate deidentifyTemplate =
        dlpResourceManager.createDeidentifyTemplate(
            DeidentifyTemplate.newBuilder()
                .setName(String.format("projects/%s/deidentifyTemplates/%s", PROJECT, testId))
                .setDescription("Template for test " + testName)
                .setDeidentifyConfig(
                    DeidentifyConfig.newBuilder()
                        .setInfoTypeTransformations(
                            InfoTypeTransformations.newBuilder()
                                .addTransformations(
                                    InfoTypeTransformations.InfoTypeTransformation.newBuilder()
                                        .setPrimitiveTransformation(
                                            PrimitiveTransformation.newBuilder()
                                                .setCharacterMaskConfig(
                                                    CharacterMaskConfig.newBuilder()
                                                        .setMaskingCharacter("X")
                                                        .setNumberToMask(5)
                                                        .build())
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
                                .build())
                        .build())
                .build());
    LOG.info("Created deidentify template: {}", deidentifyTemplate.getName());

    return deidentifyTemplate;
  }

  private DeidentifyTemplate createRecordTypeTemplate() throws IOException {
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
                                .build())
                        .build())
                .build());
    LOG.info("Created deidentify template: {}", deidentifyTemplate.getName());

    return deidentifyTemplate;
  }
}
