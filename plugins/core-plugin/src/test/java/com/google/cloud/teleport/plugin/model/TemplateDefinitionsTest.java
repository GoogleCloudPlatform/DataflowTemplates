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
package com.google.cloud.teleport.plugin.model;

import static com.google.cloud.teleport.plugin.model.TemplateDefinitions.ADDITIONAL_EXPERIMENTS;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.plugin.sample.AtoBMissingAnnotation;
import com.google.cloud.teleport.plugin.sample.AtoBOk;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for class {@link TemplateDefinitions}. */
@RunWith(JUnit4.class)
public class TemplateDefinitionsTest {

  @Test
  public void testSampleAtoBOk() {
    TemplateDefinitions definitions =
        new TemplateDefinitions(AtoBOk.class, AtoBOk.class.getAnnotation(Template.class));
    assertNotNull(definitions);

    ImageSpec imageSpec = definitions.buildSpecModel(true);
    assertNotNull(imageSpec);

    ImageSpecMetadata metadata = imageSpec.getMetadata();
    assertNotNull(metadata);

    assertEquals("A to B", metadata.getName());
    assertEquals(
        "Streaming Template that sends A to B.\n\nBut it can also send B to C.",
        metadata.getDescription());
    assertEquals("com.google.cloud.teleport.plugin.sample.AtoBOk", metadata.getMainClass());

    // Make sure metadata follows stable order
    assertEquals("from", metadata.getParameters().get(0).getName());

    ImageSpecParameter from = metadata.getParameter("from").get();
    assertEquals(ImageSpecParameterType.TEXT, from.getParamType());

    ImageSpecParameter to = metadata.getParameter("to").get();
    assertEquals(ImageSpecParameterType.BIGQUERY_TABLE, to.getParamType());

    ImageSpecParameter inputKafkaTopic = metadata.getParameter("inputKafkaTopic").get();
    assertEquals(ImageSpecParameterType.KAFKA_TOPIC, inputKafkaTopic.getParamType());

    ImageSpecParameter logical = metadata.getParameter("logical").get();
    assertEquals(ImageSpecParameterType.BOOLEAN, logical.getParamType());
    assertEquals("^(true|false)$", logical.getRegexes().get(0));

    ImageSpecParameter json = metadata.getParameter("JSON").get();
    assertEquals(ImageSpecParameterType.BOOLEAN, json.getParamType());

    ImageSpecParameter gcsReadBucket = metadata.getParameter("gcsReadBucket").get();
    assertEquals(ImageSpecParameterType.GCS_READ_BUCKET, gcsReadBucket.getParamType());

    ImageSpecParameter gcsWriteBucket = metadata.getParameter("gcsWriteBucket").get();
    assertEquals(ImageSpecParameterType.GCS_WRITE_BUCKET, gcsWriteBucket.getParamType());

    ImageSpecParameter javascriptUdfFile = metadata.getParameter("javascriptUdfFile").get();
    assertEquals(ImageSpecParameterType.JAVASCRIPT_UDF_FILE, javascriptUdfFile.getParamType());

    ImageSpecParameter machineType = metadata.getParameter("machineType").get();
    assertEquals(ImageSpecParameterType.MACHINE_TYPE, machineType.getParamType());

    ImageSpecParameter serviceAccount = metadata.getParameter("serviceAccount").get();
    assertEquals(ImageSpecParameterType.SERVICE_ACCOUNT, serviceAccount.getParamType());

    ImageSpecParameter workerRegion = metadata.getParameter("workerRegion").get();
    assertEquals(ImageSpecParameterType.WORKER_REGION, workerRegion.getParamType());

    ImageSpecParameter workerZone = metadata.getParameter("workerZone").get();
    assertEquals(ImageSpecParameterType.WORKER_ZONE, workerZone.getParamType());
  }

  @Test
  public void testSampleAtoBMissingAnnotation() {
    TemplateDefinitions definitions =
        new TemplateDefinitions(
            AtoBMissingAnnotation.class, AtoBMissingAnnotation.class.getAnnotation(Template.class));
    assertNotNull(definitions);

    assertThrows(IllegalArgumentException.class, () -> definitions.buildSpecModel(true));
  }

  @Test
  public void givenBatchDefault_thenValidates() {
    assertThat(imageSpecWithValidationOf(Batch.class)).isNotNull();
  }

  @Test
  public void givenStreamingDefault_thenValidates() {
    assertThat(imageSpecWithValidationOf(Streaming.class)).isNotNull();
  }

  @Test
  public void givenBatchWithStreamingModeConfigurations_throws() {
    assertThrows(
        IllegalArgumentException.class, () -> imageSpecWithValidationOf(BatchSupportsALO.class));
    assertThrows(
        IllegalArgumentException.class,
        () -> imageSpecWithValidationOf(BatchStreamingModeALO.class));
    assertThrows(
        IllegalArgumentException.class,
        () -> imageSpecWithValidationOf(BatchStreamingModeEO.class));
  }

  @Test
  public void givenStreamingModeMismatch_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> imageSpecWithValidationOf(StreamingNotALONotEO.class));
    assertThrows(
        IllegalArgumentException.class,
        () -> imageSpecWithValidationOf(StreamingALOMismatch.class));
    assertThrows(
        IllegalArgumentException.class, () -> imageSpecWithValidationOf(StreamingEOMismatch.class));
  }

  @Test
  public void givenStreamingBothAOEO_thenValidates() {
    imageSpecWithValidationOf(StreamingALOEOEnabledDefaultNone.class);
    imageSpecWithValidationOf(StreamingALOEOEnabledDefaultALO.class);
    imageSpecWithValidationOf(StreamingALOEOEnabledDefaultEO.class);
  }

  @Test
  public void givenStreamingModeNone_noAdditionalExperiments() {
    ImageSpec imageSpec = imageSpecWithValidationOf(StreamingALOEOEnabledDefaultNone.class);
    assertThat(imageSpec.getDefaultEnvironment()).isNotNull();
    assertThat(imageSpec.getDefaultEnvironment()).doesNotContainKey(ADDITIONAL_EXPERIMENTS);
  }

  @Test
  public void givenStreamingModeALO_populatesAdditionalExperiments() {
    ImageSpec imageSpec = imageSpecWithValidationOf(StreamingALOEOEnabledDefaultALO.class);
    assertThat(imageSpec.getDefaultEnvironment()).isNotNull();
    Map<String, Object> defaultEnvironment = imageSpec.getDefaultEnvironment();
    assertThat(defaultEnvironment.size()).isGreaterThan(1);
    assertThat(defaultEnvironment).containsKey(ADDITIONAL_EXPERIMENTS);
    List<String> additionalExperiments =
        (List<String>) defaultEnvironment.get(ADDITIONAL_EXPERIMENTS);
    assertThat(additionalExperiments).containsExactly("streaming_mode_at_least_once");
  }

  @Test
  public void givenStreamingModeEO_populatesAdditionalExperiments() {
    ImageSpec imageSpec = imageSpecWithValidationOf(StreamingALOEOEnabledDefaultEO.class);
    assertThat(imageSpec.getDefaultEnvironment()).isNotNull();
    Map<String, Object> defaultEnvironment = imageSpec.getDefaultEnvironment();
    assertThat(defaultEnvironment.size()).isGreaterThan(1);
    assertThat(defaultEnvironment).containsKey(ADDITIONAL_EXPERIMENTS);
    List<String> additionalExperiments =
        (List<String>) defaultEnvironment.get(ADDITIONAL_EXPERIMENTS);
    assertThat(additionalExperiments).containsExactly("streaming_mode_exactly_once");
  }

  private static ImageSpec imageSpecWithValidationOf(Class<?> clazz) {
    return templateDefinitionsOf(clazz).buildSpecModel(true);
  }

  private static TemplateDefinitions templateDefinitionsOf(Class<?> clazz) {
    checkArgument(clazz.isAnnotationPresent(Template.class));
    return new TemplateDefinitions(clazz, clazz.getAnnotation(Template.class));
  }

  @Template(
      name = "Batch",
      displayName = "",
      description = {},
      streaming = false,
      category = TemplateCategory.BATCH,
      testOnly = true)
  private static class Batch {}

  @Template(
      supportsAtLeastOnce = true,
      name = "BatchSupportsALO",
      displayName = "",
      description = {},
      streaming = false,
      category = TemplateCategory.BATCH,
      testOnly = true)
  private static class BatchSupportsALO {}

  @Template(
      defaultStreamingMode = Template.StreamingMode.AT_LEAST_ONCE,
      name = "BatchStreamingModeALO",
      displayName = "",
      description = {},
      streaming = false,
      category = TemplateCategory.BATCH,
      testOnly = true)
  private static class BatchStreamingModeALO {}

  @Template(
      defaultStreamingMode = Template.StreamingMode.EXACTLY_ONCE,
      name = "BatchStreamingModeEO",
      displayName = "",
      description = {},
      streaming = false,
      category = TemplateCategory.BATCH,
      testOnly = true)
  private static class BatchStreamingModeEO {}

  @Template(
      name = "Streaming",
      displayName = "",
      description = {},
      streaming = true,
      category = TemplateCategory.STREAMING,
      testOnly = true)
  private static class Streaming {}

  @Template(
      supportsAtLeastOnce = true,
      supportsExactlyOnce = true,
      name = "StreamingALOEOEnabledDefaultNone",
      displayName = "",
      description = {},
      streaming = true,
      category = TemplateCategory.STREAMING,
      testOnly = true)
  private static class StreamingALOEOEnabledDefaultNone {}

  @Template(
      supportsAtLeastOnce = true,
      supportsExactlyOnce = true,
      defaultStreamingMode = Template.StreamingMode.AT_LEAST_ONCE,
      name = "StreamingALOEOEnabledDefaultALO",
      displayName = "",
      description = {},
      streaming = true,
      category = TemplateCategory.STREAMING,
      testOnly = true)
  private static class StreamingALOEOEnabledDefaultALO {}

  @Template(
      supportsAtLeastOnce = true,
      supportsExactlyOnce = true,
      defaultStreamingMode = Template.StreamingMode.EXACTLY_ONCE,
      name = "StreamingALOEOEnabledDefaultEO",
      displayName = "",
      description = {},
      streaming = true,
      category = TemplateCategory.STREAMING,
      testOnly = true)
  private static class StreamingALOEOEnabledDefaultEO {}

  @Template(
      supportsAtLeastOnce = false,
      supportsExactlyOnce = false,
      name = "StreamingNotALONotEO",
      displayName = "",
      description = {},
      streaming = true,
      category = TemplateCategory.STREAMING,
      testOnly = true)
  private static class StreamingNotALONotEO {}

  @Template(
      supportsAtLeastOnce = false,
      defaultStreamingMode = Template.StreamingMode.AT_LEAST_ONCE,
      name = "StreamingALOMismatch",
      displayName = "",
      description = {},
      streaming = true,
      category = TemplateCategory.STREAMING,
      testOnly = true)
  private static class StreamingALOMismatch {}

  @Template(
      supportsExactlyOnce = false,
      defaultStreamingMode = Template.StreamingMode.EXACTLY_ONCE,
      name = "StreamingALOMismatch",
      displayName = "",
      description = {},
      streaming = true,
      category = TemplateCategory.STREAMING,
      testOnly = true)
  private static class StreamingEOMismatch {}
}
