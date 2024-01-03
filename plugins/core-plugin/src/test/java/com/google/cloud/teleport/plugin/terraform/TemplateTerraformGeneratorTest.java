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
package com.google.cloud.teleport.plugin.terraform;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.plugin.model.ImageSpec;
import com.google.cloud.teleport.plugin.model.ImageSpecMetadata;
import com.google.cloud.teleport.plugin.model.ImageSpecParameter;
import com.google.cloud.teleport.plugin.model.TemplateDefinitions;
import com.google.cloud.teleport.plugin.sample.AtoBOk;
import freemarker.template.TemplateException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link TemplateTerraformGenerator}. */
@RunWith(JUnit4.class)
public class TemplateTerraformGeneratorTest {
  @Test
  public void givenClassic_generatesJobModule() throws TemplateException, IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ImageSpec spec = atoBOkImageSpec();
    ImageSpecMetadata metadata = spec.getMetadata();
    metadata.setFlexTemplate(false);
    TemplateTerraformGenerator.process(spec, baos);
    String got = baos.toString(StandardCharsets.UTF_8);
    for (ImageSpecParameter parameter : metadata.getParameters()) {
      assertThat(got.contains(parameter.getName())).isTrue();
    }
    assertThat(got.contains("resource \"google_dataflow_job\"")).isTrue();
    assertThat(got.contains("resource \"google_dataflow_flex_template_job\"")).isFalse();
  }

  @Test
  public void givenFlex_generatesFlexJobModule() throws TemplateException, IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ImageSpec spec = atoBOkImageSpec();
    ImageSpecMetadata metadata = spec.getMetadata();
    metadata.setFlexTemplate(true);
    TemplateTerraformGenerator.process(spec, baos);
    String got = baos.toString(StandardCharsets.UTF_8);
    for (ImageSpecParameter parameter : metadata.getParameters()) {
      assertThat(got.contains(parameter.getName())).isTrue();
    }
    assertThat(got.contains("resource \"google_dataflow_job\"")).isFalse();
    assertThat(got.contains("resource \"google_dataflow_flex_template_job\"")).isTrue();
  }

  private static ImageSpec atoBOkImageSpec() {
    TemplateDefinitions definitions =
        new TemplateDefinitions(AtoBOk.class, AtoBOk.class.getAnnotation(Template.class));
    return definitions.buildSpecModel(false);
  }
}
