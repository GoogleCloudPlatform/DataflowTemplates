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
package com.google.cloud.teleport.plugin.terraform;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.plugin.model.ImageSpec;
import com.google.cloud.teleport.plugin.model.TemplateDefinitions;
import com.google.cloud.teleport.plugin.sample.AtoBOk;
import freemarker.ext.beans.BeanModel;
import freemarker.template.TemplateException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import freemarker.template.TemplateModel;
import org.junit.Test;

public class TemplateTerraformGeneratorTest {

  @Test
  public void testAtoBOk() throws IOException, URISyntaxException, TemplateException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    TemplateTerraformGenerator.process(buildSpecModel(AtoBOk.class), out);
    String result = out.toString(StandardCharsets.UTF_8);
    assertThat(result, equalTo(loadResource("terraform/atobok.tf")));
  }

  private static ImageSpec buildSpecModel(Class<?> clazz) {
    checkArgument(clazz.isAnnotationPresent(Template.class));
    return new TemplateDefinitions(clazz, clazz.getAnnotation(Template.class))
            .buildSpecModel(false);
  }
  private static String loadResource(String fileName) throws URISyntaxException, IOException {
    URI uri =
            checkStateNotNull(
                    TemplateTerraformGeneratorTest.class.getClassLoader().getResource(fileName))
                    .toURI();
    return Files.readString(Paths.get(uri));
  }

}
