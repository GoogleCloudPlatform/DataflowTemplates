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
package com.google.cloud.teleport.plugin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.plugin.model.ImageSpec;
import com.google.cloud.teleport.plugin.model.ImageSpecMetadata;
import com.google.cloud.teleport.plugin.model.TemplateDefinitions;
import com.google.cloud.teleport.plugin.sample.AtoBOk;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for class {@link TemplateSpecsGenerator}. */
@RunWith(JUnit4.class)
public class TemplateSpecsGeneratorTest {

  private static final Gson GSON = new GsonBuilder().create();

  private final TemplateDefinitions definitions =
      new TemplateDefinitions(AtoBOk.class, AtoBOk.class.getAnnotation(Template.class));
  private final ImageSpec imageSpec = definitions.buildSpecModel(false);
  private final File outputFolder = Files.createTempDir().getAbsoluteFile();
  private final TemplateSpecsGenerator generator = new TemplateSpecsGenerator();

  @Test
  public void saveImageSpec() throws IOException {
    File saveImageSpec = generator.saveImageSpec(definitions, imageSpec, outputFolder);
    assertNotNull(saveImageSpec);
    assertTrue(saveImageSpec.exists());

    try (FileInputStream fis = new FileInputStream(saveImageSpec)) {
      String json = new String(fis.readAllBytes(), StandardCharsets.UTF_8);
      ImageSpec read = GSON.fromJson(json, ImageSpec.class);
      assertEquals(imageSpec.getMetadata().getName(), read.getMetadata().getName());
      assertEquals(imageSpec.getMetadata().getParameter("hiddenParam").get().hiddenUi(), true);
    }
  }

  @Test
  public void saveMetadata() throws IOException {
    ImageSpecMetadata metadata = imageSpec.getMetadata();
    File saveMetadata = generator.saveMetadata(definitions, metadata, outputFolder);
    assertNotNull(saveMetadata);
    assertTrue(saveMetadata.exists());

    try (FileInputStream fis = new FileInputStream(saveMetadata)) {
      ImageSpecMetadata read =
          GSON.fromJson(
              new String(fis.readAllBytes(), StandardCharsets.UTF_8), ImageSpecMetadata.class);
      assertEquals(metadata.getName(), read.getName());
    }
  }

  @Test
  public void saveCommandSpec() throws IOException {
    File spec = generator.saveCommandSpec(definitions, outputFolder);
    assertNotNull(spec);
    assertTrue(spec.exists());

    try (FileInputStream fis = new FileInputStream(spec)) {
      String specString = new String(fis.readAllBytes(), StandardCharsets.UTF_8);
      assertTrue(specString.contains(definitions.getTemplateClass().getName()));
    }
  }

  @Test
  public void getTemplateNameDash() {
    assertEquals("cloud-pubsub-to-text", generator.getTemplateNameDash("Cloud PubSub to Text"));
    assertEquals("cloud-pubsub-to-text", generator.getTemplateNameDash("Cloud_PubSub_to_Text"));
  }
}
