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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.plugin.sample.AtoBMissingAnnotation;
import com.google.cloud.teleport.plugin.sample.AtoBOk;
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

    assertEquals("AtoB", metadata.getName());
    assertEquals("Send A to B", metadata.getDescription());
    assertEquals("com.google.cloud.teleport.plugin.sample.AtoBOk", metadata.getMainClass());

    // Make sure metadata follows stable order
    assertEquals("from", metadata.getParameters().get(0).getName());

    ImageSpecParameter from = metadata.getParameter("from").get();
    assertEquals(ImageSpecParameterType.TEXT, from.getParamType());

    ImageSpecParameter to = metadata.getParameter("to").get();
    assertEquals(ImageSpecParameterType.TEXT, to.getParamType());

    ImageSpecParameter logical = metadata.getParameter("logical").get();
    assertEquals(ImageSpecParameterType.TEXT, logical.getParamType());
    assertEquals("^(true|false)$", logical.getRegexes().get(0));

    ImageSpecParameter json = metadata.getParameter("JSON").get();
    assertEquals(ImageSpecParameterType.TEXT, logical.getParamType());
  }

  @Test
  public void testSampleAtoBMissingAnnotation() {
    TemplateDefinitions definitions =
        new TemplateDefinitions(
            AtoBMissingAnnotation.class, AtoBMissingAnnotation.class.getAnnotation(Template.class));
    assertNotNull(definitions);

    assertThrows(IllegalArgumentException.class, () -> definitions.buildSpecModel(true));
  }
}
