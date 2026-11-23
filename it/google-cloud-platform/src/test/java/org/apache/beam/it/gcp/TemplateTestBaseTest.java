/*
 * Copyright (C) 2026 Google LLC
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
package org.apache.beam.it.gcp;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.Template.TemplateType;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link TemplateTestBase}. */
@RunWith(JUnit4.class)
public class TemplateTestBaseTest {

  @BeforeClass
  public static void setUpClass() {
    if (System.getProperty("project") == null) {
      System.setProperty("project", "testProject");
    }
    if (System.getProperty("region") == null) {
      System.setProperty("region", "us-central1");
    }
  }

  @After
  public void tearDown() {
    System.clearProperty(TemplateTestBase.SPEC_PATH_PREFIX_PROPERTY);
    System.clearProperty("specPath");
  }

  @Test
  public void testGetPreStagedSpecPathWhenUnsetOrBlank() {
    System.clearProperty(TemplateTestBase.SPEC_PATH_PREFIX_PROPERTY);
    assertThat(TemplateTestBase.getPreStagedSpecPath("Word_Count", false)).isNull();

    System.setProperty(TemplateTestBase.SPEC_PATH_PREFIX_PROPERTY, "   ");
    assertThat(TemplateTestBase.getPreStagedSpecPath("Word_Count", true)).isNull();
  }

  @Test
  public void testGetPreStagedSpecPathClassicTemplate() {
    System.setProperty(
        TemplateTestBase.SPEC_PATH_PREFIX_PROPERTY,
        "gs://dataflow-templates-staging/2026-11-24-00_RC00");
    Template classic = mock(Template.class);
    when(classic.name()).thenReturn("Word_Count");
    when(classic.type()).thenReturn(TemplateType.JAVA);
    when(classic.flexContainerName()).thenReturn("");

    assertThat(TemplateTestBase.getPreStagedSpecPath(classic))
        .isEqualTo("gs://dataflow-templates-staging/2026-11-24-00_RC00/Word_Count");
  }

  @Test
  public void testGetPreStagedSpecPathFlexTemplate() {
    System.setProperty(
        TemplateTestBase.SPEC_PATH_PREFIX_PROPERTY,
        "gs://dataflow-templates-staging/2026-11-24-00_RC00/");
    Template flex = mock(Template.class);
    when(flex.name()).thenReturn("PubSub_to_BigQuery_Flex");
    when(flex.type()).thenReturn(TemplateType.JAVA);
    when(flex.flexContainerName()).thenReturn("googlecloud-to-googlecloud");

    assertThat(TemplateTestBase.getPreStagedSpecPath(flex))
        .isEqualTo(
            "gs://dataflow-templates-staging/2026-11-24-00_RC00/flex/PubSub_to_BigQuery_Flex");
  }

  @Test
  public void testGetPreStagedSpecPathYamlAndPythonTemplates() {
    System.setProperty(
        TemplateTestBase.SPEC_PATH_PREFIX_PROPERTY,
        "dataflow-templates-staging/2026-11-24-00_RC00");

    Template yaml = mock(Template.class);
    when(yaml.name()).thenReturn("Word_Count_Yaml");
    when(yaml.type()).thenReturn(TemplateType.YAML);
    when(yaml.flexContainerName()).thenReturn("");
    assertThat(TemplateTestBase.getPreStagedSpecPath(yaml))
        .isEqualTo("gs://dataflow-templates-staging/2026-11-24-00_RC00/flex/Word_Count_Yaml");

    Template python = mock(Template.class);
    when(python.name()).thenReturn("Yaml_Template");
    when(python.type()).thenReturn(TemplateType.PYTHON);
    when(python.flexContainerName()).thenReturn("");
    assertThat(TemplateTestBase.getPreStagedSpecPath(python))
        .isEqualTo("gs://dataflow-templates-staging/2026-11-24-00_RC00/flex/Yaml_Template");
  }

  @Test
  public void testGetSpecPathPrecedenceSpecPathOverPrefix() throws Exception {
    System.setProperty(
        TemplateTestBase.SPEC_PATH_PREFIX_PROPERTY,
        "gs://dataflow-templates-staging/2026-11-24-00_RC00");
    System.setProperty("specPath", "gs://custom-bucket/override/My_Template");

    Template template = mock(Template.class);
    when(template.name()).thenReturn("My_Template");
    when(template.type()).thenReturn(TemplateType.JAVA);
    when(template.flexContainerName()).thenReturn("my-container");

    TemplateTestBase testBase = new TemplateTestBase() {};
    assertThat(testBase.getSpecPath(Object.class, template, "pom.xml"))
        .isEqualTo("gs://custom-bucket/override/My_Template");
  }

  @Test
  public void testGetSpecPathUsesPreStagedPrefixWithoutStaging() throws Exception {
    System.setProperty(
        TemplateTestBase.SPEC_PATH_PREFIX_PROPERTY,
        "gs://dataflow-templates-staging/2026-11-24-00_RC00");

    Template template = mock(Template.class);
    when(template.name()).thenReturn("My_Template");
    when(template.type()).thenReturn(TemplateType.JAVA);
    when(template.flexContainerName()).thenReturn("my-container");

    TemplateTestBase testBase = new TemplateTestBase() {};
    assertThat(testBase.getSpecPath(Object.class, template, "nonexistent-pom.xml"))
        .isEqualTo("gs://dataflow-templates-staging/2026-11-24-00_RC00/flex/My_Template");
  }
}
