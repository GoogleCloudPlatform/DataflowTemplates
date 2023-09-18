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
package com.google.cloud.teleport.dfmetrics.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.dfmetrics.model.MetricsFetcherConfig;
import com.google.cloud.teleport.dfmetrics.model.TemplateLauncherConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConfigValidatorTest {

  @Test(expected = Test.None.class /* no exception expected */)
  public void metricsFetcherConfigValid_success() throws IllegalAccessException {
    // Assign
    MetricsFetcherConfig metricsFetcherConfig = new MetricsFetcherConfig();
    metricsFetcherConfig.setJobId("dummyJobId");
    metricsFetcherConfig.setProject("dummyProject");
    metricsFetcherConfig.setRegion("dummyRegion");

    // Act
    ConfigValidator.validateMetricsFetcherConfig(metricsFetcherConfig);
  }

  @Test
  public void metricsFetcherConfigValid_missingjobId_ThrowsException()
      throws IllegalArgumentException {
    // Assign
    MetricsFetcherConfig metricsFetcherConfig = new MetricsFetcherConfig();
    metricsFetcherConfig.setProject("dummyProject");
    metricsFetcherConfig.setRegion("dummyRegion");
    String expectedMessage = "Config fields:jobId are mandatory and cannot have null values.";

    // Act
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> ConfigValidator.validateMetricsFetcherConfig(metricsFetcherConfig));

    // Assert
    assertThat(thrown.getMessage(), is(expectedMessage));
  }

  @Test
  public void metricsFetcherConfigValid_missingProject_ThrowsException()
      throws IllegalArgumentException {
    // Assign
    MetricsFetcherConfig metricsFetcherConfig = new MetricsFetcherConfig();
    metricsFetcherConfig.setJobId("jobId");
    metricsFetcherConfig.setRegion("dummyRegion");
    String expectedMessage = "Config fields:project are mandatory and cannot have null values.";

    // Act
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> ConfigValidator.validateMetricsFetcherConfig(metricsFetcherConfig));

    // Assert
    assertThat(thrown.getMessage(), is(expectedMessage));
  }

  @Test
  public void metricsFetcherConfigValid_nullRegion_ThrowsIllegalArgumentException()
      throws IllegalArgumentException {
    // Assign
    MetricsFetcherConfig metricsFetcherConfig = new MetricsFetcherConfig();
    metricsFetcherConfig.setJobId("jobId");
    metricsFetcherConfig.setRegion(null);
    metricsFetcherConfig.setProject("dummyProject");
    String expectedMessage = "Config fields:region are mandatory and cannot have null values.";

    // Act
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> ConfigValidator.validateMetricsFetcherConfig(metricsFetcherConfig));

    // Assert
    assertThat(thrown.getMessage(), is(expectedMessage));
  }

  @Test(expected = Test.None.class /* no exception expected */)
  public void templateLauncherConfigValid_allFieldsExists_Success()
      throws IllegalArgumentException, IllegalAccessException {
    // Assign
    Map<String, String> dummyPipelineOptions = new HashMap<>();
    Map<String, Object> dummyEnv = new HashMap<>();
    dummyPipelineOptions.put("dummyKey", "dummyValue");
    TemplateLauncherConfig templateLauncherConfig = new TemplateLauncherConfig();
    templateLauncherConfig.setJobName("jobName");
    templateLauncherConfig.setRegion("dummyRegion");
    templateLauncherConfig.setProject("dummyProject");
    templateLauncherConfig.setTemplateType("classic");
    templateLauncherConfig.setTemplateSpec("dummySpec");
    templateLauncherConfig.setEnvironmentOptions(dummyEnv);
    templateLauncherConfig.setPipelineOptions(dummyPipelineOptions);
    templateLauncherConfig.setJobPrefix("dummyPrefix");
    templateLauncherConfig.setTemplateVersion("dummyVersion");
    templateLauncherConfig.setTimeoutInMinutes(5);

    // Act
    ConfigValidator.validateTemplateLauncherConfig(templateLauncherConfig);
  }

  @Test(expected = Test.None.class /* no exception expected */)
  public void templateLauncherConfigValid_minimumRequiresInput_Success()
      throws IllegalArgumentException, IllegalAccessException {
    // Assign
    Map<String, String> dummyPipelineOptions = new HashMap<>();
    dummyPipelineOptions.put("dummyKey", "dummyValue");
    TemplateLauncherConfig templateLauncherConfig = new TemplateLauncherConfig();
    templateLauncherConfig.setJobName("jobName");
    templateLauncherConfig.setRegion("dummyRegion");
    templateLauncherConfig.setProject("dummyProject");
    templateLauncherConfig.setTemplateType("flex");
    templateLauncherConfig.setTemplateSpec("dummySpec");
    templateLauncherConfig.setPipelineOptions(dummyPipelineOptions);

    // Act
    ConfigValidator.validateTemplateLauncherConfig(templateLauncherConfig);
  }

  @Test
  public void templateLauncherConfigValid_nullValues_IllegalArgumentException()
      throws IllegalArgumentException, IllegalAccessException {
    // Assign
    TemplateLauncherConfig templateLauncherConfig = new TemplateLauncherConfig();
    templateLauncherConfig.setJobName("jobName");
    templateLauncherConfig.setRegion(null);
    templateLauncherConfig.setProject("dummyProject");
    templateLauncherConfig.setTemplateType("dummyType");
    templateLauncherConfig.setTemplateSpec("dummySpec");
    templateLauncherConfig.setEnvironmentOptions(null);
    String expectedMessage = "Config fields:region are mandatory and cannot have null values.";

    // Act
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> ConfigValidator.validateTemplateLauncherConfig(templateLauncherConfig));

    // Assert
    assertThat(thrown.getMessage(), is(expectedMessage));
  }

  @Test
  public void templateLauncherConfigValid_missingRequiredProjectId_IllegalArgumentException()
      throws IllegalArgumentException {
    // Assign
    Map<String, String> dummyPipelineOptions = new HashMap<>();
    dummyPipelineOptions.put("dummyKey", "dummyValue");

    TemplateLauncherConfig templateLauncherConfig = new TemplateLauncherConfig();
    templateLauncherConfig.setRegion("dummyRegion");
    templateLauncherConfig.setTemplateType("dummyType");
    templateLauncherConfig.setTemplateSpec("dummySpec");
    templateLauncherConfig.setPipelineOptions(dummyPipelineOptions);
    String expectedMessage = "Config fields:project are mandatory and cannot have null values.";

    // Act
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> ConfigValidator.validateTemplateLauncherConfig(templateLauncherConfig));

    // Assert
    assertThat(thrown.getMessage(), is(expectedMessage));
  }
}
