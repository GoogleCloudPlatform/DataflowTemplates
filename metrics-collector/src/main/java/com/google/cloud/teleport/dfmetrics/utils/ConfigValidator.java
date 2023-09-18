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

import com.google.cloud.teleport.dfmetrics.model.MetricsFetcherConfig;
import com.google.cloud.teleport.dfmetrics.model.Required;
import com.google.cloud.teleport.dfmetrics.model.TemplateLauncherConfig;
import com.google.cloud.teleport.dfmetrics.pipelinemanager.DataflowJobManager;
import java.lang.reflect.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class {@link ConfigValidator} validates the user supplied configuration values. */
public class ConfigValidator {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigValidator.class);

  private static <T> String validateForNulls(T obj) throws IllegalAccessException {
    Field[] declaredFields = obj.getClass().getDeclaredFields();

    StringBuilder nullFields = new StringBuilder();
    for (Field field : declaredFields) {
      Required annotation = field.getAnnotation(Required.class);
      if (annotation != null) {
        field.setAccessible(true); // Make sure private field is accessible
        if (field.get(obj) == null) {
          nullFields.append("," + field.getName());
        }
      }
    }
    return nullFields.length() == 0 ? "" : nullFields.substring(1);
  }

  public static boolean validateMetricsFetcherConfig(MetricsFetcherConfig metricsFetcherConfig)
      throws IllegalAccessException {
    String missingFields = validateForNulls(metricsFetcherConfig);

    if (!missingFields.isEmpty()) {
      throw new IllegalArgumentException(
          "Config fields:" + missingFields + " are mandatory and cannot have null values.");
    }
    return true;
  }

  public static void validateTemplateLauncherConfig(TemplateLauncherConfig templateLauncherConfig)
      throws IllegalArgumentException, IllegalAccessException {
    String missingFields = validateForNulls(templateLauncherConfig);

    if (!missingFields.isEmpty()) {
      throw new IllegalArgumentException(
          "Config fields:" + missingFields + " are mandatory and cannot have null values.");
    }
    // Check fields like pipeline options, environment options are empty or not
    if (templateLauncherConfig.getPipelineOptions() == null
        || templateLauncherConfig.getPipelineOptions().size() == 0) {
      throw new RuntimeException("Pipeline options cannot be empty.");
    }

    if (templateLauncherConfig.getEnvironmentOptions() == null
        || templateLauncherConfig.getEnvironmentOptions().size() == 0) {
      LOG.warn(
          "Environment options is empty. Various options such as subnetwork, service account, user"
              + " labels etcare provided through the environment options");
    }

    // Validate for specific values
    String templateType = templateLauncherConfig.getTemplateType().toLowerCase();
    if (!(templateType.equals("flex") || templateType.equals("classic"))) {
      throw new RuntimeException(
          "Invalid template type. Supported template types are flex,classic.");
    }

    // Set default values
    if (templateLauncherConfig.getTimeoutInMinutes() == null) {
      templateLauncherConfig.setTimeoutInMinutes(
          (int) DataflowJobManager.getDefaultTimeOut().toMinutes());
    }
  }
}
