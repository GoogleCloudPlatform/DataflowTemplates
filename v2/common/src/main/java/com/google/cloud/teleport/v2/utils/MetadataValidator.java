/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.teleport.v2.utils;

import com.google.cloud.teleport.metadata.util.MetadataUtils;
import com.google.re2j.Pattern;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class provides common utility methods for validating Templates Metadata during runtime. */
public class MetadataValidator {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataValidator.class);

  private MetadataValidator() {}

  /**
   * Validates a given instance of {@link PipelineOptions}, and reports divergences for invalid
   * parameter usage.
   *
   * <p>Status: for now, errors/warnings will be provided but no exceptions thrown. This behavior
   * might change in the future.
   *
   * @param options Options instance to validate.
   */
  public static void validate(PipelineOptions options) {

    for (Method method : options.getClass().getMethods()) {

      Annotation parameterAnnotation = MetadataUtils.getParameterAnnotation(method);
      if (parameterAnnotation == null) {
        continue;
      }

      List<String> regexes = MetadataUtils.getRegexes(parameterAnnotation);
      if (regexes == null) {
        LOG.info(
            "Validation regex for method {} ({}) not specified.",
            method.getName(),
            parameterAnnotation.getClass().getSimpleName());
        continue;
      }

      for (String regex : regexes) {

        try {
          Object objectValue = method.invoke(options);
          if (objectValue == null || objectValue.toString().isEmpty()) {
            continue;
          }

          Pattern pattern = Pattern.compile(regex);
          if (!pattern.matches(objectValue.toString())) {
            LOG.warn(
                "Parameter {} ({}) not matching the expected format: {}",
                MetadataUtils.getParameterNameFromMethod(method.getName()),
                parameterAnnotation.getClass().getSimpleName(),
                regex);
          }

        } catch (Exception e) {
          LOG.warn(
              "Error validating method {} ({})",
              method.getName(),
              parameterAnnotation.getClass().getSimpleName());
        }
      }
    }
  }
}
