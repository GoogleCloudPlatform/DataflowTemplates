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
package com.google.cloud.teleport.plugin.docs;

import com.google.cloud.teleport.plugin.model.ImageSpecParameter;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/** Utility methods that can be used to generate docs / called statically from Freemarker. */
public final class TemplateDocsUtils {

  private static final Map<String, String> VARIABLE_INTERPOLATION_NAMES =
      new LinkedHashMap<>() {
        {
          put("Dataflow", "dataflow_name");
          put("BigQuery", "bigquery_name");
          put("Bigtable", "bigtable_name_short");
          put("Spanner", "spanner_name");
          put("Elasticsearch", "product_name_elasticsearch");
          put("Google Cloud", "gcp_name");
          put("MongoDB", "product_name_mongodb");
          put("Apache Beam", "apache_beam");
          put("Cloud Storage", "storage_name");
        }
      };

  /**
   * Prints the right side of a variable to set to a shell variable. The main idea is to make
   * scripts a bit cleaner and use quotes only when needed, or to show explicitly that the value is
   * empty.
   *
   * <p>In case there's no default, {@code <paramName>} is used.
   */
  public static String printDefaultValueVariable(ImageSpecParameter parameter) {
    if (parameter.getDefaultValue() == null) {
      return "<" + parameter.getName() + ">";
    }

    switch (parameter.getParamType()) {
      case NUMBER:
      case BOOLEAN:
        return parameter.getDefaultValue();
      default:
        if (StringUtils.isEmpty(parameter.getDefaultValue())) {
          return "\"\"";
        }

        if (parameter.getDefaultValue().contains(" ")
            || parameter.getDefaultValue().contains("#")) {
          return "\"" + parameter.getDefaultValue() + "\"";
        }
        return parameter.getDefaultValue();
    }
  }

  /**
   * Replace entries in the keys of {@link #VARIABLE_INTERPOLATION_NAMES} for its corresponding
   * value.
   */
  public static String replaceVariableInterpolationNames(String text) {
    for (Map.Entry<String, String> replaceEntry : VARIABLE_INTERPOLATION_NAMES.entrySet()) {
      // Replace only full word boundaries (\b)
      text =
          text.replaceAll(
              "\\b" + replaceEntry.getKey() + "\\b", "{{" + replaceEntry.getValue() + "}}");
    }
    return text;
  }
}
