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
import com.google.common.base.MoreObjects;
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
          put("Google Cloud", "gcp_name_short");
          put("MongoDB", "product_name_mongodb");
          put("Apache Beam", "apache_beam");
          put("Cloud Storage", "storage_name");
          put("Pub/Sub", "pubsub_name_short");
          put(
              "projects/your-project-id/subscriptions/your-subscription-name",
              "pubsub_subscription_format");
          put("projects/your-project-id/topics/your-topic-name", "pubsub_topic_format");
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

  /**
   * Wrap a string - inserts a line break of the line will become longer than the given length.
   * Optionally prepad new lines or convert lists to HTML format.
   */
  public static String wrapText(String text, int lineLength, String prepad, boolean htmlList) {
    prepad = MoreObjects.firstNonNull(prepad, "");

    StringBuilder result = new StringBuilder();

    boolean listMode = false;

    for (String textLine : text.split(System.lineSeparator())) {
      // Every line break is translated to a line break.
      if (result.length() > 0) {
        result.append(System.lineSeparator());
      }
      StringBuilder line = new StringBuilder();

      for (String word : textLine.split("\\s+")) {
        if (line.length() + word.length() > lineLength) {
          result.append(line).append(System.lineSeparator());
          line.setLength(0);
          line.append(prepad);
        } else if (line.length() > 0) {
          line.append(" ");
        }
        line.append(word);
      }

      if (htmlList) {
        if (line.toString().replaceAll(prepad, "").trim().startsWith("-")) {
          if (!listMode) {
            result.append(prepad + "<ul>").append(System.lineSeparator());
            listMode = true;
          }
          String noDash = line.toString().trim().substring(1).trim();
          result.append(prepad + "  <li>").append(noDash).append("</li>");
        } else if (listMode) {
          result.append(prepad + "</ul>").append(System.lineSeparator());
          result.append(line);
          listMode = false;
        } else {
          listMode = false;
          result.append(line);
        }
      } else {
        result.append(line);
      }
    }

    if (listMode) {
      result.append(System.lineSeparator()).append(prepad + "</ul>");
    }
    return result.toString();
  }

  /**
   * Replace site tags. Examples, defaults and backticks must be printed in the form of {@code
   * <code>text</code>} in the resulting doc.
   */
  public static String replaceSiteTags(String text) {
    return text.replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll("(?m)\\.? \\(Example: (.*?)\\)", ". For example: <code>$1</code>")
        .replaceAll("(?m)For example, \"(.*?)\"", "For example: <code>$1</code>")
        .replaceAll("(?m)Defaults to: (.*?)\\.", "Defaults to: <code>$1</code>.")
        .replaceAll("(?m)`(.*?)`", "<code>$1</code>");
  }
}
