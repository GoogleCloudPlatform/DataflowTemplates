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
package com.google.cloud.teleport.metadata.util;

import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import java.beans.Introspector;
import java.lang.annotation.Annotation;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** Utilities for working with template metadata. */
public final class MetadataUtils {

  private static final Class<? extends Annotation>[] PARAMETER_ANNOTATIONS =
      new Class[] {
        TemplateParameter.BigQueryTable.class,
        TemplateParameter.Boolean.class,
        TemplateParameter.DateTime.class,
        TemplateParameter.Double.class,
        TemplateParameter.Duration.class,
        TemplateParameter.Enum.class,
        TemplateParameter.Float.class,
        TemplateParameter.GcsReadFile.class,
        TemplateParameter.GcsReadFolder.class,
        TemplateParameter.GcsWriteFile.class,
        TemplateParameter.GcsWriteFolder.class,
        TemplateParameter.Integer.class,
        TemplateParameter.KmsEncryptionKey.class,
        TemplateParameter.Long.class,
        TemplateParameter.Password.class,
        TemplateParameter.ProjectId.class,
        TemplateParameter.PubsubSubscription.class,
        TemplateParameter.PubsubTopic.class,
        TemplateParameter.Text.class
      };
  public static final String BIGQUERY_TABLE_PATTERN = ".+[\\.:].+\\..+";

  private MetadataUtils() {}

  /**
   * Get the instance of TemplateParameter for a given object.
   *
   * @param accessibleObject Object (e.g., {@link Method}) to search the parameter.
   * @return Annotation if it is annotated, null otherwise.
   */
  public static Annotation getParameterAnnotation(AccessibleObject accessibleObject) {

    for (Class<? extends Annotation> annotation : PARAMETER_ANNOTATIONS) {
      if (accessibleObject.getAnnotation(annotation) != null) {
        return accessibleObject.getAnnotation(annotation);
      }
    }

    return null;
  }

  /** This method is inspired by {@code org.apache.beam.sdk.options.PipelineOptionsReflector}. */
  public static String getParameterNameFromMethod(String originalName) {
    String methodName;
    if (originalName.startsWith("is")) {
      methodName = originalName.substring(2);
    } else if (originalName.startsWith("get")) {
      methodName = originalName.substring(3);
    } else {
      methodName = originalName;
    }
    return Introspector.decapitalize(methodName);
  }

  /**
   * There are cases in which users will pass a gs://{bucketName} or a gs://{bucketName}/path
   * wrongly to a bucket name property. This will ensure that execution will run as expected
   * considering some input variations.
   *
   * @param bucketName User input with the bucket name.
   * @return Bucket name if parseable, or throw exception otherwise.
   * @throws IllegalArgumentException If bucket name can not be handled as such.
   */
  public static String bucketNameOnly(String bucketName) {

    String changedName = bucketName;
    // replace leading gs://
    if (changedName.startsWith("gs://")) {
      changedName = changedName.replaceFirst("gs://", "");
    }
    // replace trailing slash
    if (changedName.endsWith("/")) {
      changedName = changedName.replaceAll("/$", "");
    }

    if (changedName.contains("/") || changedName.contains(":")) {
      throw new IllegalArgumentException(
          "Bucket name "
              + bucketName
              + " is invalid. It should only contain the name of the bucket (not a path or URL).");
    }

    return changedName;
  }

  /** Get the list of regexes that needs to be used for the validation of a template parameter. */
  public static List<String> getRegexes(Annotation parameterAnnotation) {
    switch (parameterAnnotation.annotationType().getSimpleName()) {
      case "Text":
        TemplateParameter.Text simpleTextParam = (TemplateParameter.Text) parameterAnnotation;
        if (simpleTextParam.regexes() != null
            && simpleTextParam.regexes().length > 0
            && !(simpleTextParam.regexes().length == 1
                && simpleTextParam.regexes()[0].equals(""))) {
          return Arrays.asList(simpleTextParam.regexes());
        }
        return null;
      case "GcsReadFile":
        TemplateParameter.GcsReadFile gcsReadFileParam =
            (TemplateParameter.GcsReadFile) parameterAnnotation;
        return List.of("^gs:\\/\\/[^\\n\\r]+$");
      case "GcsReadFolder":
        TemplateParameter.GcsReadFolder gcsReadFolderParam =
            (TemplateParameter.GcsReadFolder) parameterAnnotation;
        return List.of("^gs:\\/\\/[^\\n\\r]+$");
      case "GcsWriteFile":
        TemplateParameter.GcsWriteFile gcsWriteFileParam =
            (TemplateParameter.GcsWriteFile) parameterAnnotation;
        return List.of("^gs:\\/\\/[^\\n\\r]+$");
      case "GcsWriteFolder":
        TemplateParameter.GcsWriteFolder gcsWriteFolderParam =
            (TemplateParameter.GcsWriteFolder) parameterAnnotation;
        return List.of("^gs:\\/\\/[^\\n\\r]+$");
      case "PubsubSubscription":
        TemplateParameter.PubsubSubscription pubsubSubscriptionParam =
            (TemplateParameter.PubsubSubscription) parameterAnnotation;
        return List.of("^projects\\/[^\\n\\r\\/]+\\/subscriptions\\/[^\\n\\r\\/]+$|^$");
      case "PubsubTopic":
        TemplateParameter.PubsubTopic pubsubTopicParam =
            (TemplateParameter.PubsubTopic) parameterAnnotation;
        return List.of("^projects\\/[^\\n\\r\\/]+\\/topics\\/[^\\n\\r\\/]+$|^$");
      case "Password":
        TemplateParameter.Password passwordParam = (TemplateParameter.Password) parameterAnnotation;
        return null;
      case "ProjectId":
        TemplateParameter.ProjectId projectIdParam =
            (TemplateParameter.ProjectId) parameterAnnotation;
        return List.of("[a-z0-9\\-\\.\\:]+");
      case "Boolean":
        TemplateParameter.Boolean booleanParam = (TemplateParameter.Boolean) parameterAnnotation;
        return List.of("^(true|false)$");
      case "Integer":
        TemplateParameter.Integer integerParam = (TemplateParameter.Integer) parameterAnnotation;
        return List.of("^[0-9]+$");
      case "Long":
        TemplateParameter.Long longParam = (TemplateParameter.Long) parameterAnnotation;
        return List.of("^[0-9]+$");
      case "Float":
        TemplateParameter.Float floatParam = (TemplateParameter.Float) parameterAnnotation;
        return List.of("^-?(0|[1-9][0-9]*)(\\.[0-9]+)?([eE][-+]?[0-9]+)?$");
      case "Double":
        TemplateParameter.Double doubleParam = (TemplateParameter.Double) parameterAnnotation;
        return List.of("^-?(0|[1-9][0-9]*)(\\.[0-9]+)?([eE][-+]?[0-9]+)?$");
      case "Enum":
        TemplateParameter.Enum enumParam = (TemplateParameter.Enum) parameterAnnotation;

        String optionsRegex =
            Arrays.stream(enumParam.enumOptions())
                .map(TemplateEnumOption::value)
                .collect(Collectors.joining("|"));

        return List.of("^(" + optionsRegex + ")$");
      case "DateTime":
        TemplateParameter.DateTime dateTimeParam = (TemplateParameter.DateTime) parameterAnnotation;
        return List.of(
            "^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):(([0-9]{2})(\\\\.[0-9]+)?)Z$");
      case "BigQueryTable":
        TemplateParameter.BigQueryTable bigQueryTableParam =
            (TemplateParameter.BigQueryTable) parameterAnnotation;
        return List.of(BIGQUERY_TABLE_PATTERN);
      case "KmsEncryptionKey":
        TemplateParameter.KmsEncryptionKey kmsEncryptionKeyParam =
            (TemplateParameter.KmsEncryptionKey) parameterAnnotation;
        return List.of(
            "^projects\\/[^\\n"
                + "\\r"
                + "\\/]+\\/locations\\/[^\\n"
                + "\\r"
                + "\\/]+\\/keyRings\\/[^\\n"
                + "\\r"
                + "\\/]+\\/cryptoKeys\\/[^\\n"
                + "\\r"
                + "\\/]+$");
      case "Duration":
        TemplateParameter.Duration durationParam = (TemplateParameter.Duration) parameterAnnotation;
        return List.of("^[1-9][0-9]*[s|m|h]$");
      default:
        return null;
    }
  }
}
