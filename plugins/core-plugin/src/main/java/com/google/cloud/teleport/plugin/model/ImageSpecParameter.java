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

import com.google.cloud.teleport.metadata.TemplateParameter;
import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.List;

/** Parameters in a template. */
public class ImageSpecParameter {

  private String name;
  private String label;
  private String helpText;
  private Boolean isOptional;
  private List<String> regexes;
  private ImageSpecParameterType paramType;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  public String getHelpText() {
    return helpText;
  }

  public void setHelpText(String helpText) {
    this.helpText = helpText;
  }

  public Boolean isOptional() {
    return isOptional;
  }

  public void setOptional(Boolean optional) {
    isOptional = optional;
  }

  public List<String> getRegexes() {
    return regexes;
  }

  public void setRegexes(List<String> regexes) {
    this.regexes = regexes;
  }

  public ImageSpecParameterType getParamType() {
    return paramType;
  }

  public void setParamType(ImageSpecParameterType parameterType) {
    this.paramType = parameterType;
  }

  public void processParamType(Annotation parameterAnnotation) {
    switch (parameterAnnotation.annotationType().getSimpleName()) {
      case "Text":
        TemplateParameter.Text simpleTextParam = (TemplateParameter.Text) parameterAnnotation;
        if (!simpleTextParam.name().isEmpty()) {
          this.setName(simpleTextParam.name());
        }
        processDescriptions(
            simpleTextParam.description(), simpleTextParam.helpText(), simpleTextParam.example());
        this.setOptional(simpleTextParam.optional());
        this.setParamType(ImageSpecParameterType.TEXT);

        if (simpleTextParam.regexes() != null
            && simpleTextParam.regexes().length > 0
            && !(simpleTextParam.regexes().length == 1
                && simpleTextParam.regexes()[0].equals(""))) {
          this.setRegexes(Arrays.asList(simpleTextParam.regexes()));
        }

        break;
      case "GcsReadFile":
        TemplateParameter.GcsReadFile gcsReadFileParam =
            (TemplateParameter.GcsReadFile) parameterAnnotation;
        if (!gcsReadFileParam.name().isEmpty()) {
          this.setName(gcsReadFileParam.name());
        }
        processDescriptions(
            gcsReadFileParam.description(),
            gcsReadFileParam.helpText(),
            gcsReadFileParam.example());
        this.setOptional(gcsReadFileParam.optional());
        this.setRegexes(List.of("^gs:\\/\\/[^\\n\\r]+$"));
        this.setParamType(ImageSpecParameterType.GCS_READ_FILE);
        break;
      case "GcsReadFolder":
        TemplateParameter.GcsReadFolder gcsReadFolderParam =
            (TemplateParameter.GcsReadFolder) parameterAnnotation;
        if (!gcsReadFolderParam.name().isEmpty()) {
          this.setName(gcsReadFolderParam.name());
        }
        processDescriptions(
            gcsReadFolderParam.description(),
            gcsReadFolderParam.helpText(),
            gcsReadFolderParam.example());
        this.setOptional(gcsReadFolderParam.optional());
        this.setRegexes(List.of("^gs:\\/\\/[^\\n\\r]+$"));
        this.setParamType(ImageSpecParameterType.GCS_READ_FOLDER);
        break;
      case "GcsWriteFile":
        TemplateParameter.GcsWriteFile gcsWriteFileParam =
            (TemplateParameter.GcsWriteFile) parameterAnnotation;
        if (!gcsWriteFileParam.name().isEmpty()) {
          this.setName(gcsWriteFileParam.name());
        }
        processDescriptions(
            gcsWriteFileParam.description(),
            gcsWriteFileParam.helpText(),
            gcsWriteFileParam.example());
        this.setOptional(gcsWriteFileParam.optional());
        this.setRegexes(List.of("^gs:\\/\\/[^\\n\\r]+$"));
        this.setParamType(ImageSpecParameterType.GCS_WRITE_FILE);
        break;
      case "GcsWriteFolder":
        TemplateParameter.GcsWriteFolder gcsWriteFolderParam =
            (TemplateParameter.GcsWriteFolder) parameterAnnotation;
        if (!gcsWriteFolderParam.name().isEmpty()) {
          this.setName(gcsWriteFolderParam.name());
        }
        processDescriptions(
            gcsWriteFolderParam.description(),
            gcsWriteFolderParam.helpText(),
            gcsWriteFolderParam.example());
        this.setOptional(gcsWriteFolderParam.optional());
        this.setRegexes(List.of("^gs:\\/\\/[^\\n\\r]+$"));
        this.setParamType(ImageSpecParameterType.GCS_WRITE_FOLDER);
        break;
      case "PubsubSubscription":
        TemplateParameter.PubsubSubscription pubsubSubscriptionParam =
            (TemplateParameter.PubsubSubscription) parameterAnnotation;
        if (!pubsubSubscriptionParam.name().isEmpty()) {
          this.setName(pubsubSubscriptionParam.name());
        }
        processDescriptions(
            pubsubSubscriptionParam.description(),
            pubsubSubscriptionParam.helpText(),
            pubsubSubscriptionParam.example());
        this.setOptional(pubsubSubscriptionParam.optional());
        this.setRegexes(List.of("^projects\\/[^\\n\\r\\/]+\\/subscriptions\\/[^\\n\\r\\/]+$|^$"));
        this.setParamType(ImageSpecParameterType.PUBSUB_SUBSCRIPTION);
        break;
      case "PubsubTopic":
        TemplateParameter.PubsubTopic pubsubTopicParam =
            (TemplateParameter.PubsubTopic) parameterAnnotation;
        if (!pubsubTopicParam.name().isEmpty()) {
          this.setName(pubsubTopicParam.name());
        }
        processDescriptions(
            pubsubTopicParam.description(),
            pubsubTopicParam.helpText(),
            pubsubTopicParam.example());
        this.setOptional(pubsubTopicParam.optional());
        this.setRegexes(List.of("^projects\\/[^\\n\\r\\/]+\\/topics\\/[^\\n\\r\\/]+$|^$"));
        this.setParamType(ImageSpecParameterType.PUBSUB_TOPIC);
        break;
      case "Password":
        TemplateParameter.Password passwordParam = (TemplateParameter.Password) parameterAnnotation;
        if (!passwordParam.name().isEmpty()) {
          this.setName(passwordParam.name());
        }
        processDescriptions(
            passwordParam.description(), passwordParam.helpText(), passwordParam.example());
        this.setOptional(passwordParam.optional());
        this.setParamType(ImageSpecParameterType.TEXT);
        break;
      case "ProjectId":
        TemplateParameter.ProjectId projectIdParam =
            (TemplateParameter.ProjectId) parameterAnnotation;
        if (!projectIdParam.name().isEmpty()) {
          this.setName(projectIdParam.name());
        }
        processDescriptions(
            projectIdParam.description(), projectIdParam.helpText(), projectIdParam.example());
        this.setOptional(projectIdParam.optional());
        // More specific? {"^([a-z0-9\\.]+:)?[a-z0-9][a-z0-9-]{5,29}$"}
        this.setRegexes(List.of("[a-z0-9\\-\\.\\:]+"));
        this.setParamType(ImageSpecParameterType.TEXT);
        break;
      case "Boolean":
        TemplateParameter.Boolean booleanParam = (TemplateParameter.Boolean) parameterAnnotation;
        if (!booleanParam.name().isEmpty()) {
          this.setName(booleanParam.name());
        }
        processDescriptions(
            booleanParam.description(), booleanParam.helpText(), booleanParam.example());
        this.setOptional(booleanParam.optional());
        this.setParamType(ImageSpecParameterType.TEXT);
        this.setRegexes(List.of("^(true|false)$"));
        break;
      case "Integer":
        TemplateParameter.Integer integerParam = (TemplateParameter.Integer) parameterAnnotation;
        if (!integerParam.name().isEmpty()) {
          this.setName(integerParam.name());
        }
        processDescriptions(
            integerParam.description(), integerParam.helpText(), integerParam.example());
        this.setOptional(integerParam.optional());
        this.setParamType(ImageSpecParameterType.TEXT);
        this.setRegexes(List.of("^[0-9]+$"));
        break;
      case "Long":
        TemplateParameter.Long longParam = (TemplateParameter.Long) parameterAnnotation;
        if (!longParam.name().isEmpty()) {
          this.setName(longParam.name());
        }
        processDescriptions(longParam.description(), longParam.helpText(), longParam.example());
        this.setOptional(longParam.optional());
        this.setParamType(ImageSpecParameterType.TEXT);
        this.setRegexes(List.of("^[0-9]+$"));
        break;
      case "Enum":
        TemplateParameter.Enum enumParam = (TemplateParameter.Enum) parameterAnnotation;
        if (!enumParam.name().isEmpty()) {
          this.setName(enumParam.name());
        }
        processDescriptions(enumParam.description(), enumParam.helpText(), enumParam.example());
        this.setOptional(enumParam.optional());
        this.setParamType(ImageSpecParameterType.TEXT);
        this.setRegexes(List.of("^(" + String.join("|", enumParam.enumOptions()) + ")$"));
        break;
      case "DateTime":
        TemplateParameter.DateTime dateTimeParam = (TemplateParameter.DateTime) parameterAnnotation;
        if (!dateTimeParam.name().isEmpty()) {
          this.setName(dateTimeParam.name());
        }
        processDescriptions(
            dateTimeParam.description(), dateTimeParam.helpText(), dateTimeParam.example());
        this.setOptional(dateTimeParam.optional());
        this.setParamType(ImageSpecParameterType.TEXT);
        this.setRegexes(
            List.of(
                "^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):(([0-9]{2})(\\\\.[0-9]+)?)Z$"));
        break;
      case "BigQueryTable":
        TemplateParameter.BigQueryTable bigQueryTableParam =
            (TemplateParameter.BigQueryTable) parameterAnnotation;
        if (!bigQueryTableParam.name().isEmpty()) {
          this.setName(bigQueryTableParam.name());
        }
        processDescriptions(
            bigQueryTableParam.description(),
            bigQueryTableParam.helpText(),
            bigQueryTableParam.example());
        this.setOptional(bigQueryTableParam.optional());
        this.setParamType(ImageSpecParameterType.TEXT);
        this.setRegexes(List.of(".+:.+\\..+"));
        break;
      case "KmsEncryptionKey":
        TemplateParameter.KmsEncryptionKey kmsEncryptionKeyParam =
            (TemplateParameter.KmsEncryptionKey) parameterAnnotation;
        if (!kmsEncryptionKeyParam.name().isEmpty()) {
          this.setName(kmsEncryptionKeyParam.name());
        }
        processDescriptions(
            kmsEncryptionKeyParam.description(),
            kmsEncryptionKeyParam.helpText(),
            kmsEncryptionKeyParam.example());
        this.setOptional(kmsEncryptionKeyParam.optional());
        this.setParamType(ImageSpecParameterType.TEXT);
        this.setRegexes(
            List.of(
                "^projects\\/[^\\n"
                    + "\\r"
                    + "\\/]+\\/locations\\/[^\\n"
                    + "\\r"
                    + "\\/]+\\/keyRings\\/[^\\n"
                    + "\\r"
                    + "\\/]+\\/cryptoKeys\\/[^\\n"
                    + "\\r"
                    + "\\/]+$"));
        break;
      case "Duration":
        TemplateParameter.Duration durationParam = (TemplateParameter.Duration) parameterAnnotation;
        if (!durationParam.name().isEmpty()) {
          this.setName(durationParam.name());
        }
        processDescriptions(
            durationParam.description(), durationParam.helpText(), durationParam.example());
        this.setOptional(durationParam.optional());
        this.setParamType(ImageSpecParameterType.TEXT);
        this.setRegexes(List.of("^[1-9][0-9]*[s|m|h]$"));
        break;
      default:
        throw new IllegalArgumentException("Invalid type " + parameterAnnotation);
    }
  }

  protected void processDescriptions(String description, String helpText, String example) {

    if (description != null) {
      this.setLabel(description);
    }

    if (helpText != null) {
      this.setHelpText(helpText);

      if (example != null && !example.isEmpty()) {
        this.setHelpText(this.getHelpText() + " (Example: " + example + ")");
      }
    }
  }

  public void validate() {
    if (getName() == null || getName().isEmpty()) {
      throw new IllegalArgumentException("Parameter name can not be empty.");
    }
    if (getLabel() == null || getLabel().isEmpty()) {
      throw new IllegalArgumentException("Parameter label can not be empty.");
    }
  }
}
