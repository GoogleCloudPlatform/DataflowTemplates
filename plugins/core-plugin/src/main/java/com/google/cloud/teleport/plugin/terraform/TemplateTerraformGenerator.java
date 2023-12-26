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
package com.google.cloud.teleport.plugin.terraform;

import com.google.cloud.teleport.plugin.model.ImageSpec;
import com.google.cloud.teleport.plugin.model.ImageSpecMetadata;
import com.google.cloud.teleport.plugin.model.ImageSpecParameter;
import com.google.cloud.teleport.plugin.model.ImageSpecParameterType;
import freemarker.template.Configuration;
import freemarker.template.DefaultObjectWrapperBuilder;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import freemarker.template.Version;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class that can generate terraform files based on template annotations. */
public class TemplateTerraformGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(TemplateTerraformGenerator.class);
  private static final String TEMPLATE_PATH_CLASSIC = "terraform-templates/google.tf";
  private static final String TEMPLATE_PATH_FLEX = "terraform-templates/google-beta.tf";

  private static final Version VERSION = Configuration.VERSION_2_3_32;
  private static final Configuration FREEMARKER_CONFIG = new Configuration(VERSION);

  static {
    DefaultObjectWrapperBuilder wrapperBuilder = new DefaultObjectWrapperBuilder(VERSION);
    FREEMARKER_CONFIG.setDefaultEncoding("UTF-8");
    FREEMARKER_CONFIG.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    FREEMARKER_CONFIG.setLogTemplateExceptions(true);
    FREEMARKER_CONFIG.setClassForTemplateLoading(TemplateTerraformGenerator.class, "/");
    FREEMARKER_CONFIG.setObjectWrapper(wrapperBuilder.build());
  }

  public static void process(ImageSpec imageSpec, OutputStream destination)
      throws IOException, TemplateException {
    Template template = templateOf(imageSpec);
    LOG.info("Generating terraform for template {}...", imageSpec.getMetadata().getInternalName());
    OutputStreamWriter writer = new OutputStreamWriter(destination);
    template.process(moduleOf(imageSpec), writer);
    destination.flush();
    destination.close();
  }

  private static Template templateOf(ImageSpec spec) throws IOException {
    if (spec.getMetadata().isFlexTemplate()) {
      return FREEMARKER_CONFIG.getTemplate(TEMPLATE_PATH_FLEX);
    }

    return FREEMARKER_CONFIG.getTemplate(TEMPLATE_PATH_CLASSIC);
  }

  private static TerraformModule moduleOf(ImageSpec imageSpec) {
    return TerraformModule.builder().setParameters(variablesOf(imageSpec.getMetadata())).build();
  }

  private static List<TerraformVariable> variablesOf(ImageSpecMetadata metadata) {
    List<TerraformVariable> result = new ArrayList<>();
    for (ImageSpecParameter parameter : metadata.getParameters()) {
      result.add(variableOf(parameter));
    }
    return result;
  }

  private static TerraformVariable variableOf(ImageSpecParameter parameter) {
    String defaultValue = parameter.getDefaultValue();
    if (parameter.isOptional() != null && parameter.isOptional() && defaultValue == null) {
      defaultValue = "";
    }
    return TerraformVariable.builder()
        .setName(parameter.getName())
        .setType(variableTypeOf(parameter.getParamType()))
        .setDescription(parameter.getHelpText())
        .setDefaultValue(defaultValue)
        .setRegexes(parameter.getRegexes())
        .build();
  }

  private static TerraformVariable.Type variableTypeOf(ImageSpecParameterType parameterType) {
    switch (parameterType) {
      case BOOLEAN:
        return TerraformVariable.Type.BOOL;
      case NUMBER:
        return TerraformVariable.Type.NUMBER;
      default:
        return TerraformVariable.Type.STRING;
    }
  }
}
