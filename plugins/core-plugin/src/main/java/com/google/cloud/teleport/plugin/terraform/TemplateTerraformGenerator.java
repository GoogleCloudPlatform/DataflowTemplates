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
import com.google.cloud.teleport.plugin.model.ImageSpecParameter;
import com.google.common.annotations.VisibleForTesting;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class that can generate terraform files based on template annotations. */
public class TemplateTerraformGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(TemplateTerraformGenerator.class);
  private static final String VARIABLE_TEMPLATE = "terraform/variable-template.tf";
  private static final Configuration FREEMARKER_CONFIG =
      new Configuration(Configuration.VERSION_2_3_32);

  static {
    FREEMARKER_CONFIG.setDefaultEncoding("UTF-8");
    FREEMARKER_CONFIG.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    FREEMARKER_CONFIG.setLogTemplateExceptions(true);
    FREEMARKER_CONFIG.setClassForTemplateLoading(TemplateTerraformGenerator.class, "/");
  }

  public static void terraform(ImageSpec imageSpec, OutputStream destination) throws IOException {
    LOG.info("Generating terraform for template {}...", imageSpec.getMetadata().getInternalName());
    destination.flush();
    destination.close();
  }

  @VisibleForTesting
  static void variable(ImageSpecParameter parameter, OutputStreamWriter destination)
      throws IOException, TemplateException {
    TerraformVariable variable = TerraformVariable.from(parameter);
    Template template = FREEMARKER_CONFIG.getTemplate(VARIABLE_TEMPLATE);
    template.process(variable, destination);
  }
}
