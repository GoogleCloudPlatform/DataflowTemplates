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

import com.google.cloud.teleport.plugin.model.ImageSpec;
import freemarker.ext.beans.BeansWrapper;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class that can generate documentation files based on template annotations. */
public class TemplateDocsGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(TemplateDocsGenerator.class);

  public static String readmeMarkdown(ImageSpec imageSpec) throws IOException, TemplateException {

    LOG.info(
        "Generating documentation for template {}...", imageSpec.getMetadata().getInternalName());

    Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_32);
    freemarkerConfig.setDefaultEncoding("UTF-8");
    freemarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    freemarkerConfig.setLogTemplateExceptions(true);
    freemarkerConfig.setClassForTemplateLoading(TemplateDocsGenerator.class, "/");

    Map<String, Object> parameters = new HashMap<>();
    parameters.put("spec", imageSpec);
    parameters.put("flex", imageSpec.getMetadata().isFlexTemplate());
    parameters.put("statics", BeansWrapper.getDefaultInstance().getStaticModels());

    Template template = freemarkerConfig.getTemplate("README-template.md");

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputStreamWriter writer = new OutputStreamWriter(baos)) {

      template.process(parameters, writer);
      writer.flush();

      return baos.toString(StandardCharsets.UTF_8);
    }
  }

  public static String siteMarkdown(ImageSpec imageSpec) throws IOException, TemplateException {

    LOG.info("Generating site for template {}...", imageSpec.getMetadata().getInternalName());

    Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_32);
    freemarkerConfig.setDefaultEncoding("UTF-8");
    freemarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    freemarkerConfig.setLogTemplateExceptions(true);
    freemarkerConfig.setClassForTemplateLoading(TemplateDocsGenerator.class, "/");

    Map<String, Object> parameters = new HashMap<>();
    parameters.put("spec", imageSpec);
    parameters.put("flex", imageSpec.getMetadata().isFlexTemplate());
    parameters.put("statics", BeansWrapper.getDefaultInstance().getStaticModels());
    parameters.put("base_include", "dataflow");

    Template template = freemarkerConfig.getTemplate("site-template.md");

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputStreamWriter writer = new OutputStreamWriter(baos)) {
      template.process(parameters, writer);
      writer.flush();

      return baos.toString(StandardCharsets.UTF_8);
    }
  }
}
