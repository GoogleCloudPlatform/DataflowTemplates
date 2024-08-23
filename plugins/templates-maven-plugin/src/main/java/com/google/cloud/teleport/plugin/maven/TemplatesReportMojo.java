/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.plugin.maven;

import com.google.cloud.teleport.plugin.CoverageReportPomGenerator;
import freemarker.template.TemplateException;
import java.io.IOException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Mojo(
    name = "report",
    defaultPhase = LifecyclePhase.COMPILE,
    requiresDependencyResolution = ResolutionScope.COMPILE_PLUS_RUNTIME)
public class TemplatesReportMojo extends TemplatesBaseMojo {
  private static final Logger LOG = LoggerFactory.getLogger(TemplatesReportMojo.class);

  public void execute() {
    try {
      CoverageReportPomGenerator.generateCoverageReportPom(session.getExecutionRootDirectory());
    } catch (IOException | TemplateException e) {
      throw new RuntimeException(e);
    }
  }
}
