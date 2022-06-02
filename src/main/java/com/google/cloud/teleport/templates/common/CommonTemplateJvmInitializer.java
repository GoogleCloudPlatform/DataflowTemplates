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
package com.google.cloud.teleport.templates.common;

import com.google.auto.service.AutoService;
import com.google.cloud.teleport.options.CommonTemplateOptions;
import java.security.Security;
import javax.net.ssl.SSLServerSocketFactory;
import org.apache.beam.sdk.harness.JvmInitializer;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CommonTemplateJvmInitializer performs all the required steps to support CommonTemplateOptions.
 */
@AutoService(JvmInitializer.class)
public class CommonTemplateJvmInitializer implements JvmInitializer {

  private static final Logger LOG = LoggerFactory.getLogger(CommonTemplateJvmInitializer.class);

  @Override
  public void onStartup() {}

  @Override
  public void beforeProcessing(PipelineOptions options) {
    CommonTemplateOptions pipelineOptions = options.as(CommonTemplateOptions.class);
    if (pipelineOptions.getDisabledAlgorithms() != null
        && pipelineOptions.getDisabledAlgorithms().get() != null) {
      String value = pipelineOptions.getDisabledAlgorithms().get();
      // if the user sets disabledAlgorithms to "none" then set "jdk.tls.disabledAlgorithms" to ""
      if (value.equals("none")) {
        value = "";
      }
      LOG.info("disabledAlgorithms is set to {}.", value);
      Security.setProperty("jdk.tls.disabledAlgorithms", value);
      SSLServerSocketFactory fact = (SSLServerSocketFactory) SSLServerSocketFactory.getDefault();
      LOG.info("Supported Ciper Suites: " + String.join(", ", fact.getSupportedCipherSuites()));
    }
  }
}
