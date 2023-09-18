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
package com.google.cloud.teleport.dfmetrics.pipelinemanager;

import com.google.auto.value.AutoValue;
import java.util.Map;

/** Class {@link TemplateConfig} represents the configuration for launching the template. */
@AutoValue
public abstract class TemplateConfig {

  /** Enum representing Apache Beam SDKs. */
  enum Sdk {
    JAVA("JAVA"),
    PYTHON("PYTHON"),
    GO("GO");

    private final String text;

    Sdk(String text) {
      this.text = text;
    }

    @Override
    public String toString() {
      return text;
    }
  }

  public abstract String jobName();

  public abstract String specPath();

  public abstract Sdk sdk();

  public abstract Map<String, String> parameters();

  public abstract Map<String, Object> environment();

  public abstract Builder toBuilder();

  public static Builder builder(String specPath) {
    return new AutoValue_TemplateConfig.Builder().setspecPath(specPath);
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setjobName(String value);

    public abstract Builder setspecPath(String value);

    public abstract Builder setsdk(Sdk value);

    public abstract Builder setparameters(Map<String, String> value);

    public abstract Builder setenvironment(Map<String, Object> value);

    public abstract TemplateConfig build();
  }

  public TemplateConfig withJobName(String value) {
    return toBuilder().setjobName(value).build();
  }
}
