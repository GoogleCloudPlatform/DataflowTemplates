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
package com.google.cloud.teleport.v2.spanner.migrations.transformation;

import java.io.Serializable;

public class CustomTransformation implements Serializable {
  String jarPath;
  String classPath;

  String customParameters;

  public CustomTransformation(String jarPath, String classPath, String customParameters) {
    this.jarPath = jarPath;
    this.classPath = classPath;
    this.customParameters = customParameters;
  }

  public CustomTransformation() {}

  public String getJarPath() {
    return jarPath;
  }

  public String getClassPath() {
    return classPath;
  }

  public String getCustomParameters() {
    return customParameters;
  }
}
