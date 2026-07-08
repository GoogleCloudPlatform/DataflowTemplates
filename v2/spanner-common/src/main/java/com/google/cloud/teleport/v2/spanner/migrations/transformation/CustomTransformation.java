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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.parquet.Strings;

@AutoValue
public abstract class CustomTransformation implements Serializable {
  public abstract String jarPath();

  public abstract String classPath();

  @Nullable
  public abstract String customParameters();

  public static CustomTransformation.Builder builder(String jarPath, String classPath) {
    return new AutoValue_CustomTransformation.Builder().setJarPath(jarPath).setClassPath(classPath);
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract CustomTransformation.Builder setJarPath(String jarPath);

    public abstract CustomTransformation.Builder setClassPath(String classPath);

    public abstract CustomTransformation.Builder setCustomParameters(String customParameters);

    abstract CustomTransformation autoBuild();

    public CustomTransformation build() {

      CustomTransformation customTransformation = autoBuild();
      checkState(
          (Strings.isNullOrEmpty(customTransformation.jarPath()))
              == (Strings.isNullOrEmpty(customTransformation.classPath())),
          "Both jarPath and classPath must be set or both must be empty/null.");
      return customTransformation;
    }
  }
}
