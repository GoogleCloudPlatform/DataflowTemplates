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
package com.google.cloud.teleport.metadata;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that marks a helper image for a Dataflow Template. The helper images will use the same
 * underlying image as the template they are annotating.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface HelperImage {
  /**
   * Comma-separated list of files to include in Template image when building with Dockerfile. Only
   * works for YAML and XLANG types. Must be in the path of the build files, i.e. copied to target
   * folder.
   *
   * <p>Will be copied as such, using Docker command: COPY ${otherFiles} /template/
   */
  String filesToCopy() default "";

  /** Container name to stage. */
  String containerName();

  /** Language of base template image. */
  Template.TemplateType type() default Template.TemplateType.NONE;

  /** Override the entry point for the image. */
  String[] entryPoint();
}
