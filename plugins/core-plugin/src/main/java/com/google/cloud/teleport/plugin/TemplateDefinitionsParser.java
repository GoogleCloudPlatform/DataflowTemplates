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
package com.google.cloud.teleport.plugin;

import com.google.cloud.teleport.metadata.MultiTemplate;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.plugin.model.TemplateDefinitions;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.reflections.Reflections;

/**
 * Utility class that will be used to scan for {@link Template} or {@link MultiTemplate} classes in
 * the classpath.
 */
public final class TemplateDefinitionsParser {

  private TemplateDefinitionsParser() {}

  /**
   * Given a ClassLoader, this method will scan look for every class that is annotated with {@link
   * Template} or {@link MultiTemplate}, by using {@link Reflections}. It then wraps all the
   * annotations and class name in a {@link TemplateDefinitions} instance, puts them in a list and
   * returns to the caller.
   *
   * @param classLoader ClassLoader that should be used to scan for the annotations.
   * @return List with all definitions that could be found in the classpath.
   */
  public static List<TemplateDefinitions> scanDefinitions(ClassLoader classLoader) {

    List<TemplateDefinitions> definitions = new ArrayList<>();

    // Scan every @Template class
    Set<Class<?>> templates = new Reflections(classLoader).getTypesAnnotatedWith(Template.class);
    for (Class<?> templateClass : templates) {
      Template templateAnnotation = templateClass.getAnnotation(Template.class);
      definitions.add(new TemplateDefinitions(templateClass, templateAnnotation));
    }

    // Scan every @MultiTemplate class
    Set<Class<?>> multiTemplates =
        new Reflections(classLoader).getTypesAnnotatedWith(MultiTemplate.class);
    for (Class<?> multiTemplateClass : multiTemplates) {
      MultiTemplate multiTemplateAnnotation = multiTemplateClass.getAnnotation(MultiTemplate.class);
      for (Template templateAnnotation : multiTemplateAnnotation.value()) {
        definitions.add(new TemplateDefinitions(multiTemplateClass, templateAnnotation));
      }
    }

    return definitions;
  }

  /**
   * Parse the version of a template, given the stage prefix.
   *
   * @param stagePrefix GCS path to store the templates (e.g., templates/2023-03-03_RC00).
   * @return Only the last part, after replacing characters not allowed in labels.
   */
  public static String parseVersion(String stagePrefix) {
    String[] parts = stagePrefix.split("/");
    String lastPart = parts[parts.length - 1];

    // Replace not allowed chars (anything other than letters, digits, hyphen and underscore)
    return lastPart.toLowerCase().replaceAll("[^\\p{Ll}\\p{Lo}\\p{N}_-]", "_");
  }
}
