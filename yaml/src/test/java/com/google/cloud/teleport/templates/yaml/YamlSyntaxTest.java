/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.templates.yaml;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.interpret.RenderResult;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test validates that all YAML files in the project are syntactically correct after Jinja
 * templating.
 */
@RunWith(JUnit4.class)
public class YamlSyntaxTest {
  private static final Logger LOG = LoggerFactory.getLogger(YamlSyntaxTest.class);

  @Test
  public void testAllYamlFilesAreWellFormed() throws IOException {
    // The ObjectMapper configured for YAML will be our parser
    ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
    Jinjava jinjava = new Jinjava();

    // Define the directory where your YAML templates are stored
    Path yamlDirectory = Paths.get("src", "main", "yaml");

    // Use Java's file streaming to find all YAML files
    try (Stream<Path> paths = Files.walk(yamlDirectory)) {
      paths
          .filter(Files::isRegularFile)
          .filter(path -> path.toString().endsWith(".yaml"))
          .forEach(
              path -> {
                try {
                  // Read the raw YAML content
                  String rawYamlContent = Files.readString(path);

                  // Dynamically create a context with 'placeholder' values for all found variables.
                  Map<String, Object> context = new HashMap<>();
                  RenderResult result = jinjava.renderForResult(rawYamlContent, new HashMap<>());
                  Set<String> variables = result.getContext().getResolvedExpressions();
                  LOG.info("Found variables: " + variables);
                  variables.forEach(variable -> context.put(variable, "placeholder"));

                  // Render the template with the 'placeholder' context
                  String renderedYamlContent = jinjava.render(rawYamlContent, context);
                  LOG.info("Rendered YAML content: " + renderedYamlContent);

                  // Now validate the rendered YAML content
                  Object ignored = yamlMapper.readValue(renderedYamlContent, Object.class);
                  LOG.info("Successfully validated: " + path);
                } catch (IOException e) {
                  throw new RuntimeException("Failed to parse YAML file: " + path, e);
                }
              });
    }
  }
}
