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
package com.google.cloud.teleport.v2.neo4j.model.helpers;

import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.utils.FileSystemUtils;
import java.io.StringReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.neo4j.importer.v1.ImportSpecification;
import org.neo4j.importer.v1.ImportSpecificationDeserializer;
import org.neo4j.importer.v1.sources.Source;
import org.neo4j.importer.v1.validation.SpecificationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.YAMLException;

/**
 * Helper class for parsing import specification files, accepts file URI as entry point. Delegates
 * to {@link ImportSpecificationDeserializer} for non-legacy spec payloads.
 */
public class JobSpecMapper {
  private static final Logger LOG = LoggerFactory.getLogger(JobSpecMapper.class);

  public static ImportSpecification parse(String jobSpecUri, OptionsParams options) {
    String content = fetchContent(jobSpecUri);
    JSONObject spec = getJsonObject(content);

    if (!spec.has("version")) {
      return parseLegacyJobSpec(options, spec);
    }

    try {
      // TODO: interpolate runtime tokens into new spec elements
      return ImportSpecificationDeserializer.deserialize(new StringReader(content));
    } catch (SpecificationException e) {
      throw validationFailure(e);
    }
  }

  private static JSONObject getJsonObject(String content) {
    try {
      return new JSONObject(content);
    } catch (JSONException jsonException) {
      Yaml yaml = new Yaml(new SafeConstructor(new LoaderOptions()));
      try {
        Map<String, Object> yamlMap = yaml.load(content);
        return new JSONObject(yamlMap);
      } catch (YAMLException yamlException) {
        throw new IllegalArgumentException(
            "Parsing failed: content is neither valid JSON nor valid YAML."
                + "\nJSON parse error: "
                + jsonException.getMessage()
                + "\nYAML parse error: "
                + yamlException.getMessage(),
            yamlException);
      }
    }
  }

  private static String fetchContent(String jobSpecUri) {
    try {
      return FileSystemUtils.getPathContents(jobSpecUri);
    } catch (Exception e) {
      LOG.error("Unable to fetch Neo4j job specification from URI {}: ", jobSpecUri, e);
      throw new RuntimeException(e);
    }
  }

  @Deprecated
  private static ImportSpecification parseLegacyJobSpec(OptionsParams options, JSONObject spec) {
    LOG.debug("Converting legacy JSON job spec to new import specification format");
    var configuration = parseConfig(spec);
    var targets = extractTargets(spec);
    var actions = extractActions(spec);
    var index = indexLegacyJobSpec(targets, actions);
    var specification =
        new ImportSpecification(
            "0.legacy",
            configuration,
            parseSources(spec, options),
            TargetMapper.parse(
                targets,
                options,
                index,
                (boolean) configuration.getOrDefault("index_all_properties", false)),
            ActionMapper.parse(actions, options));
    try {
      ImportSpecificationDeserializer.validate(specification);
    } catch (SpecificationException e) {
      throw validationFailure(e);
    }
    return specification;
  }

  private static JobSpecIndex indexLegacyJobSpec(JSONArray targets, JSONArray actions) {
    var index = new JobSpecIndex();
    TargetMapper.index(targets, index);
    ActionMapper.index(actions, index);
    return index;
  }

  private static RuntimeException validationFailure(SpecificationException e) {
    return new RuntimeException("Unable to process Neo4j job specification", e);
  }

  private static Map<String, Object> parseConfig(JSONObject json) {
    return json.has("config") ? json.getJSONObject("config").toMap() : Collections.emptyMap();
  }

  private static List<Source> parseSources(JSONObject json, OptionsParams options) {
    if (json.has("source")) {
      return List.of(SourceMapper.parse(json.getJSONObject("source"), options));
    }
    if (json.has("sources")) {
      return SourceMapper.parse(json.getJSONArray("sources"), options);
    }
    return List.of();
  }

  private static JSONArray extractTargets(JSONObject spec) {
    if (!spec.has("targets")) {
      throw new IllegalArgumentException("could not find any targets");
    }
    return spec.getJSONArray("targets");
  }

  private static JSONArray extractActions(JSONObject spec) {
    if (!spec.has("actions")) {
      return new JSONArray(0);
    }
    return spec.getJSONArray("actions");
  }
}
