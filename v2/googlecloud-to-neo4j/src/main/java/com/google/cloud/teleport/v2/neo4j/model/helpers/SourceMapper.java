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

import static com.google.cloud.teleport.v2.neo4j.model.helpers.JsonObjects.getStringOrDefault;
import static com.google.cloud.teleport.v2.neo4j.model.helpers.JsonObjects.getStringOrNull;

import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.model.sources.BigQuerySource;
import com.google.cloud.teleport.v2.neo4j.model.sources.ExternalTextSource;
import com.google.cloud.teleport.v2.neo4j.model.sources.InlineTextSource;
import com.google.cloud.teleport.v2.neo4j.model.sources.TextFormat;
import com.google.cloud.teleport.v2.neo4j.model.sources.TextSource;
import com.google.cloud.teleport.v2.neo4j.utils.ModelUtils;
import com.google.cloud.teleport.v2.neo4j.utils.TextParserUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.importer.v1.sources.Source;

/**
 * Helper class for parsing legacy json into {@link Source} model object.
 *
 * @deprecated use the current JSON format instead
 */
@Deprecated
public class SourceMapper {

  static final String DEFAULT_SOURCE_NAME = "";
  static final Pattern NEWLINE_PATTERN = Pattern.compile("\\R");

  public static List<Source> parse(JSONArray rawSources, OptionsParams options) {
    List<Source> sources = new ArrayList<>(rawSources.length());
    for (int i = 0; i < rawSources.length(); i++) {
      sources.add(parse(rawSources.getJSONObject(i), options));
    }
    return sources;
  }

  public static Source parse(JSONObject rawSource, OptionsParams options) {
    var sourceType = getStringOrDefault(rawSource, "type", "text").toLowerCase(Locale.ROOT);
    switch (sourceType) {
      case "bigquery":
        return parseBigQuerySource(rawSource, options);
      case "text":
        return parseTextSource(rawSource, options);
      default:
        throw new RuntimeException(String.format("Unsupported source type: %s", sourceType));
    }
  }

  private static BigQuerySource parseBigQuerySource(JSONObject rawSource, OptionsParams options) {
    var sourceName = getStringOrDefault(rawSource, "name", DEFAULT_SOURCE_NAME);
    var sql = ModelUtils.replaceVariableTokens(rawSource.getString("query"), options.getTokenMap());
    return new BigQuerySource(sourceName, sql);
  }

  private static TextSource parseTextSource(JSONObject rawSource, OptionsParams options) {
    var sourceName = getStringOrDefault(rawSource, "name", DEFAULT_SOURCE_NAME);
    var header =
        Arrays.asList(StringUtils.stripAll(rawSource.getString("ordered_field_names").split(",")));
    var format =
        TextFormat.valueOf(
            getStringOrDefault(rawSource, "format", "default").toUpperCase(Locale.ROOT));
    var delimiter = getStringOrDefault(rawSource, "delimiter", ",").substring(0, 1);
    var separator = getStringOrNull(rawSource, "separator");
    if (rawSource.has("uri") || rawSource.has("url")) {
      var url = rawSource.has("uri") ? rawSource.getString("uri") : rawSource.getString("url");
      url = ModelUtils.replaceVariableTokens(url, options.getTokenMap());
      return new ExternalTextSource(sourceName, List.of(url), header, format, delimiter, separator);
    }

    Object rawData = rawSource.get("data");
    var csvFormat = CsvSources.toCsvFormat(format);
    List<List<Object>> data;
    if (rawData instanceof JSONArray) {
      var array = (JSONArray) rawData;
      if (format == TextFormat.DEFAULT) {
        data = TextParserUtils.jsonToListOfListsArray(array);
      } else {
        String[] rows = TextParserUtils.jsonToListOfStringArray(array, delimiter);
        data = TextParserUtils.parseDelimitedLines(csvFormat, rows);
      }
    } else if (rawData instanceof String) {
      var content = (String) rawData;
      String[] rows;
      if (separator != null && content.contains(separator)) {
        rows = StringUtils.split(content, separator);
      } else {
        rows = NEWLINE_PATTERN.split(content);
      }
      data = TextParserUtils.parseDelimitedLines(csvFormat, rows);
    } else {
      throw new RuntimeException("data should either be a JSON array of array or plain string");
    }
    return new InlineTextSource(sourceName, data, header);
  }
}
