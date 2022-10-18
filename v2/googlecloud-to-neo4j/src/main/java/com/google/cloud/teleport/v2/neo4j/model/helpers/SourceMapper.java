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

import com.google.cloud.teleport.v2.neo4j.model.enums.SourceType;
import com.google.cloud.teleport.v2.neo4j.model.job.Source;
import com.google.cloud.teleport.v2.neo4j.utils.TextParserUtils;
import java.util.regex.Pattern;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper class for parsing json into Source model object. */
public class SourceMapper {

  static final Pattern NEWLINE_PATTERN = Pattern.compile("\\R");
  private static final Logger LOG = LoggerFactory.getLogger(SourceMapper.class);

  public static Source fromJson(JSONObject sourceObj) {
    Source source = new Source();
    source.setName(sourceObj.getString("name"));
    // TODO: avro, parquet, etc.
    source.setSourceType(
        sourceObj.has("type") ? SourceType.valueOf(sourceObj.getString("type")) : SourceType.text);

    boolean isJson = false;
    String formatStr =
        sourceObj.has("format") ? sourceObj.getString("format").toUpperCase() : "DEFAULT";
    if ("EXCEL".equals(formatStr)) {
      source.setCsvFormat(CSVFormat.EXCEL);
    } else if ("MONGO".equals(formatStr)) {
      source.setCsvFormat(CSVFormat.MONGODB_CSV);
    } else if ("INFORMIX".equals(formatStr)) {
      source.setCsvFormat(CSVFormat.INFORMIX_UNLOAD_CSV);
    } else if ("POSTGRES".equals(formatStr)) {
      source.setCsvFormat(CSVFormat.POSTGRESQL_CSV);
    } else if ("MYSQL".equals(formatStr)) {
      source.setCsvFormat(CSVFormat.MYSQL);
    } else if ("ORACLE".equals(formatStr)) {
      source.setCsvFormat(CSVFormat.ORACLE);
    } else if ("MONGO_TSV".equals(formatStr)) {
      source.setCsvFormat(CSVFormat.MONGODB_TSV);
    } else if ("RFC4180".equals(formatStr)) {
      source.setCsvFormat(CSVFormat.RFC4180);
    } else if ("POSTGRESQL_CSV".equals(formatStr)) {
      source.setCsvFormat(CSVFormat.POSTGRESQL_CSV);
    } else {
      source.setCsvFormat(CSVFormat.DEFAULT);
    }

    source.setDelimiter(
        sourceObj.has("delimiter") ? sourceObj.getString("delimiter") : source.getDelimiter());
    source.setSeparator(
        sourceObj.has("separator") ? sourceObj.getString("separator") : source.getSeparator());
    // handle inline data
    if (sourceObj.has("data")) {
      if (sourceObj.get("data") instanceof JSONArray) {

        if (source.getCsvFormat() == CSVFormat.DEFAULT) {
          source.setInline(Source.jsonToListOfListsArray(sourceObj.getJSONArray("data")));
        } else {
          String[] rows =
              Source.jsonToListOfStringArray(sourceObj.getJSONArray("data"), source.getDelimiter());
          source.setInline(TextParserUtils.parseDelimitedLines(source.getCsvFormat(), rows));
        }

      } else {
        String csv = sourceObj.getString("data");
        String[] rows;
        if (source.getSeparator() != null && csv.contains(source.getSeparator())) {
          rows = StringUtils.split(csv, source.getSeparator());
          // we may have more luck with varieties of newline
        } else {
          rows = NEWLINE_PATTERN.split(csv);
        }
        if (rows.length < 2) {
          String errMsg = "Cold not parse inline data.  Check separator: " + source.getSeparator();
          LOG.error(errMsg);
          throw new RuntimeException(errMsg);
        }
        source.setInline(TextParserUtils.parseDelimitedLines(source.getCsvFormat(), rows));
      }
    }
    source.setQuery(sourceObj.has("query") ? sourceObj.getString("query") : "");
    // uri or url accepted
    source.setUri(
        sourceObj.has("uri")
            ? sourceObj.getString("uri")
            : sourceObj.has("url") ? sourceObj.getString("url") : "");
    String colNamesStr =
        sourceObj.has("ordered_field_names") ? sourceObj.getString("ordered_field_names") : "";
    if (StringUtils.isNotEmpty(colNamesStr)) {
      source.setFieldNames(StringUtils.split(colNamesStr, ","));
      for (int i = 0; i < source.getFieldNames().length; i++) {
        source.getFieldPosByName().put(source.getFieldNames()[i], (i + 1));
      }
    }
    if (StringUtils.isNotEmpty(source.getDelimiter())) {
      source.getCsvFormat().withDelimiter(source.getDelimiter().charAt(0));
    }
    return source;
  }
}
