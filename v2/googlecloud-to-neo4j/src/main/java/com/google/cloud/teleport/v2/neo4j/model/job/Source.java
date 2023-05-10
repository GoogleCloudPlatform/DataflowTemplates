/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.model.job;

import com.google.cloud.teleport.v2.neo4j.model.enums.ActionExecuteAfter;
import com.google.cloud.teleport.v2.neo4j.model.enums.SourceType;
import com.google.cloud.teleport.v2.neo4j.utils.BeamUtils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.commons.csv.CSVFormat;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Source query metadata. */
public class Source implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(Source.class);
  private SourceType sourceType = SourceType.text;
  private String name = "";
  private String uri = "";
  private String delimiter = ",";
  // row separator
  private String separator;

  private String query = "";
  private CSVFormat csvFormat = CSVFormat.DEFAULT;
  private String[] fieldNames = new String[0];
  private Map<String, Integer> fieldPosByName = new HashMap<>();
  private List<List<Object>> inline = new ArrayList<>();

  private ActionExecuteAfter executeAfter = ActionExecuteAfter.preloads;
  private String executeAfterName = "";

  public static List<List<Object>> jsonToListOfListsArray(JSONArray lines) {
    if (lines == null) {
      return new ArrayList<>();
    }

    List<List<Object>> rows = new ArrayList<>();
    for (int i = 0; i < lines.length(); i++) {
      JSONArray rowArr = lines.getJSONArray(i);
      List<Object> tuples = new ArrayList<>();
      for (int j = 0; j < rowArr.length(); j++) {
        tuples.add(rowArr.optString(j));
      }
      rows.add(tuples);
    }
    return rows;
  }

  public static String[] jsonToListOfStringArray(JSONArray lines, String delimiter) {
    if (lines == null) {
      return new String[0];
    }

    String[] rows = new String[lines.length()];
    for (int i = 0; i < lines.length(); i++) {
      JSONArray rowArr = lines.getJSONArray(i);
      StringBuilder sb = new StringBuilder();
      for (int j = 0; j < rowArr.length(); j++) {
        if (j > 0) {
          sb.append(delimiter);
        }
        sb.append(rowArr.optString(j));
      }
      rows[i] = sb.toString();
    }
    return rows;
  }

  public Schema getTextFileSchema() {
    return BeamUtils.textToBeamSchema(fieldNames);
  }

  public SourceType getSourceType() {
    return sourceType;
  }

  public void setSourceType(SourceType sourceType) {
    this.sourceType = sourceType;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getUri() {
    return uri;
  }

  public void setUri(String uri) {
    this.uri = uri;
  }

  public String getDelimiter() {
    return delimiter;
  }

  public void setDelimiter(String delimiter) {
    this.delimiter = delimiter;
  }

  public String getSeparator() {
    return separator;
  }

  public void setSeparator(String separator) {
    this.separator = separator;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public CSVFormat getCsvFormat() {
    return csvFormat;
  }

  public void setCsvFormat(CSVFormat csvFormat) {
    this.csvFormat = csvFormat;
  }

  public String[] getFieldNames() {
    return fieldNames;
  }

  public void setFieldNames(String[] fieldNames) {
    this.fieldNames = fieldNames;
  }

  public Map<String, Integer> getFieldPosByName() {
    return fieldPosByName;
  }

  public void setFieldPosByName(Map<String, Integer> fieldPosByName) {
    this.fieldPosByName = fieldPosByName;
  }

  public List<List<Object>> getInline() {
    return inline;
  }

  public void setInline(List<List<Object>> inline) {
    this.inline = inline;
  }

  public ActionExecuteAfter getExecuteAfter() {
    return executeAfter;
  }

  public void setExecuteAfter(ActionExecuteAfter executeAfter) {
    this.executeAfter = executeAfter;
  }

  public String getExecuteAfterName() {
    return executeAfterName;
  }

  public void setExecuteAfterName(String executeAfterName) {
    this.executeAfterName = executeAfterName;
  }
}
