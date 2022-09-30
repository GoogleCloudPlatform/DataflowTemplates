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
package com.google.cloud.teleport.v2.neo4j.utils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utils for parsing text files. */
public class TextParserUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TextParserUtils.class);
  private static final DateTimeFormatter jsDateTimeFormatter =
      DateTimeFormat.forPattern("YYYY-MM-DD HH:MM:SSZ");
  private static final DateTimeFormatter jsDateFormatter = DateTimeFormat.forPattern("YYYY-MM-DD");
  private static final SimpleDateFormat jsTimeFormatter = new SimpleDateFormat("HH:MM:SS");

  public static List<Object> parseDelimitedLine(CSVFormat csvFormat, String line) {
    List<Object> textCols = new ArrayList<>();
    if (StringUtils.isEmpty(line)) {
      return null;
    }

    try (CSVParser csvParser = CSVParser.parse(line, csvFormat)) {
      // this is always going to be 1 row
      CSVRecord csvRecord = csvParser.getRecords().get(0);
      for (String s : csvRecord) {
        textCols.add(s);
      }
    } catch (IOException ioException) {
      LOG.error("Error parsing line {}", line, ioException);
    }
    return textCols;
  }

  // Function useful for small in-memory datasets
  public static List<List<Object>> parseDelimitedLines(CSVFormat csvFormat, String[] lines) {
    List<List<Object>> rows = new ArrayList<>();
    for (String line : lines) {
      rows.add(parseDelimitedLine(csvFormat, line));
    }
    return rows;
  }
}
