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
package com.google.cloud.teleport.v2.utils;

import com.google.ads.googleads.v10.services.GoogleAdsRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Functions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import com.google.protobuf.Descriptors;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Various utility functions to assist in parsing and transforming <a
 * href="https://developers.google.com/google-ads/api/docs/query/overview">Google Ads Query Language
 * queries</a>.
 */
public class GoogleAdsUtils {
  // This regex matches the minimal valid query grammar for the Google Ads Query Language.
  // See https://developers.google.com/google-ads/api/docs/query/grammar for details.
  private static final String SELECT = "\\b(?i:SELECT)\\b";
  private static final String FIELD_NAME = "[a-z][a-zA-Z0-9._]*";
  private static final String COMMA = "\\s*,\\s*";
  private static final String SELECT_CLAUSE =
      String.format("%s\\s*(%s(?:%s%s)*)", SELECT, FIELD_NAME, COMMA, FIELD_NAME);
  private static final String FROM = "\\b(?i:FROM)\\b";
  private static final String RESOURCE_NAME = "[a-z][a-zA-Z_]*";
  private static final String FROM_CLAUSE = String.format("%s\\s*(%s)", FROM, RESOURCE_NAME);
  private static final String MINIMAL_QUERY =
      String.format("\\s*%s\\s*%s", SELECT_CLAUSE, FROM_CLAUSE);
  private static final Pattern MINIMAL_QUERY_PATTERN = Pattern.compile(MINIMAL_QUERY);

  private static final Splitter FIELD_PATH_SPLITTER = Splitter.on('.');

  private static final Descriptors.Descriptor GOOGLE_ADS_ROW_DESCRIPTOR =
      GoogleAdsRow.getDescriptor();

  private static String[] parseSelectedFields(String query) {
    Matcher matcher = MINIMAL_QUERY_PATTERN.matcher(query);

    if (!matcher.lookingAt()) {
      throw new IllegalArgumentException(
          String.format("Query has an invalid SELECT or FROM clause: %s", query));
    }

    return matcher.group(1).split(COMMA);
  }

  public static Map<String, Descriptors.FieldDescriptor> getSelectedFieldDescriptors(String query) {
    return Arrays.stream(parseSelectedFields(query))
        .collect(
            ImmutableMap.toImmutableMap(
                Functions.identity(),
                field -> {
                  Iterator<String> fieldPath = FIELD_PATH_SPLITTER.split(field).iterator();
                  String fieldName = fieldPath.next();
                  return Streams.stream(fieldPath)
                      .reduce(
                          GOOGLE_ADS_ROW_DESCRIPTOR.findFieldByName(fieldName),
                          (fieldDescriptor, name) ->
                              fieldDescriptor.getMessageType().findFieldByName(name),
                          (prev, next) -> next);
                }));
  }

  public static TableSchema createBigQuerySchema(String query) {
    return new TableSchema()
        .setFields(
            getSelectedFieldDescriptors(query).entrySet().stream()
                .map(
                    entry ->
                        SchemaUtils.convertProtoFieldDescriptorToBigQueryField(
                                entry.getValue(), true, null, 1)
                            .setName(entry.getKey().replace('.', '_')))
                .collect(Collectors.toList()));
  }
}
