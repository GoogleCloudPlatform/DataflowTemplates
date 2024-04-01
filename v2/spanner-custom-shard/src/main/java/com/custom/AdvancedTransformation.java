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
package com.custom;

import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationRequest;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdvancedTransformation implements ISpannerMigrationTransformer {
  private static final Logger LOG = LoggerFactory.getLogger(AdvancedTransformation.class);

  @Override
  public void init(String customParameters) {
    LOG.info("init called with {}", customParameters);
  }

  @Override
  public MigrationTransformationResponse toSpannerRow(MigrationTransformationRequest request) {
    Map<String, Object> transformedRecord = new HashMap<>();
    if (request.getTableName().equals("sample_table")) {
      Map<String, Object> sourceRecord = request.getRequestRow();
      Double a = (Double) sourceRecord.get("double_column");
      Integer b = (Integer) sourceRecord.get("int_column");
      String d = (String) sourceRecord.get("date_column");
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

      // Parse the date
      LocalDate date = LocalDate.parse(d, formatter);

      // Add one day
      LocalDate newDate = date.plusDays(1);

      // Format the result
      String formattedDate = newDate.format(formatter);
      Double c = a+b;
      transformedRecord.put("sum", c);
      transformedRecord.put("date_column", formattedDate);
    }
    return new MigrationTransformationResponse(transformedRecord, false);
  }

  @Override
  public MigrationTransformationResponse toSourceRow(MigrationTransformationRequest request) {
    return new MigrationTransformationResponse(null, false);
  }
}
