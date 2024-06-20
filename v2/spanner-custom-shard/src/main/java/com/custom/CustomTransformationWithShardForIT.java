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

import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationRequest;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomTransformationWithShardForIT implements ISpannerMigrationTransformer {

  private static final Logger LOG = LoggerFactory.getLogger(CustomShardIdFetcher.class);

  @Override
  public void init(String parameters) {
    LOG.info("init called with {}", parameters);
  }

  @Override
  public MigrationTransformationResponse toSpannerRow(MigrationTransformationRequest request)
      throws InvalidTransformationException {
    if (request.getTableName().equals("Customers")) {
      Map<String, Object> row = new HashMap<>(request.getRequestRow());
      row.put("full_name", row.get("first_name") + " " + row.get("last_name"));
      row.put("migration_shard_id", request.getShardId() + "_" + row.get("id"));
      MigrationTransformationResponse response = new MigrationTransformationResponse(row, false);
      return response;
    } else if (request.getTableName().equals("AllDatatypeTransformation")) {
      Map<String, Object> row = new HashMap<>(request.getRequestRow());
      // Filter event in case "varchar_column" = "example1"
      if (row.get("varchar_column").equals("example1")) {
        return new MigrationTransformationResponse(request.getRequestRow(), true);
      }
      // In case of update events, return request as response without any transformation
      if (request.getEventType().equals("UPDATE-INSERT")) {
        return new MigrationTransformationResponse(request.getRequestRow(), false);
      }
      // In case of backfill update the values for all the columns in all the rows except the
      // filtered row.
      row.put("tinyint_column", (Long) row.get("tinyint_column") + 1);
      row.put("text_column", row.get("text_column") + " append");
      row.put("int_column", (Long) row.get("int_column") + 1);
      row.put("bigint_column", (Long) row.get("bigint_column") + 1);
      row.put("float_column", (double) row.get("float_column") + 1);
      row.put("double_column", (double) row.get("double_column") + 1);
      Double value = Double.parseDouble((String) row.get("decimal_column"));
      row.put("decimal_column", String.valueOf(value + 1));
      row.put("time_column", (Long) row.get("time_column") + 1000);
      row.put("bool_column", 1);
      row.put("enum_column", "1");
      row.put("blob_column", "576f726d64");
      row.put("binary_column", "0102030405060708090A0B0C0D0E0F1011121314");
      row.put("bit_column", 13);
      row.put("year_column", (Long) row.get("year_column") + 1);
      try {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
        Date date = dateFormat.parse((String) row.get("date_column"));
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, 1);
        row.put("date_column", dateFormat.format(calendar.getTime()));
        Date dateTime = dateTimeFormat.parse((String) row.get("datetime_column"));
        calendar.setTime(dateTime);
        calendar.add(Calendar.SECOND, -1);
        row.put("datetime_column", dateTimeFormat.format(calendar.getTime()));
        dateTime = dateTimeFormat.parse((String) row.get("timestamp_column"));
        calendar.setTime(dateTime);
        calendar.add(Calendar.SECOND, -1);
        row.put("timestamp_column", dateTimeFormat.format(calendar.getTime()));

      } catch (Exception e) {
        throw new InvalidTransformationException(e);
      }

      MigrationTransformationResponse response = new MigrationTransformationResponse(row, false);
      return response;
    }
    return new MigrationTransformationResponse(request.getRequestRow(), false);
  }

  @Override
  public MigrationTransformationResponse toSourceRow(MigrationTransformationRequest request) {
    return null;
  }
}
