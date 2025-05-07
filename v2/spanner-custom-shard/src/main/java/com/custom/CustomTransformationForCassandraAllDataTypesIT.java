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
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class used only for Integration test. Do not use for production. */
public class CustomTransformationForCassandraAllDataTypesIT
    implements ISpannerMigrationTransformer {

  private static final Logger LOG =
      LoggerFactory.getLogger(CustomTransformationForCassandraAllDataTypesIT.class);

  @Override
  public void init(String parameters) {
    LOG.info("init called with {}", parameters);
  }

  @Override
  public MigrationTransformationResponse toSpannerRow(MigrationTransformationRequest request)
      throws InvalidTransformationException {
    LOG.info(
        "Got transformation request {}\ntableName = {}\nrow={}",
        request,
        request.getTableName(),
        request.getRequestRow());
    if (request.getTableName().toLowerCase().equals("all_data_types")) {
      Map<String, Object> row = new HashMap<>(request.getRequestRow());
      BigDecimal decimalCol = new BigDecimal(row.get("decimal_col").toString());
      LOG.info("DecimalCol is {}\nrow is {}", decimalCol, row);
      String newDecimalCol =
          decimalCol
              .divide((new BigDecimal(10)).pow(40))
              .setScale(4, RoundingMode.HALF_DOWN)
              .toString();
      row.remove("decimal_col");
      row.<String, String>put("decimal_col", newDecimalCol);
      LOG.info("NewDecimalCol is {}\n new row is {}\n", newDecimalCol, row);
      MigrationTransformationResponse response = new MigrationTransformationResponse(row, false);
      return response;
    }
    LOG.error("Returning request without processing {}", request);
    return new MigrationTransformationResponse(null, false);
  }

  @Override
  public MigrationTransformationResponse toSourceRow(MigrationTransformationRequest request)
      throws InvalidTransformationException {
    throw new UnsupportedOperationException(
        "This test custom transform is not intended for reverse replication.");
  }
}
