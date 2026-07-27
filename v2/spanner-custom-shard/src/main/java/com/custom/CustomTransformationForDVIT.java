package com.custom;

import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationRequest;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomTransformationForDVIT implements ISpannerMigrationTransformer {

  private static final Logger LOG = LoggerFactory.getLogger(CustomTransformationForDVIT.class);
  private String customParam = "";

  @Override
  public void init(String parameters) {
    LOG.info("init called with {}", parameters);
    if (parameters != null) {
      this.customParam = parameters;
    }
  }

  @Override
  public MigrationTransformationResponse toSpannerRow(MigrationTransformationRequest request)
      throws InvalidTransformationException {
    if (request.getTableName().equals("Users")) {
      Map<String, Object> row = request.getRequestRow();
      
      // Filter out records where age is 99
      if (row.get("age") != null && ((Number) row.get("age")).intValue() == 99) {
        return new MigrationTransformationResponse(new HashMap<>(), true);
      }

      Map<String, Object> responseRow = new HashMap<>(row);

      if (row.get("full_name") != null) {
        // Convert full_name to a hex string representation
        String name = row.get("full_name").toString();
        String val = name + customParam;
        responseRow.put("full_name", java.util.HexFormat.of().formatHex(val.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
      }

      if (row.get("created_at") != null) {
        // Parse created_at and add 1 hour as a data transformation
        java.time.Instant t = java.time.Instant.parse((String) row.get("created_at"));
        java.time.Instant shifted = t.plus(1, java.time.temporal.ChronoUnit.HOURS);
        Long micros = (shifted.getEpochSecond() * 1000000L) + (shifted.getNano() / 1000L);
        responseRow.put("created_at", "Time_" + micros);
      }


      return new MigrationTransformationResponse(responseRow, false);
    }
    return new MigrationTransformationResponse(null, false);
  }

  @Override
  public MigrationTransformationResponse toSourceRow(MigrationTransformationRequest request)
      throws InvalidTransformationException {
    return new MigrationTransformationResponse(null, false);
  }

  @Override
  public MigrationTransformationResponse transformFailedSpannerMutation(
      MigrationTransformationRequest request) throws InvalidTransformationException {
    return new MigrationTransformationResponse(null, false);
  }
}
