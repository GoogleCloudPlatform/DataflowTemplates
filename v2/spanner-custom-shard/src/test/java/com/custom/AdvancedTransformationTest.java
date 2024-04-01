package com.custom;

import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationRequest;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AdvancedTransformationTest {

    @Test
    public void getSpannerRow() {
        Map<String, Object> sourceMap = new HashMap<>();
        sourceMap.put("double_column", 10.5);
        sourceMap.put("int_column", 5);
        sourceMap.put("date_column", "2024-04-01");
        MigrationTransformationRequest migrationTransformationRequest = new MigrationTransformationRequest("sample_table", sourceMap, "", "");
        AdvancedTransformation advancedTransformation = new AdvancedTransformation();
        MigrationTransformationResponse migrationTransformationResponse = advancedTransformation.toSpannerRow(migrationTransformationRequest);
        Map<String, Object> spannerMap = new HashMap<>();
        spannerMap.put("sum", 15.5);
        spannerMap.put("date_column", "2024-04-02");
        assertEquals(spannerMap, migrationTransformationResponse.getResponseRow());
    }
}
