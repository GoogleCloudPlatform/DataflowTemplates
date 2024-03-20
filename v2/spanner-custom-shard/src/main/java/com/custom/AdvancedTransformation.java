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

import com.google.cloud.teleport.v2.spanner.utils.DatastreamToSpannerFilterRequest;
import com.google.cloud.teleport.v2.spanner.utils.DatastreamToSpannerTransformationRequest;
import com.google.cloud.teleport.v2.spanner.utils.DatastreamToSpannerTransformationResponse;
import com.google.cloud.teleport.v2.spanner.utils.IDatastreamToSpannerTransformation;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdvancedTransformation implements IDatastreamToSpannerTransformation {
  private static final Logger LOG = LoggerFactory.getLogger(AdvancedTransformation.class);

  @Override
  public void init(String customParameters, Map<String, List<String>> transformColumns) {
    LOG.info("init called with {}", customParameters);
  }

  @Override
  public DatastreamToSpannerTransformationResponse applyAdvancedTransformation(
      DatastreamToSpannerTransformationRequest request) {
    Map<String, Object> sourceRecord = request.getSourceRecord();
    Map<String, Object> transformedRecord = new HashMap<>();
    transformedRecord.put("name", sourceRecord.get("name") + "123");
    return new DatastreamToSpannerTransformationResponse(transformedRecord);
  }

  @Override
  public boolean filterRecord(DatastreamToSpannerFilterRequest request) {
    return true;
  }
}
