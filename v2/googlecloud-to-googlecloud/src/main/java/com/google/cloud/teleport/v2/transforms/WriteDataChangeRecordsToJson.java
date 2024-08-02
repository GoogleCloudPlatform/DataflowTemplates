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
package com.google.cloud.teleport.v2.transforms;

import com.google.auto.value.AutoValue;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link WriteDataChangeRecordsToGcsText} class is a {@link PTransform} that takes in {@link
 * PCollection} of Spanner data change records. The transform converts and writes these records to
 * GCS in JSON text file format.
 */
@AutoValue
public abstract class WriteDataChangeRecordsToJson {
  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(WriteDataChangeRecordsToJson.class);

  static class DataChangeRecordToJsonTextFn extends SimpleFunction<DataChangeRecord, String> {
    private static Gson gson = new Gson();

    private boolean includeSpannerResource = false;

    private String spannerDatabase;

    private String spannerInstanceId;

    public Boolean includeSpannerSource() {
      return includeSpannerResource;
    }

    public String spannerDatabase() {
      return spannerDatabase;
    }

    public String spannerInstanceId() {
      return spannerInstanceId;
    }

    public DataChangeRecordToJsonTextFn() {}

    private DataChangeRecordToJsonTextFn(Builder builder) {
      this.includeSpannerResource = builder.includeSpannerResource;
      this.spannerDatabase = builder.spannerDatabase;
      this.spannerInstanceId = builder.spannerInstanceId;
    }

    @Override
    public String apply(DataChangeRecord record) {
      if (includeSpannerSource()) {
        JsonElement jsonElement = gson.toJsonTree(record);
        jsonElement.getAsJsonObject().addProperty("spannerDatabase", spannerDatabase());
        jsonElement.getAsJsonObject().addProperty("spannerInstanceId", spannerInstanceId());
        return gson.toJson(jsonElement);
      }
      return gson.toJson(record, DataChangeRecord.class);
    }

    static class Builder {
      private boolean includeSpannerResource = false;
      private String spannerDatabase;

      private String spannerInstanceId;

      public Builder setIncludeSpannerSource(Boolean value) {
        this.includeSpannerResource = value;
        return this;
      }

      public Builder setSpannerDatabase(String value) {
        this.spannerDatabase = value;
        return this;
      }

      public Builder setSpannerInstanceId(String value) {
        this.spannerInstanceId = value;
        return this;
      }

      public DataChangeRecordToJsonTextFn build() {
        return new DataChangeRecordToJsonTextFn(this);
      }
    }
  }
}
