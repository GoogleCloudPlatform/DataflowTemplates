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
package com.google.cloud.teleport.v2.kafka.transforms;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.transforms.ErrorConverters;
import com.google.cloud.teleport.v2.transforms.ErrorConverters.PayloadExtractor;
import com.google.cloud.teleport.v2.transforms.ErrorConverters.WriteAbstractMessageErrors;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaRecordErrorConverters {
  private static final Logger LOG = LoggerFactory.getLogger(ErrorConverters.class);

  @AutoValue
  public abstract static class WriteKafkaRecordMessageErrors
      extends WriteAbstractMessageErrors<KafkaRecord<String, String>> {
    public abstract String getErrorRecordsTable();

    public abstract String getErrorRecordsTableSchema();

    @Override
    protected ErrorConverters.PayloadExtractor<KafkaRecord<String, String>>
        createPayloadExtractor() {
      return new KafkaRecordPayloadExtractor();
    }

    public static Builder newBuilder() {
      return new AutoValue_KafkaRecordErrorConverters_WriteKafkaRecordMessageErrors.Builder();
    }

    /** Builder for {@link WriteKafkaRecordMessageErrors}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setErrorRecordsTable(String errorRecordsTable);

      public abstract Builder setErrorRecordsTableSchema(String errorRecordsTableSchema);

      public abstract WriteKafkaRecordMessageErrors build();
    }
  }

  public static class KafkaRecordPayloadExtractor
      implements PayloadExtractor<KafkaRecord<String, String>>, Serializable {

    @Override
    public String getPayloadString(KafkaRecord<String, String> message) {
      ObjectWriter objectWriter = new ObjectMapper().writer().withDefaultPrettyPrinter();
      String payloadString = "";
      try {
        payloadString = objectWriter.writeValueAsString(message);
      } catch (Exception e) {
        LOG.error(
            "Unable to serialize record as JSON. Human readable record attempted via "
                + ".toString",
            e);
        try {
          payloadString = message.toString();
        } catch (Exception e2) {
          LOG.error(
              "Unable to serialize record via .toString. Human readable record will be " + "null",
              e2);
        }
      }
      return payloadString;
    }

    @Override
    public byte[] getPayloadBytes(KafkaRecord<String, String> message) {
      if (message == null) {
        return null;
      }
      return (message.getKV().getValue() == null
          ? "".getBytes(StandardCharsets.UTF_8)
          : message.getKV().getValue().getBytes(StandardCharsets.UTF_8));
    }
  }
}
