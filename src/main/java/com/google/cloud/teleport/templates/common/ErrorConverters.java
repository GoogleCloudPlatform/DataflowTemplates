/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.templates.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;

/** Transforms & DoFns & Options for Teleport Error logging. */
public class ErrorConverters {
  /** Options for Writing Errors to GCS. */
  public interface ErrorWriteOptions extends PipelineOptions {
    @Description("Pattern of where to write errors, ex: gs://mybucket/somepath/errors.txt")
    ValueProvider<String> getErrorWritePath();
    void setErrorWritePath(ValueProvider<String> errorWritePath);
  }

  /** An entry in the Error Log. */
  @AutoValue
  @JsonDeserialize(builder = ErrorMessage.Builder.class)
  public abstract static class ErrorMessage {
    @JsonProperty public abstract String message();
    @JsonProperty public abstract String textElementType();
    @JsonProperty public abstract String textElementData();

    /** Builder for {@link ErrorMessage}. */
    @AutoValue.Builder
    @JsonPOJOBuilder(withPrefix = "set")
    public abstract static class Builder {
      public abstract Builder setMessage(String message);
      public abstract Builder setTextElementType(String textElementType);
      public abstract Builder setTextElementData(String textElementData);
      public abstract ErrorMessage build();

      @JsonCreator
      public static Builder create() {
        return ErrorMessage.newBuilder();
      }
    }

    public static Builder newBuilder() {
      return new AutoValue_ErrorConverters_ErrorMessage.Builder();
    }

    public String toJson() throws JsonProcessingException {
      return new ObjectMapper().writeValueAsString(this);
    }

    public static ErrorMessage fromJson(String json) throws IOException {
      return new ObjectMapper().readValue(json, ErrorMessage.class);
    }
  }

  /** Writes all Errors to GCS, place at the end of your pipeline. */
  @AutoValue
  public abstract static class LogErrors extends PTransform<PCollectionTuple, PDone> {
    public abstract ValueProvider<String> errorWritePath();
    public abstract TupleTag<String> errorTag();

    /** Builder for {@link LogErrors}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setErrorWritePath(ValueProvider<String> errorWritePath);
      public abstract Builder setErrorTag(TupleTag<String> errorTag);
      public abstract LogErrors build();
    }

    public static Builder newBuilder() {
      return new AutoValue_ErrorConverters_LogErrors.Builder();
    }

    @Override
    public PDone expand(PCollectionTuple pCollectionTuple) {
      return pCollectionTuple.get(errorTag()).apply(TextIO.write()
          .to(errorWritePath())
          .withNumShards(1));
    }
  }

}
