/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.teleport.v2.elasticsearch.transforms;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.elasticsearch.utils.ElasticsearchIO.Write.BooleanFieldValueExtractFn;
import com.google.cloud.teleport.v2.elasticsearch.utils.ElasticsearchIO.Write.FieldValueExtractFn;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.JavascriptRuntime;
import java.io.IOException;
import javax.annotation.Nullable;
import javax.script.ScriptException;

/**
 * The {@link ValueExtractorTransform} allows for any Javascript function to be applied to a {@link
 * JsonNode}.
 *
 * <p>The transform takes in the path to the JS function via {@link
 * ValueExtractorFn#fileSystemPath()} and the name of the JS function to be applied via {@link
 * ValueExtractorFn#functionName()}. The transform will return a {@link String} that is the result
 * of the JS function.
 */
public class ValueExtractorTransform {

  /** Base Class for routing functions that implements {@link FieldValueExtractFn}. */
  public abstract static class ValueExtractorFn {
    @Nullable
    abstract String functionName();

    @Nullable
    abstract String fileSystemPath();

    @Nullable transient JavascriptRuntime runtime;

    abstract Object postProcessValue(String value);

    public Object apply(JsonNode input) {
      if (functionName() == null && fileSystemPath() == null) {
        return null;
      } else {
        checkArgument(
            functionName() != null && fileSystemPath() != null,
            "Both function name and file system path need to be set.");
      }

      if (runtime == null) {
        runtime =
            JavascriptRuntime.newBuilder()
                .setFunctionName(functionName())
                .setFileSystemPath(fileSystemPath())
                .build();
      }

      try {
        return postProcessValue(runtime.invoke(input.toString()));
      } catch (ScriptException | IOException | NoSuchMethodException e) {
        throw new RuntimeException("Error in processing field value extraction: " + e.getMessage());
      }
    }
  }

  /**
   * Class for routing functions that implement {@link FieldValueExtractFn}. {@link
   * ValueExtractorFn#apply(JsonNode)} will return null if {@link ValueExtractorFn#functionName()}
   * and {@link ValueExtractorFn#fileSystemPath()} are null meaning no function is applied to the
   * document.
   *
   * <p>If only one of {@link ValueExtractorFn#functionName()} or {@link
   * ValueExtractorFn#fileSystemPath()} are null then {@link
   * com.google.api.gax.rpc.InvalidArgumentException} is thrown.
   */
  @AutoValue
  public abstract static class StringValueExtractorFn extends ValueExtractorFn
      implements FieldValueExtractFn {
    public static Builder newBuilder() {
      return new AutoValue_ValueExtractorTransform_StringValueExtractorFn.Builder();
    }

    @Override
    String postProcessValue(String value) {
      return value;
    }

    @Override
    public String apply(JsonNode input) {
      return (String) super.apply(input);
    }

    /** Builder for {@link StringValueExtractorFn}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setFunctionName(String functionName);

      public abstract Builder setFileSystemPath(String fileSystemPath);

      public abstract StringValueExtractorFn build();
    }
  }

  /**
   * Class for routing functions that implement {@link BooleanFieldValueExtractFn}. {@link
   * BooleanValueExtractorFn#apply(JsonNode)} will return null if {@link
   * BooleanValueExtractorFn#functionName()} and {@link BooleanValueExtractorFn#fileSystemPath()}
   * are null meaning no function is applied to the document.
   *
   * <p>If only one of {@link BooleanValueExtractorFn#functionName()} or {@link
   * BooleanValueExtractorFn#fileSystemPath()} are null then {@link
   * com.google.api.gax.rpc.InvalidArgumentException} is thrown.
   */
  @AutoValue
  public abstract static class BooleanValueExtractorFn extends ValueExtractorFn
      implements BooleanFieldValueExtractFn {
    public static Builder newBuilder() {
      return new AutoValue_ValueExtractorTransform_BooleanValueExtractorFn.Builder();
    }

    @Override
    Boolean postProcessValue(String value) {
      return Boolean.valueOf(value);
    }

    @Override
    public Boolean apply(JsonNode input) {
      return (Boolean) super.apply(input);
    }

    /** Builder for {@link BooleanValueExtractorFn}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setFunctionName(String functionName);

      public abstract Builder setFileSystemPath(String fileSystemPath);

      public abstract BooleanValueExtractorFn build();
    }
  }
}
