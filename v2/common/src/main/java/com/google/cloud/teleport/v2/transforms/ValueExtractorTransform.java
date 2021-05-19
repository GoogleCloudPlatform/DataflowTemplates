/*
 * Copyright (C) 2019 Google Inc.
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
package com.google.cloud.teleport.v2.transforms;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.JavascriptRuntime;
import java.io.IOException;
import javax.annotation.Nullable;
import javax.script.ScriptException;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Write.FieldValueExtractFn;

/**
 * The {@link ValueExtractorTransform} allows for any Javascript function to be applied to a {@link JsonNode}.
 *
 * The transform takes in the path to the JS function via {@link ValueExtractorFn#fileSystemPath()}
 * and the name of JS function to be applied via {@link ValueExtractorFn#functionName()}. The transform will
 * return a {@link String} that is the result of the JS function.
 */
public class ValueExtractorTransform {

  /**
   * Class for routing functions that implements {@link FieldValueExtractFn}. {@link
   * ValueExtractorFn#apply(JsonNode)} will return null if {@link ValueExtractorFn#functionName()}
   * and {@link ValueExtractorFn#fileSystemPath()} are null meaning no function is applied to the
   * document.
   *
   * <p>If only one of {@link ValueExtractorFn#functionName()} or {@link
   * ValueExtractorFn#fileSystemPath()} are null then {@link com.google.api.gax.rpc.InvalidArgumentException} is thrown.
   */
  @AutoValue
  public abstract static class ValueExtractorFn implements FieldValueExtractFn {
    public static Builder newBuilder() {
      return new AutoValue_ValueExtractorTransform_ValueExtractorFn.Builder();
    }

    @Nullable
    abstract String functionName();

    @Nullable
    abstract String fileSystemPath();

    @Override
    public String apply(JsonNode input) {
      if (functionName() == null && fileSystemPath() == null) {
        return null;
      } else {
        checkArgument(
                functionName() != null && fileSystemPath() != null,
                "Both function name and file system path need to be set.");
      }

      JavascriptRuntime runtime =
              JavascriptRuntime.newBuilder()
                      .setFunctionName(functionName())
                      .setFileSystemPath(fileSystemPath())
                      .build();

      try {
        return runtime.invoke(input.toString());
      } catch (ScriptException | IOException | NoSuchMethodException e) {
        throw new RuntimeException("Error in processing field value extraction: " + e.getMessage());
      }
    }

    /** Builder for {@link ValueExtractorFn}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setFunctionName(String functionName);

      public abstract Builder setFileSystemPath(String fileSystemPath);

      public abstract ValueExtractorFn build();
    }
  }
}
