/*
 * Copyright (C) 2025 Google LLC
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

import com.langchainbeam.LangchainBeam;
import com.langchainbeam.LangchainModelHandler;
import com.langchainbeam.model.LangchainBeamOutput;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

/**
 * LlmTransform a Beam PTransform to process input text using an LLM via LangchainBeam. It wraps
 * model execution logic for easy reuse in pipelines.
 */
public class LlmTransform {

  /**
   * Returns a {@link PTransform} that applies the given {@link LangchainModelHandler} to each
   * element in the input {@link PCollection} of strings, producing a {@link PCollection} of {@link
   * LangchainBeamOutput} containing both input and model output.
   *
   * @param handler an initialized LangchainModelHandler with model and prompt configuration
   * @return a Beam transform that applies the LLM to each input element
   */
  public static PTransform<PCollection<String>, PCollection<LangchainBeamOutput>> generate(
      LangchainModelHandler handler) {
    return LangchainBeam.run(handler);
  }
}
