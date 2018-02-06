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

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * Options for beam TextIO.
 */
public class TextConverters {

  /** Options for reading data from a Filesystem. */
  public interface FilesystemReadOptions extends PipelineOptions {
    @Description("Pattern to where data lives, ex: gs://mybucket/somepath/*.json")
    ValueProvider<String> getTextReadPattern();
    void setTextReadPattern(ValueProvider<String> textReadPattern);
  }

  /** Options for writing data to a Filesystem. */
  public interface FilesystemWriteOptions extends PipelineOptions {
    @Description("Prefix path as to where data should be written, ex: gs://mybucket/somefolder/")
    ValueProvider<String> getTextWritePrefix();
    void setTextWritePrefix(ValueProvider<String> textWritePrefix);
  }
}
