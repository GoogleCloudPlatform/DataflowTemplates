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

import java.util.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import com.google.gson.Gson;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CombineJsonLines {

  private static final Logger LOG = LoggerFactory.getLogger(CombineJsonLines.class);

  // String Accumulator Class
  // Handles batching and merging String objects
  public static class StringAccum implements Combine.AccumulatingCombineFn.Accumulator<String, StringAccum, String> {

    private List<String> strings = new ArrayList<>();

    public StringAccum() {
      super();
    }

    public StringAccum(String initialValue) {
      super();
      addInput(initialValue);
    }

    @Override
    public void addInput(String input) {
      strings.add(input);
    }

    @Override
    public void mergeAccumulator(StringAccum other) {
      strings.addAll(other.strings);
    }

    @Override
    public String extractOutput() {
      // TODO: Logic should be reviewed
      if (strings.isEmpty()) {
        return "";
      }

      // TODO: remove code below
      // TODO: do not just randomly choose 1
      StringBuffer buf = new StringBuffer();
      String result;
      int i = 0;
      for (String row : strings) {
        if (i > 0) {
          buf.append(",");
        }
        buf.append(row);
        i++;
      }
      result = buf.toString();

      // LOG.info("Logging Manual Value");
      // LOG.info(result);
      strings = new ArrayList<>();
      return result;
    }
  }

  // Code Class Handles Encoding and Decoding String Objects
  static class StringAccumCoder extends AtomicCoder<StringAccum> {

    private static final Coder<String> STRING_CODER = StringUtf8Coder.of();

    @Override
    public void encode(StringAccum value, OutputStream outStream) throws IOException {
      STRING_CODER.encode(value.extractOutput(), outStream);
    }

    @Override
    public StringAccum decode(InputStream inStream) throws IOException {
      return new StringAccum(STRING_CODER.decode(inStream));
    }
  }

  // Combine JSON Class Pulls together Accumulator and Coder Classes
  public static class CombineJsonLinesFn extends Combine.AccumulatingCombineFn<String, StringAccum, String> {
    @Override
    public StringAccum createAccumulator() {
        return new StringAccum();
    }

    @Override
    public Coder<StringAccum> getAccumulatorCoder(CoderRegistry registry, Coder<String> inputCoder) {
        return new StringAccumCoder();
    }
  }
}