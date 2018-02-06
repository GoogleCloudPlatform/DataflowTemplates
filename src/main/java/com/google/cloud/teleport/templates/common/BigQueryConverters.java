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

import com.google.api.services.bigquery.model.TableRow;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

/** Common Code for Teleport BigQueryIO. */
public class BigQueryConverters {

  /** Factory method for {@link JsonToTableRow}. */
  public static PTransform<PCollection<String>, PCollection<TableRow>> jsonToTableRow() {
    return new JsonToTableRow();
  }

  /** Converts UTF8 encoded Json records to TableRow records. */
  private static class JsonToTableRow
      extends PTransform<PCollection<String>, PCollection<TableRow>> {

    @Override
    public PCollection<TableRow> expand(PCollection<String> stringPCollection) {
      return stringPCollection.apply("JsonToTableRow", MapElements.<String, TableRow>via(
          new SimpleFunction<String, TableRow>() {
            @Override
            public TableRow apply(String json) {
                try {

                  InputStream inputStream = new ByteArrayInputStream(
                      json.getBytes(StandardCharsets.UTF_8.name()));

                  //OUTER is used here to prevent EOF exception
                  return TableRowJsonCoder.of().decode(inputStream, Context.OUTER);
                } catch (IOException e) {
                  throw new RuntimeException("Unable to parse input", e);
                }
            }
          }));
    }
  }
}
