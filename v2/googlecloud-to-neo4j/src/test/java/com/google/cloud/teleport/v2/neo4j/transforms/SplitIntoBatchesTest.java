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
package com.google.cloud.teleport.v2.neo4j.transforms;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;

public class SplitIntoBatchesTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private final Schema schema = Schema.builder().addInt32Field("value").build();

  @Test
  public void batches_elements() {
    List<Row> input = Arrays.asList(row(1), row(2), row(3), row(4), row(5), row(6), row(7));

    PCollection<KV<Integer, Iterable<Row>>> batches =
        pipeline
            .apply(Create.of(input))
            .apply(CreateKvTransform.of(1))
            .apply(GroupByKey.create())
            .apply(ParDo.of(SplitIntoBatches.of(3)));

    PAssert.that(batches)
        .satisfies(
            (pairs) -> {
              List<Row> rows = new ArrayList<>();
              pairs.forEach(
                  pair -> {
                    Iterable<Row> batch = pair.getValue();
                    assertThat(batch).isNotNull();
                    assertThat(input).containsAtLeastElementsIn(batch);
                    for (Row row : batch) {
                      rows.add(row);
                    }
                  });
              assertThat(rows).containsExactlyElementsIn(input);
              return null;
            });

    pipeline.run();
  }

  @Test
  public void rejects_invalid_batch_size() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> SplitIntoBatches.of(0));
    assertThat(exception)
        .hasMessageThat()
        .isEqualTo("negative batch sizes are not permitted, 0 given");
    exception = assertThrows(IllegalArgumentException.class, () -> SplitIntoBatches.of(-1));
    assertThat(exception)
        .hasMessageThat()
        .isEqualTo("negative batch sizes are not permitted, -1 given");
  }

  private Row row(int value) {
    return Row.withSchema(schema).addValues(value).build();
  }
}
