/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.newrelic.ptransforms;

import com.google.cloud.teleport.newrelic.config.NewRelicPipelineOptions;
import com.google.cloud.teleport.newrelic.dofns.NewRelicLogRecordWriterFn;
import com.google.cloud.teleport.newrelic.dtos.NewRelicLogApiSendError;
import com.google.cloud.teleport.newrelic.dtos.NewRelicLogRecord;
import com.google.cloud.teleport.newrelic.dtos.coders.NewRelicLogApiSendErrorCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * Class {@link NewRelicIO} provides a {@link PTransform} that allows writing {@link
 * NewRelicLogRecord} records into a New Relic Logs API endpoint using HTTP POST requests. In the
 * event of an error, a {@link PCollection} of {@link NewRelicLogApiSendError} records are returned
 * for further processing or storing into a deadletter sink.
 */
public class NewRelicIO
    extends PTransform<PCollection<NewRelicLogRecord>, PCollection<NewRelicLogApiSendError>> {

  private final NewRelicPipelineOptions pipelineOptions;

  public NewRelicIO(NewRelicPipelineOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions;
  }

  @Override
  public PCollection<NewRelicLogApiSendError> expand(PCollection<NewRelicLogRecord> input) {
    return input
        .apply(
            "Distribute execution",
            DistributeExecution.withParallelism(pipelineOptions.getParallelism()))
        .apply(
          "Send logs to New Relic",
          ParDo.of(new NewRelicLogRecordWriterFn(pipelineOptions)))
        .setCoder(NewRelicLogApiSendErrorCoder.getInstance());
  }
}
