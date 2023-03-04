/*
 * Copyright (C) 2023 Google LLC
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

import com.google.cloud.teleport.v2.options.PubsubToKafkaOptions;
import com.google.cloud.teleport.v2.templates.PubsubKafkaConstants;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

/** Different transformations over the processed data in the pipeline. */
public class FormatMessageTransform {

  /** Configures Pubsub consumer. */
  public static PTransform<PBegin, PCollection<String>> readFromPubsub(String topic) {
    PubsubIO.Read<String> pubsubRecords = PubsubIO.readStrings();
    return pubsubRecords.fromTopic(topic);
  }

  /**
   * The {@link MessageToFailsafeElementFn} wraps an Pubsub Message with the {@link FailsafeElement}
   * class so errors can be recovered from and the original message can be output to a error records
   * table.
   */
  static class MessageToFailsafeElementFn extends DoFn<String, FailsafeElement<String, String>> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      String message = context.element();
      context.output(FailsafeElement.of(message, message));
    }
  }

  /** The {@link UdfProcess} enables usage of the User Defined Functions in the pipeline. */
  public static class UdfProcess extends PTransform<PCollection<String>, PCollectionTuple> {

    private final PubsubToKafkaOptions options;

    public UdfProcess(PubsubToKafkaOptions options) {
      this.options = options;
    }

    @Override
    public PCollectionTuple expand(PCollection<String> input) {
      return input
          // Map the incoming messages into FailsafeElements so we can recover from failures
          // across multiple transforms.
          .apply("mapToRecord", ParDo.of(new MessageToFailsafeElementFn()))
          .apply(
              "invokeUDF",
              JavascriptTextTransformer.FailsafeJavascriptUdf.<String>newBuilder()
                  .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                  .setFunctionName(options.getJavascriptTextTransformFunctionName())
                  .setLoggingEnabled(true)
                  .setSuccessTag(PubsubKafkaConstants.UDF_OUT)
                  .setFailureTag(PubsubKafkaConstants.UDF_DEADLETTER_OUT)
                  .build());
    }
  }
}
