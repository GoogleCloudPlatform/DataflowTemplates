/*
 * Copyright (C) 2020 Google Inc.
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

import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * The {@link PubsubToFailsafeElement} wraps an incoming {@link PubsubMessage} with the {@link
 * FailsafeElement} class so errors can be recovered from and the original message can be output to
 * a error records table.
 */
public class PubSubToFailSafeElement
    extends DoFn<PubsubMessage, FailsafeElement<String, String>> {
  @ProcessElement
  public void processElement(ProcessContext context) {
    PubsubMessage message = context.element();
    context.output(
        FailsafeElement.of(new String(message.getPayload(), StandardCharsets.UTF_8),
                               new String(message.getPayload(), StandardCharsets.UTF_8)));
  }

  public static PubSubToFailSafeElement create() {
    return new PubSubToFailSafeElement();
  }
}
