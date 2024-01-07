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
package com.google.cloud.teleport.v2.templates.processing.handler;

import com.google.cloud.teleport.v2.templates.common.ProcessingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Gives the concrete streaming handler. */
public class StreamingHandlerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingHandlerFactory.class);

  public static StreamingHandler getStreamingHandler(ProcessingContext taskContext) {
    if ("pubsub".equals(taskContext.getBufferType())) {
      LOG.info("Getting the PubSub handler");
      return new PubSubToSourceStreamingHandler(taskContext);
    } else if ("kafka".equals(taskContext.getBufferType())) {
      LOG.info("Getting the Kafka handler");
      return new KafkaToSourceStreamingHandler(taskContext);
    }

    LOG.info("No handler found");
    return null;
  }
}
