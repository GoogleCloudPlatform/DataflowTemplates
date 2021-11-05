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
package com.google.cloud.teleport.newrelic.config;

import com.google.cloud.teleport.templates.common.PubsubConverters;

/**
 * The {@link PubsubToNewRelicPipelineOptions} class provides the custom options passed by the
 * executor at the command line to execute the {@link
 * com.google.cloud.teleport.templates.PubsubToNewRelic} template. It includes: - The options to
 * read from a Pubsub subscription ({@link PubsubConverters.PubsubReadSubscriptionOptions} - The New
 * Relic-specific options to send logs to New Relic Logs ({@link NewRelicPipelineOptions}.
 */
public interface PubsubToNewRelicPipelineOptions
    extends NewRelicPipelineOptions, PubsubConverters.PubsubReadSubscriptionOptions {}
