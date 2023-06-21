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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model;

import com.google.common.collect.ImmutableSet;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/** Descriptor of PubSub destination. */
public class PubSubDestination implements Serializable {
    private final String pubSubProject;
    private final String pubSubTopic;
    private final String messageFormat;
    private final String messageEncoding;

    public PubSubDestination(
            String pubSubProject, String pubSubTopic, String messageFormat, String messageEncoding) {
        this.pubSubProject = pubSubProject;
        this.pubSubTopic = pubSubTopic;
        this.messageFormat = messageFormat;
        this.messageEncoding = messageEncoding;
    }

    public String getPubSubProject() { return pubSubProject; }

    public String getPubSubTopic() {
        return pubSubTopic;
    }

    public String getMessageFormat() { return messageFormat; }

    public String getMessageEncoding() { return messageEncoding; }
}
