/*
 * Copyright (C) 2022 Google LLC
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
package com.infusionsoft.dataflow.transformers;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.infusionsoft.dataflow.dto.ColdEmail;
import com.infusionsoft.dataflow.utils.JavaTimeUtils;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class ColdEmailTransformer {

  public static ColdEmail fromEntity(
      Entity entity, @Nullable String htmlBody, @Nullable String textBody) {
    checkNotNull(entity, "entity must not be null");

    final Map<String, Value> properties = entity.getPropertiesMap();

    final ColdEmail dto = new ColdEmail();
    dto.setAccountId(properties.get("accountId").getStringValue());
    dto.setFromAddress(properties.get("fromAddress").getStringValue());
    dto.setToAddresses(
        properties.get("toAddresses").getArrayValue().getValuesList().stream()
            .map(Value::getStringValue)
            .collect(Collectors.toList()));
    dto.setSubject(properties.get("subject").getStringValue());
    dto.setHtmlBody(htmlBody);
    dto.setTextBody(textBody);
    dto.setCreated(JavaTimeUtils.toZonedDateTime(properties.get("created").getTimestampValue()));

    return dto;
  }
}
