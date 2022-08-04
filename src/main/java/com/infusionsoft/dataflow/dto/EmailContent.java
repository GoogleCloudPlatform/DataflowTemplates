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
package com.infusionsoft.dataflow.dto;

import java.util.Objects;

public class EmailContent {

  private String htmlBody;
  private String textBody;

  public String getHtmlBody() {
    return htmlBody;
  }

  public void setHtmlBody(String htmlBody) {
    this.htmlBody = htmlBody;
  }

  public String getTextBody() {
    return textBody;
  }

  public void setTextBody(String textBody) {
    this.textBody = textBody;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EmailContent that = (EmailContent) o;
    return Objects.equals(htmlBody, that.htmlBody) && Objects.equals(textBody, that.textBody);
  }

  @Override
  public int hashCode() {
    return Objects.hash(htmlBody, textBody);
  }

  @Override
  public String toString() {
    return "EmailContent{"
        + "htmlBody='"
        + htmlBody
        + '\''
        + ", textBody='"
        + textBody
        + '\''
        + '}';
  }
}
