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
    return Objects.equals(htmlBody, that.htmlBody) &&
        Objects.equals(textBody, that.textBody);
  }

  @Override
  public int hashCode() {
    return Objects.hash(htmlBody, textBody);
  }

  @Override
  public String toString() {
    return "EmailContent{" +
        "htmlBody='" + htmlBody + '\'' +
        ", textBody='" + textBody + '\'' +
        '}';
  }
}
