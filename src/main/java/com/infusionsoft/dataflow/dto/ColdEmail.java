package com.infusionsoft.dataflow.dto;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;

public class ColdEmail {

  public String accountId;
  public String fromAddress;
  public List<String> toAddresses;
  private String subject;
  private String htmlBody;
  private String textBody;
  private ZonedDateTime created;

  public String getAccountId() {
    return accountId;
  }

  public void setAccountId(String accountId) {
    this.accountId = accountId;
  }

  public String getFromAddress() {
    return fromAddress;
  }

  public void setFromAddress(String fromAddress) {
    this.fromAddress = fromAddress;
  }

  public List<String> getToAddresses() {
    return toAddresses;
  }

  public void setToAddresses(List<String> toAddresses) {
    this.toAddresses = toAddresses;
  }

  public String getSubject() {
    return subject;
  }

  public void setSubject(String subject) {
    this.subject = subject;
  }

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

  public ZonedDateTime getCreated() {
    return created;
  }

  public void setCreated(ZonedDateTime created) {
    this.created = created;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColdEmail coldEmail = (ColdEmail) o;
    return Objects.equals(accountId, coldEmail.accountId) &&
        Objects.equals(fromAddress, coldEmail.fromAddress) &&
        Objects.equals(toAddresses, coldEmail.toAddresses) &&
        Objects.equals(subject, coldEmail.subject) &&
        Objects.equals(htmlBody, coldEmail.htmlBody) &&
        Objects.equals(textBody, coldEmail.textBody) &&
        Objects.equals(created, coldEmail.created);
  }

  @Override
  public int hashCode() {
    return Objects.hash(accountId, fromAddress, toAddresses, subject, htmlBody, textBody, created);
  }

  @Override
  public String toString() {
    return "ColdEmail{" +
        "accountId='" + accountId + '\'' +
        ", fromAddress='" + fromAddress + '\'' +
        ", toAddresses=" + toAddresses +
        ", subject='" + subject + '\'' +
        ", htmlBody='" + htmlBody + '\'' +
        ", textBody='" + textBody + '\'' +
        ", created=" + created +
        '}';
  }
}
