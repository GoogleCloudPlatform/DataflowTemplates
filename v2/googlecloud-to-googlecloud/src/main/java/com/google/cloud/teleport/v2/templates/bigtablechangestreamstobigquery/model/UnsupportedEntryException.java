package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model;

public class UnsupportedEntryException extends Exception {

  public UnsupportedEntryException() {
    super();
  }

  public UnsupportedEntryException(String message) {
    super(message);
  }

  public UnsupportedEntryException(String message, Throwable cause) {
    super(message, cause);
  }

  public UnsupportedEntryException(Throwable cause) {
    super(cause);
  }

  protected UnsupportedEntryException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
