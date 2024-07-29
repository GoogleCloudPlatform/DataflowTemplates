package com.google.cloud.teleport.lt;

public class Violation {

  private String message;

  public Violation(String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }
}
