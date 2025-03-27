package com.google.cloud.teleport.v2.spanner.service;

public class ErrorInjectionPolicyFactory {

  public static ErrorInjectionPolicy getErrorInjectionPolicy(String parameter) {
    return new AlwaysFailPolicy();
  }
}
