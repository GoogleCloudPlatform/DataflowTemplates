package com.google.cloud.teleport.v2.spanner.service;

public interface ErrorInjectionPolicy {
  boolean shouldInjectionError();
}
