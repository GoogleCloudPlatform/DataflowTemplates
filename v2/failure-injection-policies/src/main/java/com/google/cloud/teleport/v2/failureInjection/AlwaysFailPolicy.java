package com.google.cloud.teleport.v2.spanner.service;

import java.io.Serializable;

public class AlwaysFailPolicy implements ErrorInjectionPolicy, Serializable {

  @Override
  public boolean shouldInjectionError() {
    return true;
  }
}
