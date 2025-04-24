package com.google.cloud.teleport.v2.failureinjection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class NoOpPolicyTest {

  @Test
  public void testNoOpPolicy() {
    NoOpPolicy policy = new NoOpPolicy();

    assertFalse(policy.shouldInjectionError());
    assertNotNull(policy.toString());
  }
}
