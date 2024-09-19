package com.google.cloud.teleport.lt;

import java.util.ArrayList;
import java.util.List;
import org.junit.runners.model.MultipleFailureException;

public class AssertAll {

  private List<Throwable> assertionErrors = new ArrayList<>();

  public void assertAll(Runnable... assertions) throws MultipleFailureException {
    for (Runnable assertion : assertions) {
      try {
        assertion.run();
      } catch (AssertionError e) {
        assertionErrors.add(e);
      }
    }
    if (!assertionErrors.isEmpty()) {
      throw new MultipleFailureException(assertionErrors);
    }
  }
}
