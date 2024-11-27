package org.apache.beam.it.gcp.spanner;

import java.util.Arrays;
import java.util.Collection;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public abstract class SpannerTemplateITBase extends TemplateTestBase {

  @Parameterized.Parameter(0)
  public String spannerHost;

  @Parameterized.Parameter(1)
  public String spannerHostName;

  @Parameters(name = "{1}")
  public static Collection parameters() {
    if (System.getProperty("spannerHost") != null) {
      return Arrays.asList(new Object[][] {{System.getProperty("spannerHost"), "Custom"}});
    }
    return Arrays.asList(
        new Object[][] {
          {SpannerResourceManager.STAGING_SPANNER_HOST, "Staging"},
          {SpannerResourceManager.DEFAULT_SPANNER_HOST, "Default"}
        });
  }
}
