package org.apache.beam.it.gcp.spanner;

import java.util.Arrays;
import java.util.Collection;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Base class for all Spanner template integration tests. This class parameterizes the spannerHost
 * for all subclasses. If the "spannerHost" system property is not set, the value of spannerHost
 * will be set to STAGING_SPANNER_HOST and DEFAULT_SPANNER_HOST (as defined in {@link
 * SpannerResourceManager}). All tests in the base class will be run twice: once with spannerHost
 * set to STAGING_SPANNER_HOST and once with spannerHost set to DEFAULT_SPANNER_HOST. Otherwise, If
 * the "spannerHost" system property is set, its value will be used to set spannerHost. All
 * subclasses must use SpannerResourceManager.useCustomHost() and pass the spannerHost parameter to
 * it.
 */
@RunWith(Parameterized.class)
public abstract class SpannerTemplateITBase extends TemplateTestBase {

  @Parameterized.Parameter(0)
  public String spannerHost;

  @Parameterized.Parameter(1)
  public String spannerHostName;

  // Because of parameterization, the test names will have subscripts. For example:
  // testSpannerToGCSAvroBase[Staging]
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
