package org.apache.beam.it.gcp.spanner;

import static org.junit.Assume.assumeTrue;

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.junit.Before;
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

  protected Set<String> stagingEnabledTests() {
    // staging endpoint test disabled in base class. To enable it for certain test,
    // override this in derived test class.
    return ImmutableSet.of();
  }

  @Before
  public void setupSpannerBase() {
    if ("Staging".equals(spannerHostName)) {
      // Only executes allow-listed staging test
      assumeTrue(stagingEnabledTests().contains(testName));
    }
  }

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
