package org.apache.beam.sdk.io.gcp.spanner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.cloud.ServiceFactory;
import com.google.cloud.teleport.v2.spanner.service.SpannerService;
import org.junit.Test;

public class SpannerServiceFactoryImplTest {

  @Test
  public void testCreateSpannerService() {
    SpannerConfig spannerConfig = SpannerConfig.builder().build();
    SpannerConfig newSpannerConfig = SpannerServiceFactoryImpl.createSpannerService(spannerConfig, "");

    assertNotNull(newSpannerConfig.getServiceFactory());
    assertTrue(newSpannerConfig.getServiceFactory() instanceof SpannerService);
  }

  @Test
  public void testCreateSpannerServiceWhenServiceAlreadySet() {
    ServiceFactory oldService = mock(ServiceFactory.class);
    SpannerConfig spannerConfig = SpannerConfig.builder()
        .setServiceFactory(oldService)
        .build();
    SpannerConfig newSpannerConfig = SpannerServiceFactoryImpl.createSpannerService(spannerConfig, "");

    assertEquals(oldService, newSpannerConfig.getServiceFactory());
  }
}
