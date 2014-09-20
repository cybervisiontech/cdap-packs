package com.continuuity.lib.etl.batch.source;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link com.continuuity.lib.etl.batch.source.StreamSource}.
 */
public class StreamSourceTest {

  @Test
  public void testAdvanceProgressFromSuccess() {
    StreamSource streamSource = new StreamSource();
    StreamSource.Progress original = new StreamSource.Progress(10, 10);
    StreamSource.Progress progress = streamSource.advanceProgress(original, 13);
    Assert.assertEquals(10, progress.getLastProcessed());
    Assert.assertEquals(13, progress.getLastAttempted());
  }

  @Test
  public void testAdvanceProgressFromFail() {
    StreamSource streamSource = new StreamSource();
    StreamSource.Progress original = new StreamSource.Progress(5, 10);
    StreamSource.Progress progress = streamSource.advanceProgress(original, 13);
    Assert.assertEquals(5, progress.getLastProcessed());
    Assert.assertEquals(13, progress.getLastAttempted());
  }

  @Test
  public void testAdvanceProgressFromStart() {
    StreamSource streamSource = new StreamSource();
    StreamSource.Progress original = null;
    StreamSource.Progress progress = streamSource.advanceProgress(original, 13);
    Assert.assertEquals(13 - StreamSource.TO_PROCESS_ON_START, progress.getLastProcessed());
    Assert.assertEquals(13, progress.getLastAttempted());
  }

}
