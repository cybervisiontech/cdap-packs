/*
 * Copyright 2014 Cask, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.lib.etl.batch.source;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link co.cask.lib.etl.batch.source.StreamSource}.
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
