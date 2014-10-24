/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.packs.twitter;

import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.RuntimeMetrics;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.TestBase;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class TweetCollectorFlowletTest extends TestBase {
  private static final Gson GSON = new Gson();

  @Test
  public void test() throws Exception {
    Set<Tweet> tweets = ImmutableSet.of(
      new Tweet("tweet1", 1000),
      new Tweet("tweet2", 2000)
    );
    File srcFile = writeToTempFile(tweets.iterator());

    ApplicationManager applicationManager = deployApplication(TweetCollectorApp.class);
    applicationManager.startFlow("TweetCollectorFlow",
                                 ImmutableMap.of(TweetCollectorFlowlet.ARG_TWITTER4J_DISABLED, "true",
                                                 TweetCollectorFlowlet.ARG_SOURCE_FILE, srcFile.getPath()));

    RuntimeMetrics countMetrics = RuntimeStats.getFlowletMetrics(TweetCollectorApp.class.getSimpleName(),
                                                                 "TweetCollectorFlow",
                                                                 "persistor");

    countMetrics.waitForProcessed(2, 2, TimeUnit.SECONDS);

    DataSetManager<ObjectStore<Tweet>> tweetsDataset = applicationManager.getDataSet("tweets");
    CloseableIterator<KeyValue<byte[], Tweet>> scan = tweetsDataset.get().scan(null, null);
    Set<Tweet> result = Sets.newHashSet();
    while (scan.hasNext()) {
      result.add(scan.next().getValue());
    }
    Assert.assertEquals(tweets, result);
  }

  private File writeToTempFile(Iterator<Tweet> iterator) throws IOException {
    File srcFile = File.createTempFile("tweets", "txt");
    srcFile.deleteOnExit();

    FileWriter fw = new FileWriter(srcFile);
    try {
      BufferedWriter bw = new BufferedWriter(fw);
      try {
        while (iterator.hasNext()) {
          bw.write(GSON.toJson(iterator.next()));
          bw.newLine();
        }
      } finally {
        bw.close();
      }
    } finally {
      fw.close();
    }
    return srcFile;
  }
}
