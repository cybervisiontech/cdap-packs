/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.api.annotation.Tick;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.metrics.Metrics;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import twitter4j.StatusAdapter;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class TweetCollectorFlowlet extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(TweetCollectorFlowlet.class);
  private static final Gson GSON = new Gson();

  public static final String ARG_TWITTER4J_DISABLED = "tweet.collector.source.twitter4j.disabled";
  /** Use file as tweets source. Useful in test scenarios  */
  public static final String ARG_SOURCE_FILE = "tweet.collector.source.file";

  public static final int DEFAULT_INTERNAL_QUEUE_SIZE = 10000;
  public static final int DEFAULT_EMIT_BATCH_MAX_SIZE = 100;

  private Metrics metrics;
  private OutputEmitter<Tweet> output;

  private BlockingQueue<Tweet> queue;

  private FileReaderThread fileReader;
  private CollectingThread twitterStreamPuller;

  private TwitterStream twitterStream;

  private int internalQueueSize;
  private int emitBatchMaxSize;

  public TweetCollectorFlowlet() {
    this (DEFAULT_INTERNAL_QUEUE_SIZE, DEFAULT_EMIT_BATCH_MAX_SIZE);
  }

  public TweetCollectorFlowlet(String name) {
    this(name, DEFAULT_INTERNAL_QUEUE_SIZE, DEFAULT_EMIT_BATCH_MAX_SIZE);
  }

  public TweetCollectorFlowlet(int internalQueueSize, int emitBatchMaxSize) {
    super();
    this.internalQueueSize = internalQueueSize;
    this.emitBatchMaxSize = emitBatchMaxSize;
  }

  public TweetCollectorFlowlet(String name, int internalQueueSize, int emitBatchMaxSize) {
    super(name);
    this.internalQueueSize = internalQueueSize;
    this.emitBatchMaxSize = emitBatchMaxSize;
  }

  @Override
  public void initialize(FlowletContext context) throws Exception {
    super.initialize(context);

    // The flowlet pulls tweets from multiple sources in parallel, in separate threads. Pulled tweets are added to the
    // queue and are emitted from the queue using @Tick method in batches.

    Map<String, String> args = context.getRuntimeArguments();

    boolean pullDisabled = false;
    if (args.containsKey(ARG_TWITTER4J_DISABLED)) {
      if (Boolean.parseBoolean(args.get(ARG_TWITTER4J_DISABLED))) {
        LOG.warn("Pulling tweets is disabled via '" + ARG_TWITTER4J_DISABLED + "' runtime argument.");
        pullDisabled = true;
      }
    }

    // todo: allow to use Dataset as a source
    String srcFile = null;
    if (args.containsKey(ARG_SOURCE_FILE)) {
      srcFile = args.get(ARG_SOURCE_FILE);
      LOG.info("Will read tweets from file: " + srcFile);
    }

    // NOTE: we need to create queue before starting threads that write to it
    if (!pullDisabled || srcFile != null) {
      queue = new LinkedBlockingQueue<Tweet>(internalQueueSize);
    }

    if (!pullDisabled) {
      // NOTE: oath keys are loaded from twitter4j.properties file, but can be overridden with runtime arguments
      ConfigurationBuilder cb = new ConfigurationBuilder();

      // Override twitter4j.properties file, if provided in runtime args.
      // Ideally, we'd override all, but twitter4j APIs are bad not allowing set arbitrary property...
      if (args.containsKey("twitter4j.oauth.consumerKey")) {
        cb.setOAuthConsumerKey(args.get("twitter4j..oauth.consumerKey"));
      }
      if (args.containsKey("twitter4j.oauth.consumerSecret")) {
        cb.setOAuthConsumerSecret(args.get("twitter4j.oauth.consumerSecret"));
      }
      if (args.containsKey("twitter4j.oauth.accessToken")) {
        cb.setOAuthAccessToken(args.get("twitter4j.oauth.accessToken"));
      }
      if (args.containsKey("twitter4j.oauth.consumerKey")) {
        cb.setOAuthAccessTokenSecret(args.get("twitter4j.oauth.accessTokenSecret"));
      }

      Configuration conf = cb.build();

      twitterStreamPuller = new CollectingThread(conf);
      twitterStreamPuller.start();
    }

    if (srcFile != null) {
      fileReader = new FileReaderThread(srcFile);
      fileReader.start();
    }
  }

  @Override
  public void destroy() {
    if (twitterStreamPuller != null) {
      twitterStreamPuller.interrupt();
    }
    if (fileReader != null) {
      fileReader.interrupt();
    }
    if (twitterStream != null) {
      twitterStream.cleanUp();
      twitterStream.shutdown();
    }
  }

  @Tick(unit = TimeUnit.NANOSECONDS, delay = 0)
  public void collect() throws InterruptedException {
    if (queue == null) {
      // Sleep and return: flowlet is configured with no tweets source
      Thread.sleep(1000);
      return;
    }

    for (int i = 0; i < emitBatchMaxSize; i++) {
      Tweet tweet = queue.poll();
      if (tweet == null) {
        break;
      }

      metrics.count("tweet.collector.out.total", 1);
      output.emit(tweet);
    }
  }

  private class CollectingThread extends Thread {
    private final Configuration twitter4jConf;

    private CollectingThread(Configuration twitter4jConf) {
      super("CollectingThread");
      this.twitter4jConf = twitter4jConf;
    }

    @Override
    public void run() {
      twitterStream = new TwitterStreamFactory(twitter4jConf).getInstance();

      StatusListener listener = new StatusAdapter() {
        @Override
        public void onStatus(Status status) {
          String text = status.getText();
          try {
            queue.put(new Tweet(text, status.getCreatedAt().getTime()));
          } catch (InterruptedException e) {
            LOG.warn("Interrupted while writing to a queue", e);
            Thread.currentThread().interrupt();
          }
        }

        @Override
        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
          LOG.error("Got track limitation notice:" + numberOfLimitedStatuses);
        }

        @Override
        public void onException(Exception ex) {
          LOG.warn("Error during reading from stream" + ex.getMessage());
        }
      };

      twitterStream.addListener(listener);
      twitterStream.sample();
      LOG.info("CollectingThread run() is exiting");
    }
  }

  private class FileReaderThread extends Thread {
    private final String srcFile;

    private FileReaderThread(String srcFile) {
      super("FileReaderThread");
      this.srcFile = srcFile;
    }

    @Override
    public void run() {
      try {
        Files.readLines(new File(srcFile), Charsets.UTF_8, new LineProcessor<Object>() {
          @Override
          public boolean processLine(String tweetJson) throws IOException {
            try {
              Tweet tweet = GSON.fromJson(tweetJson, Tweet.class);
              queue.put(tweet);
            } catch (InterruptedException e) {
              LOG.warn("Interrupted while writing to a queue", e);
              Thread.currentThread().interrupt();
              return false;
            }
            return true;
          }

          @Override
          public Object getResult() {
            // nothing to do here
            return null;
          }
        });
      } catch (IOException e) {
        LOG.error("Failed to read tweets from file " + srcFile, e);
      }
      LOG.info("FileReaderThread run() is exiting");
    }
  }
}
