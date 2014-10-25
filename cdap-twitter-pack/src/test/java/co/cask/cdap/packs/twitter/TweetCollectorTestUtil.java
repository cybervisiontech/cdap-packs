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

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.gson.Gson;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 * Handy utilities for testing {@link TweetCollectorFlowlet} and flows that use it.
 */
public final class TweetCollectorTestUtil {
  private static final Gson GSON = new Gson();

  public static File writeToTempFile(Iterator<Tweet> iterator) throws IOException {
    File srcFile = File.createTempFile("tweets", "txt");
    srcFile.deleteOnExit();

    BufferedWriter writer = Files.newWriter(srcFile, Charsets.UTF_8);
    try {
      while (iterator.hasNext()) {
        writer.write(GSON.toJson(iterator.next()));
        writer.newLine();
      }
    } finally {
      writer.close();
    }
    return srcFile;
  }
}
