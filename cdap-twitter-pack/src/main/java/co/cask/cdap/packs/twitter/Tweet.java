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

import com.google.common.base.Objects;

public final class Tweet {
  private String text;
  private long createdAt;

  public Tweet(String text, long createdAt) {
    this.text = text;
    this.createdAt = createdAt;
  }

  public String getText() {
    return text;
  }

  public long getCreatedAt() {
    return createdAt;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null) {
      return false;
    }

    Tweet tweet = (Tweet) other;

    return createdAt == tweet.createdAt && text.equals(tweet.text);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(text, createdAt);
  }
}
