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

package co.cask.cdap.kafka.flow;

import com.google.common.base.Objects;

import java.net.InetSocketAddress;

/**
 * Represents a Kafka broker, which contains its ID and network address.
 */
public final class KafkaBroker implements Comparable<KafkaBroker> {

  private final String id;
  private final InetSocketAddress address;

  public KafkaBroker(String id, String host, int port) {
    this(id, new InetSocketAddress(host, port));
  }

  public KafkaBroker(String id, InetSocketAddress address) {
    this.id = id;
    this.address = address;
  }

  public String getId() {
    return id;
  }

  public InetSocketAddress getAddress() {
    return address;
  }

  public String getHost() {
    return address.getHostName();
  }

  public int getPort() {
    return address.getPort();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    KafkaBroker other = (KafkaBroker) o;

    return id.equals(other.id) && address.equals(other.address);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id, address);
  }

  @Override
  public int compareTo(KafkaBroker o) {
    return id.compareTo(o.id);
  }
}
