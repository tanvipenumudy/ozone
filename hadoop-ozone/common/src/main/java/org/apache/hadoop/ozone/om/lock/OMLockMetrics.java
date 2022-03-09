/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.lock;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

/**
 * This interface is for maintaining the various Ozone Manager Lock Metrics
 */
@InterfaceAudience.Private
@Metrics(about = "Ozone Manager Lock Metrics", context = "dfs")
public class OMLockMetrics {
  private static final String SOURCE_NAME =
      OMLockMetrics.class.getSimpleName();

  private @Metric MutableGaugeLong longestReadWaitingTimeMs;
  private @Metric MutableGaugeLong longestReadHeldTimeMs;
  private @Metric MutableCounterLong numReadLockLongWaiting;
  private @Metric MutableCounterLong numReadLockLongHeld;

  public static OMLockMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME, "Ozone Manager Lock Metrics",
        new OMLockMetrics());
  }

  @VisibleForTesting
  public void setLongestReadWaitingTimeMs(long val) {
    this.longestReadWaitingTimeMs.set(val);
  }

  @VisibleForTesting
  public void setLongestReadHeldTimeMs(long val) {
    this.longestReadHeldTimeMs.set(val);
  }

  @VisibleForTesting
  public void incNumReadLockLongWaiting() {
    numReadLockLongWaiting.incr();
  }

  @VisibleForTesting
  public void incNumReadLockLongHeld() {
    numReadLockLongHeld.incr();
  }

  @VisibleForTesting
  public long getLongestReadWaitingTimeMs() {
    return longestReadWaitingTimeMs.value();
  }

  @VisibleForTesting
  public long getLongestReadHeldTimeMs() {
    return longestReadHeldTimeMs.value();
  }

  @VisibleForTesting
  public long getNumReadLockLongWaiting() {
    return numReadLockLongWaiting.value();
  }

  @VisibleForTesting
  public long getNumReadLockLongHeld() {
    return numReadLockLongHeld.value();
  }
}
