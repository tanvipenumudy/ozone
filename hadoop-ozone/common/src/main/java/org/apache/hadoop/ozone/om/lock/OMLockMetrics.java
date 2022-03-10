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

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * This class is for maintaining the various Ozone Manager Lock Metrics
 */
@InterfaceAudience.Private
@Metrics(about = "Ozone Manager Lock Metrics", context = OzoneConsts.OZONE)
public class OMLockMetrics {
  private static final String SOURCE_NAME =
      OMLockMetrics.class.getSimpleName();

  private @Metric MutableGaugeLong longestReadLockWaitingTimeMs;
  private @Metric MutableGaugeLong longestReadLockHeldTimeMs;
  private @Metric MutableCounterLong numReadLockLongWaiting;
  private @Metric MutableCounterLong numReadLockLongHeld;

  /**
   * Registers OMLockMetrics source.
   *
   * @return OMLockMetrics object
   */
  public static OMLockMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME, "Ozone Manager Lock Metrics",
        new OMLockMetrics());
  }

  /**
   * Unregisters OMLockMetrics source.
   */
  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  /**
   * Sets the longest time (ms) a read lock was waiting since the last
   * measurement.
   *
   * @param longestReadLockWaitingTimeMs longest read lock waiting time (ms)
   */
  public void setLongestReadLockWaitingTimeMs(
      long longestReadLockWaitingTimeMs) {
    this.longestReadLockWaitingTimeMs.set(longestReadLockWaitingTimeMs);
  }

  /**
   * Sets the longest time (ms) a read lock was held since the last measurement.
   *
   * @param longestReadLockHeldTimeMs longest read lock held time (ms)
   */
  public void setLongestReadLockHeldTimeMs(long longestReadLockHeldTimeMs) {
    this.longestReadLockHeldTimeMs.set(longestReadLockHeldTimeMs);
  }

  /**
   * Increments number of times the read lock has been waiting longer than
   * the default threshold configuration.
   */
  public void incNumReadLockLongWaiting() {
    numReadLockLongWaiting.incr();
  }

  /**
   * Increments number of times the read lock has been held longer than the
   * default threshold configuration.
   */
  public void incNumReadLockLongHeld() {
    numReadLockLongHeld.incr();
  }

  /**
   * Returns the longest time (ms) a read lock was waiting since the last
   * measurement.
   *
   * @return longest read lock waiting time (ms)
   */
  public long getLongestReadLockWaitingTimeMs() {
    return longestReadLockWaitingTimeMs.value();
  }

  /**
   * Returns the longest time (ms) a read lock was held since the last
   * measurement.
   *
   * @return longest read lock held time (ms)
   */
  public long getLongestReadLockHeldTimeMs() {
    return longestReadLockHeldTimeMs.value();
  }

  /**
   * Returns number of times the read lock has been waiting longer than the
   * default threshold configuration.
   *
   * @return number of times read lock waiting time crossed default threshold
   */
  public long getNumReadLockLongWaiting() {
    return numReadLockLongWaiting.value();
  }

  /**
   * Returns number of times the read lock has been held longer than the default
   * threshold configuration.
   *
   * @return number of times read lock held time crossed default threshold
   */
  public long getNumReadLockLongHeld() {
    return numReadLockLongHeld.value();
  }
}
