/*
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
package org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.time.Duration;
import org.junit.Test;

/** TODO: complete javadoc. */
public class TimeSlidingTest {

  @Test
  public void testWindowAssignment() {
    TimeSliding<?> windowing = TimeSliding.of(Duration.ofMillis(10), Duration.ofMillis(5));

    Iterable<TimeInterval> windows = windowing.assignWindowsToElement(new TimestampedElement<>(16));

    assertEquals(2, Iterables.size(windows));
    assertEquals(
        Sets.newHashSet(new TimeInterval(10, 20), new TimeInterval(15, 25)),
        Sets.newHashSet(windows));

    windows = windowing.assignWindowsToElement(new TimestampedElement<>(10));

    assertEquals(2, Iterables.size(windows));
    assertEquals(
        Sets.newHashSet(new TimeInterval(5, 15), new TimeInterval(10, 20)),
        Sets.newHashSet(windows));

    windows = windowing.assignWindowsToElement(new TimestampedElement<>(9));

    assertEquals(2, Iterables.size(windows));
    assertEquals(
        Sets.newHashSet(new TimeInterval(0, 10), new TimeInterval(5, 15)),
        Sets.newHashSet(windows));
  }
}
