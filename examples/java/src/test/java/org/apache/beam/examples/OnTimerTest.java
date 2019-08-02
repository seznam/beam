/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.beam.examples;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

public class OnTimerTest {

  @Rule
  public TestPipeline pipeline = TestPipeline.create();
  private static final Instant NOW = new Instant(0);
  private static final Instant SEC_1_DURATION = NOW.plus(Duration.standardSeconds(1));

  @Test
  public void failingTest() {

    Coder<String> utf8Coder = StringUtf8Coder.of();
    Coder<Long> varLongCoder = VarLongCoder.of();

    KvCoder<String, Long> keyValueCoder = KvCoder.of(utf8Coder, varLongCoder);
    TestStream<KV<String, Long>> words = TestStream.create(keyValueCoder).addElements(
        TimestampedValue.of(KV.of("a1", 1L), SEC_1_DURATION))
        .advanceProcessingTime(Duration.standardSeconds(20))
        .advanceWatermarkToInfinity();
    PCollection<Iterable<String>> results = pipeline.apply(words)
        .apply(ParDo.of(new OnTimerDoFn()));
      pipeline.run();
  }

  private static class OnTimerDoFn extends DoFn<KV<String, Long>, Iterable<String>> {

    private static final String FLUSH_STATE_NAME = "expiry";
    private Duration TEN_SECONDS = Duration.standardSeconds(10);

    @TimerId(FLUSH_STATE_NAME)
    private final TimerSpec flushSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @ProcessElement
    public void processElement(ProcessContext processContext,
        @TimerId(FLUSH_STATE_NAME) Timer expiryTimer) {
        System.out.println("element ts = " + processContext.timestamp());

        expiryTimer.offset(TEN_SECONDS).setRelative();

    }

    @OnTimer(FLUSH_STATE_NAME)
    public void flushAccumulatedResults(OnTimerContext context) {
      Instant elementTs = NOW.plus(SEC_1_DURATION.getMillis());
      Instant expectedTimerTs = NOW.plus(SEC_1_DURATION.getMillis()).plus(TEN_SECONDS);
      System.out.println("Ontimer timestamp = " + context.timestamp());
      if(context.timestamp().isBefore(NOW)){
        System.out.println("expectedTimerTs = " + expectedTimerTs);
        throw new IllegalStateException("Why the timestamp of onTimerContext is so weird "
            + "context.timestamp()="+ context.timestamp());
      }

    }
  }
}
