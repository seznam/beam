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
package org.apache.beam.sdk.extensions.euphoria.core.translate.io;

import java.io.IOException;
import java.util.Objects;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.DataSink;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Writer;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Write to output sink using beam. */
@DoFn.BoundedPerElement
public class BeamWriteSink<T> extends PTransform<PCollection<T>, PDone> {
  private static final Logger LOG = LoggerFactory.getLogger(BeamWriteSink.class);

  private final DataSink<T> sink;

  private BeamWriteSink(DataSink<T> sink) {
    this.sink = Objects.requireNonNull(sink);
  }

  public static <T> BeamWriteSink<T> wrap(DataSink<T> sink) {
    return new BeamWriteSink<>(sink);
  }

  @Override
  @SuppressWarnings("unchecked")
  public PDone expand(PCollection<T> input) {
    sink.initialize();

    // TODO: decide number of shards and refactor WriteFn
    PCollection<Integer> finishedWrites = input.apply(ParDo.of(new WriteFn<>(0, sink)));

    if (finishedWrites.isBounded() != IsBounded.BOUNDED) {
      WindowingStrategy<?, ?> windowingStrategy = finishedWrites.getWindowingStrategy();

      if (windowingStrategy.getWindowFn() instanceof GlobalWindows
          && windowingStrategy.getTrigger() instanceof DefaultTrigger
          && input.isBounded() != IsBounded.BOUNDED) {

        LOG.info("Replacing windowing");
        Window<Integer> objectWindow =
            Window.into((WindowFn) new GlobalWindows())
                .triggering(AfterWatermark.pastEndOfWindow())
                .discardingFiredPanes();

        finishedWrites = finishedWrites.apply(objectWindow);
      }
    }
    //
    //    finishedWrites.setCoder(KvCoder.of(StringUtf8Coder.of(), VoidCoder.of()));
    //
    //    finishedWrites
    //        .apply(GroupByKey.<String, Void>create())
    //        .apply(
    //            ParDo.of(
    //                new DoFn<KV<String, Iterable<Void>>, Void>() {
    //
    //                  @ProcessElement
    //                  public void processElements(ProcessContext c) {
    //                    // elements consumed here should be grouped by key and window
    //                    //TODO this may not work for stream, when panes are emitted several times
    //                    try {
    //                      sink.commit();
    //                    } catch (IOException e) {
    //                      LOG.error(String.format("Unable to commit '%s' sink.", sink), e);
    //                    }
    //                  }
    //                }));

    finishedWrites.apply(
        "combine-writers-outputs",
        Combine.globally(
                new CombineFn<Integer, Integer, Integer>() {
                  @Override
                  public Integer createAccumulator() {
                    return 0;
                  }

                  @Override
                  public Integer addInput(Integer accumulator, Integer input) {
                    return accumulator + input;
                  }

                  @Override
                  public Integer mergeAccumulators(Iterable<Integer> accumulators) {
                    int sum = 0;
                    for (Integer i : accumulators) {
                      sum += i;
                    }
                    return sum;
                  }

                  @Override
                  public Integer extractOutput(Integer accumulator) {
                    if (accumulator == -1) {
                      LOG.info(
                          "Sink '%s' not committed since extractOutput() was called with default value.");
                    }

                    LOG.info(
                        String.format(
                            "Committing '%s' sink after '%d' finished writers.",
                            sink, accumulator));
                    try {
                      sink.commit();
                    } catch (IOException e) {
                      LOG.error(String.format("Unable to commit '%s' sink.", sink), e);
                    }
                    return accumulator;
                  }

                  @Override
                  public Integer defaultValue() {
                    return extractOutput(-1);
                  }
                })
            .withoutDefaults());

    return PDone.in(input.getPipeline());
  }

  private static final class WriteFn<T> extends DoFn<T, Integer> {

    private final DataSink<T> sink;
    private final int partitionId;
    private Writer<T> writer = null;

    private BoundedWindow lastWindow;
    private Instant lastTimestamp;

    WriteFn(int partitionId, DataSink<T> sink) {
      this.partitionId = partitionId;
      this.sink = sink;
    }

    @StartBundle
    public void setupBundle() {
      writer = sink.openWriter(partitionId);
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) throws IOException {
      T element = c.element();
      lastTimestamp = c.timestamp();
      lastWindow = window;
      writer.write(element);
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext context) throws Exception {
      writer.flush();
      writer.commit();
      writer.close();

      context.output(1, lastTimestamp, lastWindow);
    }

    @Teardown
    public void tearDown() throws IOException {}
  }
}
