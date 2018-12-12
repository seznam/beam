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
package org.apache.beam.runners.spark.translation;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;

public class BlockingDoFnRunner<InputT, OutputT> implements DoFnRunner<InputT, OutputT>, Closeable {

  private final DoFnRunner<InputT, OutputT> delegate;
  private final SparkProcessContext.SparkOutputManager outputManager;

  private final ExecutorService executor =
      Executors.newSingleThreadExecutor(
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat("blocking-runner-%d").build());

  BlockingDoFnRunner(
      DoFnRunner<InputT, OutputT> delegate, SparkProcessContext.SparkOutputManager outputManager) {
    this.delegate = delegate;
    this.outputManager = outputManager;
  }

  @Override
  public void startBundle() {
    executeBlocking(delegate::startBundle);
  }

  @Override
  public void processElement(WindowedValue<InputT> elem) {
    executor.execute(
        () -> {
          try {
            delegate.processElement(elem);
            outputManager.tombstone();
          } catch (Throwable t) {
            outputManager.fail(t);
          }
        });
  }

  @Override
  public void onTimer(
      String timerId, BoundedWindow window, Instant timestamp, TimeDomain timeDomain) {
    executor.execute(
        () -> {
          try {
            delegate.onTimer(timerId, window, timestamp, timeDomain);
            outputManager.tombstone();
          } catch (Throwable t) {
            outputManager.fail(t);
          }
        });
  }

  @Override
  public void finishBundle() {
    executor.execute(
        () -> {
          try {
            delegate.finishBundle();
            outputManager.tombstone();
          } catch (Throwable t) {
            outputManager.fail(t);
          }
        });
  }

  @Override
  public DoFn<InputT, OutputT> getFn() {
    final AtomicReference<DoFn<InputT, OutputT>> result = new AtomicReference<>();
    executeBlocking(() -> result.set(delegate.getFn()));
    return result.get();
  }

  @Override
  public void close() {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
        checkState(executor.shutdownNow().isEmpty(), "Executor has uncompleted tasks.");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @SuppressWarnings("unchecked")
  private <ExceptionT extends Throwable> void executeBlocking(Runnable runnable) throws ExceptionT {
    try {
      executor.submit(runnable).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      throw (ExceptionT) e.getCause();
    }
  }
}
