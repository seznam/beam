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

package org.apache.beam.runners.spark.translation;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.UnsignedBytes;
import java.util.Arrays;
import java.util.Iterator;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.joda.time.Instant;
import scala.Tuple2;

public class GroupNonMergingWindowsFunctions {

  public static <K, V, W extends BoundedWindow>
  JavaRDD<WindowedValue<KV<K, Iterable<V>>>> groupByKeyAndWindow(
      JavaRDD<WindowedValue<KV<K, V>>> rdd,
      Coder<K> keyCoder,
      Coder<V> valueCoder,
      final WindowingStrategy<?, W> winStrategy) {
    Coder<W> windowCoder = winStrategy.getWindowFn().windowCoder();
    final WindowedValue.FullWindowedValueCoder<byte[]> winValCoder =
        WindowedValue.getFullCoder(ByteArrayCoder.of(), windowCoder);

    JavaPairRDD<KeyAndWindow, byte[]> pairRDD =
        rdd.flatMapToPair(
            (WindowedValue<KV<K, V>> kvWindowedValue) -> {
              final K key = kvWindowedValue.getValue().getKey();
              final byte[] keyBytes = CoderHelpers.toByteArray(key, keyCoder);
              final V value = kvWindowedValue.getValue().getValue();
              byte[] valueBytes = CoderHelpers.toByteArray(value, valueCoder);
              Iterable<WindowedValue<KV<K, V>>> windows = kvWindowedValue.explodeWindows();
              return Iterators.transform(
                  windows.iterator(),
                  wid -> {
                    @SuppressWarnings("unchecked")
                    final W window = (W) Iterables.getOnlyElement(wid.getWindows());
                    final byte[] windowBytes = CoderHelpers.toByteArray(window, windowCoder);

                    byte[] windowValueBytes =
                        CoderHelpers.toByteArray(
                            WindowedValue.of(valueBytes, wid.getTimestamp(), window, wid.getPane()),
                            winValCoder);
                    KeyAndWindow keyAndWindow = new KeyAndWindow(keyBytes, windowBytes);
                    return new Tuple2<>(keyAndWindow, windowValueBytes);
                  });
            });

    final Partitioner partitioner = new HashPartitioner(pairRDD.getNumPartitions());

    return pairRDD
        .repartitionAndSortWithinPartitions(partitioner)
        .mapPartitionsToPair(
            it -> new GroupByKeyIterator<>(it, keyCoder, valueCoder, winStrategy, winValCoder))
        .mapPartitions(itr ->
            Iterators.transform(itr, t2 -> t2._1.withValue(KV.of(t2._1().getValue(), t2._2()))));
  }

  /**
   * Transform stream of sorted key values into stream of value iterators for each key. This
   * iterator can be iterated only once!
   *
   * @param <K> type of key iterator emits
   * @param <V> type of value iterator emits
   */
  static class GroupByKeyIterator<K, V, W extends BoundedWindow>
      extends AbstractIterator<Tuple2<WindowedValue<K>, Iterable<V>>> {

    private final PeekingIterator<Tuple2<KeyAndWindow, byte[]>> inner;
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;
    private final WindowingStrategy<?, W> windowingStrategy;
    private KeyAndWindow previousKey;
    private FullWindowedValueCoder<byte[]> windowedValueCoder;

    GroupByKeyIterator(
        Iterator<Tuple2<KeyAndWindow, byte[]>> inner,
        Coder<K> keyCoder,
        Coder<V> valueCoder,
        WindowingStrategy<?, W> windowingStrategy,
        WindowedValue.FullWindowedValueCoder<byte[]> windowedValueCoder) {
      this.inner = Iterators.peekingIterator(inner);
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
      this.windowingStrategy = windowingStrategy;
      this.windowedValueCoder = windowedValueCoder;
    }

    @Override
    protected Tuple2<WindowedValue<K>, Iterable<V>> computeNext() {
      while (inner.hasNext()) {
        // just peek, so the value iterator can see the first value
        Tuple2<KeyAndWindow, byte[]> peek = inner.peek();

        final KeyAndWindow currentKey = peek._1;

        if (currentKey.equals(previousKey)) {
          // inner iterator did not consume all values for a given key, we need to skip ahead until we
          // find value for the next key
          inner.next();
          continue;
        }
        previousKey = currentKey;
        Tuple2<WindowedValue<K>, V> decodedEl = getDecodedElement(peek);
        return new Tuple2<>(
            decodedEl._1,
            () ->
                new AbstractIterator<V>() {

                  @Override
                  protected V computeNext() {
                    // compute until we know, that next element contains a new key or there are no
                    // more elements to process
                    if (inner.hasNext() && currentKey.equals(inner.peek()._1)) {
                      Tuple2<KeyAndWindow, byte[]> next = inner.next();
                      return getDecodedElement(next)._2;
                    }
                    return endOfData();
                  }
                });
      }
      return endOfData();
    }

    Tuple2<WindowedValue<K>, V> getDecodedElement(Tuple2<KeyAndWindow, byte[]> item) {
      KeyAndWindow keyAndWindow = item._1;
      K key = CoderHelpers.fromByteArray(keyAndWindow.getKey(), keyCoder);
      WindowedValue<byte[]> windowedValue = CoderHelpers.fromByteArray(item._2, windowedValueCoder);
      V v = CoderHelpers.fromByteArray(windowedValue.getValue(), valueCoder);
      @SuppressWarnings("unchecked")
      final W w = (W) Iterables.getOnlyElement(windowedValue.getWindows());

      Instant timestamp =
          windowingStrategy
              .getTimestampCombiner()
              .assign(
                  w,
                  windowingStrategy.getWindowFn().getOutputTime(windowedValue.getTimestamp(), w));
      WindowedValue<K> keyWinValue = WindowedValue.of(key, timestamp, w, windowedValue.getPane());
      return new Tuple2<>(keyWinValue, v);
    }
  }

  /**
   * Composite key of key and window for groupByKey transformation.
   */
  public static class KeyAndWindow implements Comparable<KeyAndWindow> {

    private final byte[] key;
    private final byte[] window;

    public KeyAndWindow(byte[] key, byte[] window) {
      this.key = key;
      this.window = window;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      KeyAndWindow that = (KeyAndWindow) o;
      return Arrays.equals(key, that.key)
          && Arrays.equals(window, that.window);
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(key);
      result = 31 * result + Arrays.hashCode(window);
      return result;
    }

    public byte[] getKey() {
      return key;
    }

    public byte[] getWindow() {
      return window;
    }

    @Override
    public int compareTo(KeyAndWindow o) {
      int keyCompare = UnsignedBytes.lexicographicalComparator().compare(this.getKey(), o.getKey());
      if (keyCompare == 0) {
        return UnsignedBytes.lexicographicalComparator().compare(this.getWindow(), o.getWindow());
      }
      return keyCompare;
    }
  }

}
