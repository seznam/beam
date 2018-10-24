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
import com.google.common.primitives.SignedBytes;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.util.CompositeKey;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.joda.time.Instant;
import scala.Tuple2;

public class GroupNonMergingWindowsFunctions implements Serializable {


  public static <K, V, W extends BoundedWindow> JavaRDD<WindowedValue<KV<K, Iterable<V>>>> groupByKeyWindow(
      JavaRDD<WindowedValue<KV<K, V>>> rdd,
      Coder<K> keyCoder,
      Coder<V> valueCoder,
      Coder<W> windowCoder) {

    JavaPairRDD<CompositeKey, byte[]> pairRDD = rdd
        .flatMapToPair((WindowedValue<KV<K, V>> kvWindowedValue) -> {
          final K key = kvWindowedValue.getValue().getKey();
          final byte[] keyBytes = CoderHelpers.toByteArray(key, keyCoder);
          final V value = kvWindowedValue.getValue().getValue();
          byte[] valueBytes = CoderHelpers.toByteArray(value, valueCoder);
          Iterable<WindowedValue<KV<K, V>>> windows = kvWindowedValue.explodeWindows();
          return Iterators.transform(windows.iterator(),
              wid -> {
                @SuppressWarnings("unchecked")
                final W window = (W) Iterables.getOnlyElement(wid.getWindows());
                final byte[] windowBytes = CoderHelpers.toByteArray(window, windowCoder);
                CompositeKey compositeKey = new CompositeKey(keyBytes, windowBytes, wid.getTimestamp().getMillis());
                return new Tuple2<>(compositeKey, valueBytes);
              }
          );
        });

    final Partitioner partitioner = new HashPartitioner(pairRDD.getNumPartitions());

    return pairRDD
        .repartitionAndSortWithinPartitions(partitioner, new CompositeKeyComparator())
        .mapPartitionsToPair(it -> new ReduceByKeyIterator<>(it, keyCoder, valueCoder, windowCoder))
        .mapPartitions(fromPairFlatMapFunction2());

  }
  /** A pair to {@link KV} flatmap function . */
  static <K, V> FlatMapFunction<Iterator<Tuple2<WindowedValue<K>, Iterable<V>>>, WindowedValue<KV<K, Iterable<V>>>> fromPairFlatMapFunction2() {
    return itr -> Iterators.transform(itr, t2 -> t2._1.withValue(KV.of(t2._1().getValue(), t2._2())));
  }

  public static class CompositeKeyComparator implements Comparator<CompositeKey>, Serializable {

    @Override
    public int compare(CompositeKey o1, CompositeKey o2) {
      int keyCompare = SignedBytes.lexicographicalComparator().compare(o1.getKey(), o2.getKey());
      if (keyCompare == 0) {
        int windowCompare =
            SignedBytes.lexicographicalComparator().compare(o1.getWindow(), o2.getWindow());
        if (windowCompare == 0) {
          return Long.compare(o1.getTimestamp(), o2.getTimestamp());
        }
        return windowCompare;
      }
      return keyCompare;
    }
  }


  /**
   * Transform stream of sorted key values into stream of value iterators for each key. This iterator
   * can be iterated only once!
   *
   * @param <K> type of key iterator emits
   * @param <V> type of value iterator emits
   */
  static class ReduceByKeyIterator<K, V, W extends BoundedWindow>
      extends AbstractIterator<Tuple2<WindowedValue<K>, Iterable<V>>> {

    private final PeekingIterator<Tuple2<CompositeKey, byte []>> inner;
    private CompositeKey previousKey;
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;
    private final Coder<W> windowCoder;

    ReduceByKeyIterator(
        Iterator<Tuple2<CompositeKey, byte[]>> inner,
        Coder<K> keyCoder, Coder<V> valueCoder, Coder<W> windowCoder) {
      this.inner = Iterators.peekingIterator(inner);
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
      this.windowCoder = windowCoder;
    }

    @Override
    protected Tuple2<WindowedValue<K>, Iterable<V>> computeNext() {
      while (inner.hasNext()) {
        // just peek, so the value iterator can see the first value
        Tuple2<CompositeKey, byte[]> peek = inner.peek();

        final CompositeKey currentKey = peek._1 ;

        if (currentKey.isSameKey(previousKey)) {
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
                    if (inner.hasNext() && currentKey.isSameKey(inner.peek()._1)) {
                      Tuple2<CompositeKey, byte[]> next = inner.next();
                      return getDecodedElement(next)._2;
                    }
                    return endOfData();
                  }
                });
      }
      return endOfData();
    }

    Tuple2<WindowedValue<K>, V> getDecodedElement(Tuple2<CompositeKey, byte[]> item){
      CompositeKey compositeKey = item._1;
      W w = CoderHelpers.fromByteArray(compositeKey.getWindow(), windowCoder);

      K key = CoderHelpers.fromByteArray(compositeKey.getKey(), keyCoder);
      WindowedValue<K> of = WindowedValue.of(key, new Instant(compositeKey.getTimestamp()), w, PaneInfo.ON_TIME_AND_ONLY_FIRING);//todo predavat pane
      return new Tuple2<>(of, CoderHelpers.fromByteArray(item._2, valueCoder));

    }
  }

}
