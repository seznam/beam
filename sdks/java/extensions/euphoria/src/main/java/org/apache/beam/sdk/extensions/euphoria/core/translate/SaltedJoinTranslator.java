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
package org.apache.beam.sdk.extensions.euphoria.core.translate;

import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAwareness;
import org.apache.beam.sdk.extensions.sketching.SketchFrequencies;
import org.apache.beam.sdk.extensions.sketching.SketchFrequencies.Sketch;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Instant;

public class SaltedJoinTranslator<LeftT, RightT, KeyT, OutputT>
    extends AbstractJoinTranslator<LeftT, RightT, KeyT, OutputT> {

  private final long desiredKeySize;

  public SaltedJoinTranslator(long desiredKeySize) {
    this.desiredKeySize = desiredKeySize;
  }

  @FunctionalInterface
  private interface SaltKeyFn<KeyT> {

    void apply(KeyT key, long count, OutputReceiver<Key<KeyT>> outputReceiver);
  }

  private static class Key<KeyT> {

    private final KeyT key;
    private final long salt;

    private Key(KeyT key, long salt) {
      this.key = key;
      this.salt = salt;
    }
  }

  private static class SaltKeys<KeyT, ValueT>
      extends DoFn<KV<KeyT, ValueT>, KV<Key<KeyT>, ValueT>> {

    private final PCollectionView<Sketch<KeyT>> keySketchView;
    private final Coder<KeyT> keyCoder;
    private final SaltKeyFn<KeyT> saltKeyFn;

    private SaltKeys(
        PCollectionView<Sketch<KeyT>> keySketchView,
        Coder<KeyT> keyCoder,
        SaltKeyFn<KeyT> saltKeyFn) {
      this.keySketchView = keySketchView;
      this.keyCoder = keyCoder;
      this.saltKeyFn = saltKeyFn;
    }

    @ProcessElement
    @SuppressWarnings("unused")
    public void processElement(ProcessContext ctx, OutputReceiver<KV<Key<KeyT>, ValueT>> outputReceiver) {
      final KV<KeyT, ValueT> element = ctx.element();
      final Sketch<KeyT> sketch = ctx.sideInput(keySketchView);
      final long count = sketch.estimateCount(element.getKey(), keyCoder);
      saltKeyFn.apply(element.getKey(), count, new OutputReceiver<Key<KeyT>>() {

        @Override
        public void output(Key<KeyT> key) {
          outputReceiver.output(KV.of(key, element.getValue()));
        }

        @Override
        public void outputWithTimestamp(Key<KeyT> key, Instant timestamp) {
          throw new UnsupportedOperationException("Timestamped output is not supported.");
        }
      });
    }
  }

  /**
   * @TODO
   *
   * @param <KeyT> key type
   */
  private static class HashSaltKeyFn<KeyT> implements SaltKeyFn<KeyT>  {

    private final long desiredKeySize;

    private HashSaltKeyFn(long desiredKeySize) {
      this.desiredKeySize = desiredKeySize;
    }

    @Override
    public void apply(KeyT key, long count, OutputReceiver<Key<KeyT>> outputReceiver) {
      final int numSplits = (int) (count / desiredKeySize);
      int salt = (key.hashCode() % numSplits) & Integer.MAX_VALUE;
      outputReceiver.output(new Key<>(key, salt));
    }
  }

  /**
   * @TODO
   *
   * @param <KeyT>
   */
  private static class DistributeSaltKeyFn<KeyT> implements SaltKeyFn<KeyT>  {

    private final long desiredKeySize;

    private DistributeSaltKeyFn(long desiredKeySize) {
      this.desiredKeySize = desiredKeySize;
    }

    @Override
    public void apply(KeyT key, long count, OutputReceiver<Key<KeyT>> outputReceiver) {
      final int numSplits = (int) (count / desiredKeySize);
      for (int salt = 0; salt < numSplits; salt++) {
        outputReceiver.output(new Key<>(key, salt));
      }
    }
  }

  @Override
  PCollection<KV<KeyT, OutputT>> translate(
      Join<LeftT, RightT, KeyT, OutputT> operator,
      PCollection<KV<KeyT, LeftT>> left,
      PCollection<KV<KeyT, RightT>> right) {

    final PCollectionView<Sketch<KeyT>> keySketchView =
        left.apply(Keys.create()).apply(SketchFrequencies.globally()).apply(View.asSingleton());

    final TypeDescriptor<KeyT> keyType = TypeAwareness.orObjects(operator.getKeyType());
    final Coder<KeyT> keyCoder;
    try {
      keyCoder = left.getPipeline().getCoderRegistry().getCoder(TypeAwareness.orObjects(operator.getKeyType()));
    } catch (CannotProvideCoderException e) {
      throw new IllegalStateException(
          "Unable to provide coder for key with type[" + keyType + "].", e);
    }

    final PCollection<KV<Key<KeyT>, LeftT>> leftSalted = left.apply(
        ParDo.of(
            new SaltKeys<>(
                keySketchView,
                keyCoder,
                new DistributeSaltKeyFn<>(desiredKeySize))));

    final PCollection<KV<Key<KeyT>, RightT>> rightSalted = right.apply(
        ParDo.of(
            new SaltKeys<>(
                keySketchView,
                keyCoder,
                new HashSaltKeyFn<>(desiredKeySize))));

    return null;
  }
}
