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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.Supplier;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join;
import org.apache.beam.sdk.extensions.euphoria.core.translate.collector.BufferingCollector;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

/** {@link OperatorTranslator Translator } for Euphoria {@link Join} operator. */
public class JoinTranslator<LeftT, RightT, KeyT, OutputT>
    extends AbstractJoinTranslator<LeftT, RightT, KeyT, OutputT> {

  private static class JoinProductIterable<T> implements Iterable<T> {

    private final Iterator<Iterator<T>> partsSource;
    private Iterator<T> currentPart;

    private JoinProductIterable(Supplier<Iterator<Iterator<T>>> partsSource) {
      this.partsSource = partsSource.get();
    }

    @Override
    public Iterator<T> iterator() {
      return new AbstractIterator<T>() {

        @Override
        protected T computeNext() {

          if (currentPart == null) {
            if (partsSource.hasNext()) {
              currentPart = partsSource.next();
            } else {
              return endOfData();
            }
          }

          while (!currentPart.hasNext()) {
            if (partsSource.hasNext()) {
              currentPart = partsSource.next();
            } else {
              return endOfData();
            }
          }

          return currentPart.next();
        }
      };
    }
  }

  private static class JoinProductFlatmapFn<LeftT, RightT, KeyT>
      implements ProcessFunction<KV<KeyT, CoGbkResult>, Iterable<KV<LeftT, RightT>>> {

    private final TupleTag<LeftT> leftTag;
    private final TupleTag<RightT> rightTag;
    private final BinaryFunction<
            Iterable<LeftT>, Iterable<RightT>, Iterator<Iterator<KV<LeftT, RightT>>>>
        productFn;
    private final String name;

    JoinProductFlatmapFn(
        TupleTag<LeftT> leftTag,
        TupleTag<RightT> rightTag,
        BinaryFunction<Iterable<LeftT>, Iterable<RightT>, Iterator<Iterator<KV<LeftT, RightT>>>>
            productFn,
        String name) {
      this.leftTag = leftTag;
      this.rightTag = rightTag;
      this.productFn = productFn;
      this.name = name;
    }

    @Override
    public Iterable<KV<LeftT, RightT>> apply(KV<KeyT, CoGbkResult> oneKeyCoGrouped)
        throws Exception {
      requireNonNull(oneKeyCoGrouped);
      Iterable<LeftT> leftSide = requireNonNull(oneKeyCoGrouped.getValue()).getAll(leftTag);
      Iterable<RightT> rightSide = requireNonNull(oneKeyCoGrouped.getValue()).getAll(rightTag);

      return new JoinProductIterable<>(() -> productFn.apply(leftSide, rightSide));
    }

    String getFnName() {
      return name;
    }
  }

  private static class InnerJoinProductFlatmapFn<LeftT, RightT>
      implements BinaryFunction<
          Iterable<LeftT>, Iterable<RightT>, Iterator<Iterator<KV<LeftT, RightT>>>> {

    @Override
    public Iterator<Iterator<KV<LeftT, RightT>>> apply(
        Iterable<LeftT> leftSide, Iterable<RightT> rightSide) {

      //TODO which part of join fits in mem should be translator param
      final Iterator<LeftT> leftIter =
          leftSide.iterator(); //TODO we expexts left side to be smaller not bigger !! swap them !

      return new AbstractIterator<Iterator<KV<LeftT, RightT>>>() {
        @Override
        protected Iterator<KV<LeftT, RightT>> computeNext() {
          if (!leftIter.hasNext()) {
            return endOfData();
          }

          LeftT leftItem = leftIter.next();
          List<KV<LeftT, RightT>> out = new ArrayList<>();
          for (RightT rightItem : rightSide) {
            out.add(KV.of(leftItem, rightItem));
          }

          return out.iterator();
        }
      };
    }
  }

  private static class FullJoinProductFlatmapFn<LeftT, RightT, K>
      implements BinaryFunction<
          Iterable<LeftT>, Iterable<RightT>, Iterator<Iterator<KV<LeftT, RightT>>>> {

    @Override
    public Iterator<Iterator<KV<LeftT, RightT>>> apply(
        Iterable<LeftT> left, Iterable<RightT> right) {

      Iterator<RightT> rightIter = right.iterator();
      final boolean rightHasValues = rightIter.hasNext();

      final Iterator<LeftT> leftIter = left.iterator();
      final boolean leftHasValues = leftIter.hasNext();

      if (leftHasValues && rightHasValues) {

        return new AbstractIterator<Iterator<KV<LeftT, RightT>>>() { //TODO prepsat na hezci
          @Override
          protected Iterator<KV<LeftT, RightT>> computeNext() {
            if (!rightIter.hasNext()) {
              return endOfData();
            }

            List<KV<LeftT, RightT>> out = new ArrayList<>();

            RightT rightValue = rightIter.next();
            for (LeftT leftValue : left) {
              out.add(KV.of(leftValue, rightValue));
            }

            return out.iterator();
          }
        };

      } else if (leftHasValues) {
        return new AbstractIterator<Iterator<KV<LeftT, RightT>>>() {
          private boolean depleted = false;

          @Override
          protected Iterator<KV<LeftT, RightT>> computeNext() {
            if (depleted) {
              return endOfData();
            }

            List<KV<LeftT, RightT>> out = new ArrayList<>();

            for (LeftT leftValue : left) {
              out.add(KV.of(leftValue, null));
            }

            depleted = true;
            return out.iterator();
          }
        };

      } else if (rightHasValues) {

        return new AbstractIterator<Iterator<KV<LeftT, RightT>>>() {
          private boolean depleted = false;

          @Override
          protected Iterator<KV<LeftT, RightT>> computeNext() {
            if (depleted) {
              return endOfData();
            }

            List<KV<LeftT, RightT>> out = new ArrayList<>();
            for (RightT rightValue : right) {
              out.add(KV.of(null, rightValue));
            }

            depleted = true;
            return out.iterator();
          }
        };
      }

      return Collections.emptyIterator();
    }
  }

  private static class LeftOuterJoinProductFlatmapFn<LeftT, RightT>
      implements BinaryFunction<
          Iterable<LeftT>, Iterable<RightT>, Iterator<Iterator<KV<LeftT, RightT>>>> {

    @Override
    public Iterator<Iterator<KV<LeftT, RightT>>> apply(
        Iterable<LeftT> left, Iterable<RightT> right) {

      Iterator<LeftT> leftIter = left.iterator();

      return new AbstractIterator<Iterator<KV<LeftT, RightT>>>() {
        @Override
        protected Iterator<KV<LeftT, RightT>> computeNext() {

          if (!leftIter.hasNext()) {
            return endOfData();
          }

          LeftT leftValue = leftIter.next();

          if (right.iterator().hasNext()) {
            List<KV<LeftT, RightT>> out = new ArrayList<>();
            for (RightT rightValue : right) {
              out.add(KV.of(leftValue, rightValue));
            }
            return out.iterator();

          } else {
            return Iterators.singletonIterator(KV.of(leftValue, null));
          }
        }
      };
    }
  }

  private static class RightOuterJoinProductFlatmapFn<LeftT, RightT, K>
      implements BinaryFunction<
          Iterable<LeftT>, Iterable<RightT>, Iterator<Iterator<KV<LeftT, RightT>>>> {

    @Override
    public Iterator<Iterator<KV<LeftT, RightT>>> apply(
        Iterable<LeftT> left, Iterable<RightT> right) {

      Iterator<RightT> rightIter = right.iterator();

      return new AbstractIterator<Iterator<KV<LeftT, RightT>>>() {
        @Override
        protected Iterator<KV<LeftT, RightT>> computeNext() {

          if (!rightIter.hasNext()) {
            return endOfData();
          }

          RightT rightValue = rightIter.next();

          if (left.iterator().hasNext()) {
            List<KV<LeftT, RightT>> out = new ArrayList<>();
            for (LeftT leftValue : left) {
              out.add(KV.of(leftValue, rightValue));
            }
            return out.iterator();
          } else {
            return Iterators.singletonIterator(KV.of(null, rightValue));
          }
        }
      };
    }
  }

  /**
   * Does join operation defined by user function on specific KVs of data elements.
   *
   * @param <LeftT>
   * @param <RightT>
   * @param <KeyT>
   * @param <OutputT>
   */
  private static class UserDefinedJoinFn<LeftT, RightT, KeyT, OutputT>
      implements ProcessFunction<KV<LeftT, RightT>, Iterable<KV<KeyT, OutputT>>> {

    /** User defined join function. */
    private final BinaryFunctor<LeftT, RightT, OutputT> joinerUdf;

    private final UnaryFunction<LeftT, KeyT> leftKeyExtractor;
    private final UnaryFunction<RightT, KeyT> rightKeyExtractor;

    private final BufferingCollector<OutputT> collector;

    private UserDefinedJoinFn(
        Join<LeftT, RightT, KeyT, OutputT> operator, AccumulatorProvider accumulators) {
      this.joinerUdf = operator.getJoiner();
      this.leftKeyExtractor = operator.getLeftKeyExtractor();
      this.rightKeyExtractor = operator.getRightKeyExtractor();
      this.collector = new BufferingCollector<>(accumulators, operator.getName().orElse(null));
    }

    @Override
    public Iterable<KV<KeyT, OutputT>> apply(KV<LeftT, RightT> input) throws Exception {
      LeftT leftElement = input.getKey();
      RightT rightElement = input.getValue();
      KeyT key =
          (leftElement != null)
              ? leftKeyExtractor.apply(leftElement)
              : rightKeyExtractor.apply(rightElement);

      collector.reset();
      joinerUdf.apply(leftElement, rightElement, collector);

      return collector
          .getBuffer()
          .stream()
          .map((joinedElement) -> KV.of(key, joinedElement))
          .collect(Collectors.toList());
    }
  }

  private static <KeyT, LeftT, RightT, OutputT> JoinProductFlatmapFn<LeftT, RightT, KeyT> getJoinFn(
      Join<LeftT, RightT, KeyT, OutputT> operator,
      TupleTag<LeftT> leftTag,
      TupleTag<RightT> rightTag) {

    switch (operator.getType()) {
      case INNER:
        return new JoinProductFlatmapFn<>(
            leftTag, rightTag, new InnerJoinProductFlatmapFn<>(), "inner-join");
      case LEFT:
        return new JoinProductFlatmapFn<>(
            leftTag, rightTag, new LeftOuterJoinProductFlatmapFn<>(), "left-outer-join");
      case RIGHT:
        return new JoinProductFlatmapFn<>(
            leftTag, rightTag, new RightOuterJoinProductFlatmapFn<>(), "right-outer-join");
      case FULL:
        return new JoinProductFlatmapFn<>(
            leftTag, rightTag, new FullJoinProductFlatmapFn<>(), "full-join");
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Cannot translate Euphoria '%s' operator to Beam transformations."
                    + " Given join type '%s' is not supported.",
                Join.class.getSimpleName(), operator.getType()));
    }
  }

  @Override
  PCollection<KV<KeyT, OutputT>> translate(
      Join<LeftT, RightT, KeyT, OutputT> operator,
      PCollection<LeftT> left,
      PCollection<KV<KeyT, LeftT>> leftKeyed,
      PCollection<RightT> right,
      PCollection<KV<KeyT, RightT>> rightKeyed) {

    final AccumulatorProvider accumulators =
        new LazyAccumulatorProvider(AccumulatorProvider.of(leftKeyed.getPipeline()));
    final TupleTag<LeftT> leftTag = new TupleTag<>();
    final TupleTag<RightT> rightTag = new TupleTag<>();

    final JoinProductFlatmapFn<LeftT, RightT, KeyT> joinFnProduct =
        getJoinFn(operator, leftTag, rightTag);

    final ProcessFunction<KV<LeftT, RightT>, Iterable<KV<KeyT, OutputT>>> userJoinFn =
        new UserDefinedJoinFn<>(operator, accumulators);

    //TODO this is repeated code
    final TypeDescriptor<KV<KeyT, OutputT>> outType =
        operator
            .getOutputType()
            .orElseThrow(
                () -> new IllegalStateException("Unable to infer output type descriptor."));

    //TODO not sure about this
    TypeDescriptor<LeftT> leftTypeDescriptor = left.getTypeDescriptor();
    TypeDescriptor<RightT> rightTypeDescriptor = right.getTypeDescriptor();

    Coder<LeftT> leftNullableCoder = NullableCoder.of(left.getCoder());
    Coder<RightT> rightNullableCoder = NullableCoder.of(right.getCoder());

    return KeyedPCollectionTuple.of(leftTag, leftKeyed)
        .and(rightTag, rightKeyed)
        .apply("co-group-by-key", CoGroupByKey.create())
        .apply(
            joinFnProduct.getFnName() + "-elem-product",
            FlatMapElements.into(TypeDescriptors.kvs(leftTypeDescriptor, rightTypeDescriptor))
                .via(joinFnProduct))
        .setCoder(KvCoder.of(leftNullableCoder, rightNullableCoder))
        .apply(operator.getName() + "-join", FlatMapElements.into(outType).via(userJoinFn));
  }
}
