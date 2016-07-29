package cz.seznam.euphoria.fluent;

import cz.seznam.euphoria.core.client.dataset.Partitioner;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.operator.Distinct;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.OutputBuilder;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.executor.Executor;

import static java.util.Objects.requireNonNull;

public class Dataset<T> {

  private final cz.seznam.euphoria.core.client.dataset.Dataset<T> wrap;

  Dataset(cz.seznam.euphoria.core.client.dataset.Dataset<T> wrap) {
    this.wrap = requireNonNull(wrap);
  }

  public cz.seznam.euphoria.core.client.dataset.Dataset<T> unwrap() {
    return this.wrap;
  }

  public <S> Dataset<S>
  apply(UnaryFunction<cz.seznam.euphoria.core.client.dataset.Dataset<T>,
      OutputBuilder<S>> output)
  {
    return new Dataset<>(requireNonNull(output.apply(this.wrap)).output());
  }

  public Dataset<T> repartition(Partitioner<T> partitioner) {
    return new Dataset<>(Repartition.of(wrap)
        .setPartitioner(requireNonNull(partitioner))
        .output());
  }

  public Dataset<T> repartition(int num) {
    return new Dataset<>(Repartition.of(this.wrap)
        .setNumPartitions(num)
        .output());
  }

  public Dataset<T> repartition(int num, Partitioner<T> partitioner) {
    return new Dataset<>(Repartition.of(this.wrap)
        .setNumPartitions(num)
        .setPartitioner(requireNonNull(partitioner))
        .output());
  }

  public <S> Dataset<S> mapElements(UnaryFunction<T, S> f) {
    return new Dataset<>(MapElements.of(this.wrap).using(requireNonNull(f)).output());
  }

  public <S> Dataset<S> flatMap(UnaryFunctor<T, S> f) {
    return new Dataset<>(FlatMap.of(this.wrap).using(requireNonNull(f)).output());
  }

  public Dataset<T> distinct() {
    return new Dataset<>(Distinct.of(this.wrap).output());
  }

  public Dataset<T> union(Dataset<T> other) {
    return new Dataset<>(Union.of(this.wrap, other.wrap).output());
  }

  public <S extends DataSink<T>> Dataset<T> persist(S dst) {
    this.wrap.persist(dst);
    return this;
  }

  public void execute(Executor exec) throws Exception {
    exec.waitForCompletion(this.wrap.getFlow());
  }
}
