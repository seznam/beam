package org.apache.beam.sdk.extensions.euphoria.core.client.operator;

import com.google.common.collect.Iterables;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Builders;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.OutputHint;
import org.apache.beam.sdk.extensions.euphoria.core.translate.OperatorTransform;
import org.apache.beam.sdk.transforms.PTransform;

public class DatasetTransform<InputT, OutputT> extends Operator<OutputT>
    implements CompositeOperator<InputT, OutputT> {

  /**
   * Starts building a nameless {@link DatasetTransform} operator to process the given input dataset.
   *
   * @param <InputT> the type of elements of the input dataset
   * @param input the input data set to be processed
   * @return a builder to complete the setup of the new operator
   * @see #named(String)
   * @see OfBuilder#of(Dataset)
   */
  public static <InputT> UsingBuilder<InputT> of(Dataset<InputT> input) {
    return named(null).of(input);
  }

  /**
   * Starts building a named {@link DatasetTransform} operator.
   *
   * @param name a user provided name of the new operator to build
   * @return a builder to complete the setup of the new operator
   */
  public static OfBuilder named(@Nullable String name) {
    return new Builder(name);
  }

  /** Builder for the 'of' step */
  public interface OfBuilder extends Builders.Of {

    @Override
    <InputT> UsingBuilder<InputT> of(Dataset<InputT> input);
  }

  /** Builder for the 'using' step */
  public interface UsingBuilder<InputT> {

    <OutputT> OutputBuilder<OutputT> using(PTransform<Dataset<InputT>, Dataset<OutputT>> transform);
  }

  /** Builder for the 'output' step */
  public interface OutputBuilder<OutputT> extends Builders.Output<OutputT> {}

  private static class Builder<InputT, OutputT>
      implements OfBuilder, UsingBuilder<InputT>, OutputBuilder<OutputT> {

    @Nullable private final String name;
    private Dataset<InputT> input;
    private PTransform<Dataset<InputT>, Dataset<OutputT>> transform;

    Builder(@Nullable String name) {
      this.name = name;
    }

    @Override
    public <T> UsingBuilder<T> of(Dataset<T> input) {
      @SuppressWarnings("unchecked")
      final Builder<T, ?> casted = (Builder) this;
      casted.input = input;
      return casted;
    }

    @Override
    public <T> OutputBuilder<T> using(PTransform<Dataset<InputT>, Dataset<T>> transform) {
      @SuppressWarnings("unchecked")
      final Builder<InputT, T> casted = (Builder) this;
      casted.transform = transform;
      return casted;
    }

    @Override
    public Dataset<OutputT> output(OutputHint... outputHints) {
      final DatasetTransform<InputT, OutputT> operator = new DatasetTransform<>(name, transform);
      return OperatorTransform.apply(operator, Collections.singletonList(input));
    }
  }

  private final PTransform<Dataset<InputT>, Dataset<OutputT>> transform;

  private DatasetTransform(
      @Nullable String name, PTransform<Dataset<InputT>, Dataset<OutputT>> transform) {
    super(name, null);
    this.transform = transform;
  }

  public PTransform<Dataset<InputT>, Dataset<OutputT>> getTransform() {
    return transform;
  }

  @Override
  public Dataset<OutputT> expand(List<Dataset<InputT>> inputs) {
    final Dataset<InputT> input = Iterables.getOnlyElement(inputs);
    return getTransform().expand(input);
  }
}
