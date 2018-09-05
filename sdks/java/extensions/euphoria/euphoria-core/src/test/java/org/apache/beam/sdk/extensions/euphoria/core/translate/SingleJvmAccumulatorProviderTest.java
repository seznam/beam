package org.apache.beam.sdk.extensions.euphoria.core.translate;

import java.util.Map;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Counter;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Histogram;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.accumulators.SingleJvmAccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.accumulators.SingleJvmAccumulatorProvider.Factory;
import org.apache.beam.sdk.extensions.euphoria.core.util.Settings;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Unit test of {@link SingleJvmAccumulatorProvider}. Note that this test is placed outside of
 * {@code testkit} package on purpouse. All the other tests in {@code testkit} are not runnable by
 * JUnit directly.
 */
public class SingleJvmAccumulatorProviderTest {

  private static final String TEST_COUNTER_NAME = "test-counter";
  private static final String TEST_HISTOGRAM_NAME = "test-histogram";

  @Ignore(
      "Clashes with SingleValueCollectorTest since SingleJvmAccumulatorProvider do not respects namespaces.")
  @Test
  public void testBasicAccumulatorsFunction() {

    Factory accFactory = Factory.get();
    final AccumulatorProvider accumulators = accFactory.create(new Settings());

    Counter counter = accumulators.getCounter(TEST_COUNTER_NAME);
    System.out.println("counter: " + counter);
    Assert.assertNotNull(counter);

    counter.increment();
    counter.increment(2);

    Map<String, Long> counterSnapshots = accFactory.getCounterSnapshots();
    long counterValue = counterSnapshots.get(TEST_COUNTER_NAME);
    Assert.assertEquals(3L, counterValue);

    Histogram histogram = accumulators.getHistogram(TEST_HISTOGRAM_NAME);
    Assert.assertNotNull(histogram);

    histogram.add(1);
    histogram.add(2, 2);

    Map<String, Map<Long, Long>> histogramSnapshots = accFactory.getHistogramSnapshots();
    Map<Long, Long> histogramValue = histogramSnapshots.get(TEST_HISTOGRAM_NAME);

    long numOfValuesOfOne = histogramValue.get(1L);
    Assert.assertEquals(1L, numOfValuesOfOne);
    long numOfValuesOfTwo = histogramValue.get(2L);
    Assert.assertEquals(2L, numOfValuesOfTwo);

    // collector.getTimer() <- not yet supported
  }
}
