package org.apache.beam.sdk.io.hadoop.format;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;

public class CollectionCombinerTest {

  private static final TypeDescriptor<String> STRING_TYPE_DESCRIPTOR = TypeDescriptors.strings();

  private static final List<String> FIRST_ITEMS = Arrays.asList("a", "b", "c");
  private static final List<String> SECOND_ITEMS = Arrays.asList("c", "d", "e");

  @Test
  public void testCombining() {

    IterableCombinerFn<String> tested = new IterableCombinerFn<>(STRING_TYPE_DESCRIPTOR);

    IterableCombinerFn.CollectionAccumulator<String> first = tested.createAccumulator();
    FIRST_ITEMS.forEach(first::addInput);

    IterableCombinerFn.CollectionAccumulator<String> second = tested.createAccumulator();
    SECOND_ITEMS.forEach(second::addInput);

    IterableCombinerFn.CollectionAccumulator<String> merged =
        tested.mergeAccumulators(Arrays.asList(first, second));

    IterableCombinerFn.CollectionAccumulator<String> compacted = tested.compact(merged);

    String[] allItems =
        Stream.of(FIRST_ITEMS, SECOND_ITEMS).flatMap(List::stream).toArray(String[]::new);

    MatcherAssert.assertThat(compacted.extractOutput(), Matchers.containsInAnyOrder(allItems));
  }

  @Test
  public void testSerializing() throws IOException {

    IterableCombinerFn<String> tested = new IterableCombinerFn<>(STRING_TYPE_DESCRIPTOR);
    IterableCombinerFn.CollectionAccumulator<String> originalAccumulator =
        tested.createAccumulator();

    FIRST_ITEMS.forEach(originalAccumulator::addInput);

    Coder<IterableCombinerFn.CollectionAccumulator<String>> accumulatorCoder =
        tested.getAccumulatorCoder(null, StringUtf8Coder.of());

    byte[] bytes;

    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
      accumulatorCoder.encode(originalAccumulator, byteArrayOutputStream);
      byteArrayOutputStream.flush();

      bytes = byteArrayOutputStream.toByteArray();
    }

    IterableCombinerFn.CollectionAccumulator<String> decodedAccumulator;

    try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes)) {
      decodedAccumulator = accumulatorCoder.decode(byteArrayInputStream);
    }

    String[] originalItems = FIRST_ITEMS.toArray(new String[0]);

    MatcherAssert.assertThat(
        originalAccumulator.extractOutput(), Matchers.containsInAnyOrder(originalItems));
    MatcherAssert.assertThat(
        decodedAccumulator.extractOutput(), Matchers.containsInAnyOrder(originalItems));
  }
}
