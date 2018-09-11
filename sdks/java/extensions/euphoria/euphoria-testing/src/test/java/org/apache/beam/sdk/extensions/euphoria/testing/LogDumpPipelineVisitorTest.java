package org.apache.beam.sdk.extensions.euphoria.testing;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.DataSource;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.ListDataSource;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceByKey;
import org.apache.beam.sdk.extensions.euphoria.core.translate.BeamFlow;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.KV;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

/** Unit tests of {@link LogDumpPipelineVisitor}. */
public class LogDumpPipelineVisitorTest {

  @Rule public TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void testVisitor() {

    BeamFlow flow = BeamFlow.of(testPipeline);

    DataSource<Integer> source = ListDataSource.bounded(Arrays.asList(1, 2, 3, 4, 5));
    Dataset<Integer> input = flow.createInput(source);

    Dataset<KV<Integer, Set<Integer>>> output =
        ReduceByKey.of(input)
            .keyBy(e -> e % 2)
            .valueBy(e -> e)
            .reduceBy(s -> s.collect(Collectors.toSet()))
            .output();

    LogDumpPipelineVisitor visitor = new LogDumpPipelineVisitor();
    testPipeline.traverseTopologically(visitor);

    Assert.assertEquals(0, visitor.nesting.length());
    Assert.assertTrue(visitor.outBuff.length() > 0);

    testPipeline.run();
  }
}
