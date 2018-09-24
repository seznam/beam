package org.apache.beam.sdk.io.hadoop.outputformat;

import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class HadoopOutputFormatIOSequenceFileTest {

  private static final String TEST_FOLDER_NAME = "test";
  private static final int REDUCERS_COUNT = 2;

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void test() {

    String outputDir =
        Paths.get(tmpFolder.getRoot().getAbsolutePath(), TEST_FOLDER_NAME)
            .toAbsolutePath()
            .toString();

    HadoopOutputFormatIO.Write<Text, IntWritable> write =
        createWriteTransform(
            SequenceFileOutputFormat.class,
            Text.class,
            IntWritable.class,
            outputDir,
            REDUCERS_COUNT);

    List<KV<Text, IntWritable>> data = createTestData();

    pipeline
        .apply(Create.of(data))
        .setTypeDescriptor(
            TypeDescriptors.kvs(
                new TypeDescriptor<Text>() {}, new TypeDescriptor<IntWritable>() {}))
        .apply(write);

    pipeline.run().waitUntilFinish();

    List<KV<Text, IntWritable>> results =
        Arrays.stream(Objects.requireNonNull(new File(outputDir).list()))
            .filter(fileName -> fileName.startsWith("part-r"))
            .map(fileName -> outputDir + File.separator + fileName)
            .flatMap(this::extractResults)
            .collect(Collectors.toList());

    assertThat(results, containsInAnyOrder(data.toArray(new KV[0])));
  }

  private List<KV<Text, IntWritable>> createTestData() {
    return Arrays.asList(
        KV.of(new Text("one"), new IntWritable(1)),
        KV.of(new Text("two"), new IntWritable(2)),
        KV.of(new Text("three"), new IntWritable(3)),
        KV.of(new Text("four"), new IntWritable(4)),
        KV.of(new Text("five"), new IntWritable(5)),
        KV.of(new Text("six"), new IntWritable(6)),
        KV.of(new Text("seven"), new IntWritable(7)));
  }

  private Stream<KV<Text, IntWritable>> extractResults(String fileName) {
    try (SequenceFileRecordReader<Text, IntWritable> reader = new SequenceFileRecordReader<>()) {
      Path path = new Path(fileName);
      TaskAttemptContext taskContext =
          HadoopUtils.createTaskContext(new Configuration(), new JobID("readJob", 0), 0);
      reader.initialize(
          new FileSplit(path, 0L, Long.MAX_VALUE, new String[] {"localhost"}), taskContext);
      List<KV<Text, IntWritable>> result = new ArrayList<>();

      while (reader.nextKeyValue()) {
        result.add(
            KV.of(
                new Text(reader.getCurrentKey().toString()),
                new IntWritable(reader.getCurrentValue().get())));
      }

      return result.stream();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private <KeyT, ValueT> HadoopOutputFormatIO.Write<KeyT, ValueT> createWriteTransform(
      Class<?> outputFormatClass,
      Class<?> keyClass,
      Class<?> valueClass,
      String path,
      Integer reducersCount) {

    Configuration conf = new Configuration();

    conf.setClass(
        HadoopOutputFormatIO.OUTPUT_FORMAT_CLASS_ATTR, outputFormatClass, OutputFormat.class);
    conf.setClass(HadoopOutputFormatIO.OUTPUT_FORMAT_KEY_CLASS_ATTR, keyClass, Object.class);
    conf.setClass(HadoopOutputFormatIO.OUTPUT_FORMAT_VALUE_CLASS_ATTR, valueClass, Object.class);
    conf.setInt(HadoopOutputFormatIO.NUM_REDUCES, reducersCount);

    conf.set(FileOutputFormat.OUTDIR, path);

    return HadoopOutputFormatIO.<KeyT, ValueT>write().withConfiguration(conf);
  }
}
