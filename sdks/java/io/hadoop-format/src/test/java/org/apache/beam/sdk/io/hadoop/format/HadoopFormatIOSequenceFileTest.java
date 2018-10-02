package org.apache.beam.sdk.io.hadoop.format;

import static org.hamcrest.Matchers.equalTo;

import java.io.File;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.examples.WordCount;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.hamcrest.MatcherAssert;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class HadoopFormatIOSequenceFileTest {

  private static final Instant START_TIME = new Instant(0);
  private static final String TEST_FOLDER_NAME = "test";
  private static final int REDUCERS_COUNT = 2;

  private static final List<String> SENTENCES =
      Arrays.asList(
          "Hello world this is first streamed event",
          "Hello again this is sedcond streamed event",
          "Third time Hello event created",
          "And last event will was sent now",
          "Hello from second window",
          "First event from second window");

  private static final List<String> ON_TIME_EVENTS = SENTENCES.subList(0, 4);
  private static final List<String> LATE_EVENTS = SENTENCES.subList(4, 6);
  public static final Duration WINDOW_DURATION = Duration.standardSeconds(30);

  private static Map<String, Long> computeWordCounts(List<String> sentences) {
    return sentences
        .stream()
        .flatMap(s -> Stream.of(s.split("\\W+")))
        .map(String::toLowerCase)
        .collect(Collectors.toMap(Function.identity(), s -> Long.valueOf(1L), Long::sum));
  }

  @Rule
  public TemporaryFolder tmpFolder =
      new TemporaryFolder() {
        @Override
        protected void after() {}
      };

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void batchTest() {

    String outputDir = getOutputDirPath();

    Configuration conf =
        createWriteConf(
            SequenceFileOutputFormat.class,
            Text.class,
            LongWritable.class,
            outputDir,
            REDUCERS_COUNT);

    pipeline
        .apply(Create.of(SENTENCES))
        .apply(ParDo.of(new ConvertToLowerCaseFn()))
        .apply(new WordCount.CountWords())
        .apply("ConvertToHadoopFormat", ParDo.of(new ConvertToHadoopFormatFn()))
        .setTypeDescriptor(
            TypeDescriptors.kvs(
                new TypeDescriptor<Text>() {}, new TypeDescriptor<LongWritable>() {}))
        .apply(HadoopFormatIO.<Text, LongWritable>write().withConfiguration(conf));

    pipeline.run().waitUntilFinish();

    Map<String, Long> results = loadWrittenDataAsMap(outputDir);

    MatcherAssert.assertThat(results.entrySet(), equalTo(computeWordCounts(SENTENCES).entrySet()));
  }

  private List<KV<Text, LongWritable>> loadWrittenData(String outputDir) {
    return Arrays.stream(Objects.requireNonNull(new File(outputDir).list()))
        .filter(fileName -> fileName.startsWith("part-r"))
        .map(fileName -> outputDir + File.separator + fileName)
        .flatMap(this::extractResultsFromFile)
        .collect(Collectors.toList());
  }

  private String getOutputDirPath() {
    return Paths.get(tmpFolder.getRoot().getAbsolutePath(), TEST_FOLDER_NAME)
        .toAbsolutePath()
        .toString();
  }

  private Stream<KV<Text, LongWritable>> extractResultsFromFile(String fileName) {
    try (SequenceFileRecordReader<Text, LongWritable> reader = new SequenceFileRecordReader<>()) {
      Path path = new Path(fileName);
      TaskAttemptContext taskContext =
          HadoopUtils.createTaskContext(new Configuration(), new JobID("readJob", 0), 0);
      reader.initialize(
          new FileSplit(path, 0L, Long.MAX_VALUE, new String[] {"localhost"}), taskContext);
      List<KV<Text, LongWritable>> result = new ArrayList<>();

      while (reader.nextKeyValue()) {
        result.add(
            KV.of(
                new Text(reader.getCurrentKey().toString()),
                new LongWritable(reader.getCurrentValue().get())));
      }

      return result.stream();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Configuration createWriteConf(
      Class<?> outputFormatClass,
      Class<?> keyClass,
      Class<?> valueClass,
      String path,
      Integer reducersCount) {

    return getConfiguration(outputFormatClass, keyClass, valueClass, path, reducersCount);
  }

  private static Configuration getConfiguration(
      Class<?> outputFormatClass,
      Class<?> keyClass,
      Class<?> valueClass,
      String path,
      Integer reducersCount) {
    Configuration conf = new Configuration();

    conf.setClass(HadoopFormatIO.OUTPUT_FORMAT_CLASS_ATTR, outputFormatClass, OutputFormat.class);
    conf.setClass(HadoopFormatIO.OUTPUT_KEY_CLASS, keyClass, Object.class);
    conf.setClass(HadoopFormatIO.OUTPUT_VALUE_CLASS, valueClass, Object.class);
    conf.setInt(HadoopFormatIO.NUM_REDUCES, reducersCount);
    conf.set(FileOutputFormat.OUTDIR, path);
    return conf;
  }

  @Test
  public void streamTest() {

    TestStream<String> stringsStream =
        TestStream.create(StringUtf8Coder.of())
            .advanceWatermarkTo(START_TIME)
            .addElements(event(ON_TIME_EVENTS.get(0), 2L))
            .advanceWatermarkTo(START_TIME.plus(Duration.standardSeconds(27L)))
            .addElements(
                event(ON_TIME_EVENTS.get(1), 25L),
                event(ON_TIME_EVENTS.get(2), 18L),
                event(ON_TIME_EVENTS.get(3), 28L))
            .advanceWatermarkTo(START_TIME.plus(Duration.standardSeconds(108)))
            .addElements(event(LATE_EVENTS.get(0), 0L), event(LATE_EVENTS.get(1), 2L))
            .advanceWatermarkToInfinity();

    String outputDirPath = getOutputDirPath();

    pipeline
        .apply(stringsStream)
        .apply(Window.into(FixedWindows.of(WINDOW_DURATION)))
        .apply(ParDo.of(new ConvertToLowerCaseFn()))
        .apply(new WordCount.CountWords())
        .apply("ConvertToHadoopFormat", ParDo.of(new ConvertToHadoopFormatFn()))
        .apply(
            HadoopFormatIO.<Text, LongWritable>write()
                .withConfigurationTransformation(ParDo.of(new ConfigProvider(outputDirPath))));

    pipeline.run().waitUntilFinish();

    Map<String, Long> values = loadWrittenDataAsMap(outputDirPath);

    MatcherAssert.assertThat(
        values.entrySet(), equalTo(computeWordCounts(ON_TIME_EVENTS).entrySet()));
  }

  //  @Test
  //  public void streamTestWithEarlyFiring() {
  //
  //    TestStream<Integer> intStream =
  //        TestStream.create(VarIntCoder.of())
  //            .advanceWatermarkTo(START_TIME)
  //            .addElements(event(1, 2L), event(2, 2L))
  //            .advanceWatermarkTo(START_TIME.plus(Duration.standardSeconds(10L)))
  //            .addElements(event(3, 18L), event(4, 28L))
  //            .advanceWatermarkTo(START_TIME.plus(Duration.standardSeconds(250L)))
  //            .advanceWatermarkToInfinity();
  //
  //    String outputDirPath = getOutputDirPath();
  //
  //    TypeDescriptor<Integer> intTypeDesc = new TypeDescriptor<Integer>() {};
  //
  //    MapElements<Integer, KV<Integer, Integer>> mapFunc =
  //        MapElements.via(
  //            SimpleFunction.fromSerializableFunctionWithOutputType(
  //                (Integer i) -> KV.of(1, i), TypeDescriptors.kvs(intTypeDesc, intTypeDesc)));
  //
  //    pipeline
  //        .apply(intStream)
  //        .apply(
  //            Window.<Integer>into(FixedWindows.of(WINDOW_DURATION))
  //                .triggering(AfterPane.elementCountAtLeast(1))
  //                .withAllowedLateness(Duration.standardSeconds(100))
  //                .accumulatingFiredPanes())
  //        .apply(mapFunc)
  //        .apply(GroupByKey.create())
  //        .apply(ParDo.of(new ElementLogger()));
  //    //        .apply("ConvertToHadoopFormat", ParDo.of(new ConvertToHadoopFormatFn()))
  //    //        .apply(
  //    //            HadoopFormatIO.<Text, LongWritable>write()
  //    //                .withConfigurationTransformation(ParDo.of(new
  //    // ConfigProvider(outputDirPath))));
  //
  //    pipeline.run().waitUntilFinish();
  //
  //    Map<String, Long> values = loadWrittenDataAsMap(outputDirPath);
  //
  //    MatcherAssert.assertThat(
  //        values.entrySet(), equalTo(computeWordCounts(ON_TIME_EVENTS).entrySet()));
  //  }

  private Map<String, Long> loadWrittenDataAsMap(String outputDirPath) {
    return loadWrittenData(outputDirPath)
        .stream()
        .collect(Collectors.toMap(kv -> kv.getKey().toString(), kv -> kv.getValue().get()));
  }

  private <T> TimestampedValue<T> event(T eventValue, Long dur) {

    return TimestampedValue.of(eventValue, START_TIME.plus(new Duration(dur)));
  }

  private static class ConvertToHadoopFormatFn
      extends DoFn<KV<String, Long>, KV<Text, LongWritable>> {

    @DoFn.ProcessElement
    public void processElement(
        @DoFn.Element KV<String, Long> element,
        OutputReceiver<KV<Text, LongWritable>> outReceiver) {

      outReceiver.output(KV.of(new Text(element.getKey()), new LongWritable(element.getValue())));
    }
  }

  private static class ConvertToLowerCaseFn extends DoFn<String, String> {
    @DoFn.ProcessElement
    public void processElement(@DoFn.Element String element, OutputReceiver<String> receiver) {
      receiver.output(element.toLowerCase());
    }
  }

  private static class ConfigProvider extends DoFn<KV<Text, LongWritable>, Configuration> {

    private String outputDirPath;

    public ConfigProvider(String outputDirPath) {
      this.outputDirPath = outputDirPath;
    }

    @DoFn.ProcessElement
    public void processElement(OutputReceiver<Configuration> receiver) {

      receiver.output(
          createWriteConf(
              SequenceFileOutputFormat.class,
              Text.class,
              LongWritable.class,
              outputDirPath,
              REDUCERS_COUNT));
    }
  }

  private static class ElementLogger<KeyT, ValueT>
      extends DoFn<KV<KeyT, ValueT>, KV<KeyT, ValueT>> {

    @DoFn.ProcessElement
    public void processElement(
        @DoFn.Element KV<KeyT, ValueT> element,
        OutputReceiver<KV<KeyT, ValueT>> receiver,
        DoFn<KV<KeyT, ValueT>, KV<KeyT, ValueT>>.ProcessContext context,
        BoundedWindow window) {

      System.out.println(
          String.format(
              "%4s element %20s in window with end in %s, pane id: %s, pane timing: %s %s",
              Thread.currentThread().getId(),
              element,
              window.maxTimestamp(),
              context.pane().getIndex(),
              context.pane().getTiming(),
              context.pane().toString()));

      receiver.output(element);
    }
  }
}
