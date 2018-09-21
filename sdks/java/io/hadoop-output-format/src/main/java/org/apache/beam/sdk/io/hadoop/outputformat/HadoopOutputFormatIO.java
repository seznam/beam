/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.sdk.io.hadoop.outputformat;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.*;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link HadoopOutputFormatIO} is a Transform for writing data to any sink which implements
 * Hadoop {@link OutputFormat}. For example - Cassandra, Elasticsearch, HBase, Redis, Postgres etc.
 * {@link HadoopOutputFormatIO} has to make several performance trade-offs in connecting to {@link
 * OutputFormat}, so if there is another Beam IO Transform specifically for connecting to your data
 * sink of choice, we would recommend using that one, but this IO Transform allows you to connect to
 * many data sinks that do not yet have a Beam IO Transform.
 *
 * <p>You will need to pass a Hadoop {@link Configuration} with parameters specifying how the write
 * will occur. Many properties of the Configuration are optional, and some are required for certain
 * {@link OutputFormat} classes, but the following properties must be set for all OutputFormats:
 *
 * <ul>
 *   <li>{@code mapreduce.job.outputformat.class}: The {@link OutputFormat} class used to connect to
 *       your data sink of choice.
 *   <li>{@code mapreduce.job.outputformat.key.class}: The key class passed to the {@link
 *       OutputFormat} in {@code mapreduce.job.outputformat.class}.
 *   <li>{@code mapreduce.job.outputformat.value.class}: The value class passed to the {@link
 *       OutputFormat} in {@code mapreduce.job.outputformat.class}.
 * </ul>
 *
 * <p>For example:
 *
 * <pre>{@code
 * Configuration myHadoopConfiguration = new Configuration(false);
 * // Set Hadoop OutputFormat, key and value class in configuration
 * myHadoopConfiguration.setClass(&quot;mapreduce.job.outputformat.class&quot;,
 *    MyDbOutputFormatClass, OutputFormat.class);
 * myHadoopConfiguration.setClass(&quot;mapreduce.job.outputformat.key.class&quot;,
 *    MyDbOutputFormatKeyClass, Object.class);
 * myHadoopConfiguration.setClass(&quot;mapreduce.job.outputformat.value.class&quot;,
 *    MyDbOutputFormatValueClass, Object.class);
 * }</pre>
 *
 * <p>You will need to set appropriate OutputFormat key and value class (i.e.
 * "mapreduce.job.outputformat.key.class" and "mapreduce.job.outputformat.value.class") in Hadoop
 * {@link Configuration}. If you set different OutputFormat key or value class than OutputFormat's
 * actual key or value class then, it may result in an error like "unexpected extra bytes after
 * decoding" while the decoding process of key/value object happens. Hence, it is important to set
 * appropriate OutputFormat key and value class.
 *
 * <h3>Writing using {@link HadoopOutputFormatIO}</h3>
 *
 * <pre>{@code
 * Pipeline p = ...; // Create pipeline.
 * // Read data only with Hadoop configuration.
 * p.apply("read",
 *     HadoopOutputFormatIO.<OutputFormatKeyClass, OutputFormatKeyClass>write()
 *              .withConfiguration(myHadoopConfiguration);
 * }</pre>
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class HadoopOutputFormatIO {
  private static final Logger LOGGER = LoggerFactory.getLogger(HadoopOutputFormatIO.class);

  public static final String OUTPUT_FORMAT_CLASS_ATTR = MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR;
  public static final String OUTPUT_FORMAT_KEY_CLASS_ATTR = "mapreduce.job.outputformat.key.class";
  public static final String OUTPUT_FORMAT_VALUE_CLASS_ATTR =
      "mapreduce.job.outputformat.value.class";
  public static final String NUM_REDUCES = MRJobConfig.NUM_REDUCES;
  public static final String PARTITIONER_CLASS_ATTR = MRJobConfig.PARTITIONER_CLASS_ATTR;

  /**
   * Creates an uninitialized {@link HadoopOutputFormatIO.Write}. Before use, the {@code Write} must
   * be initialized with a HadoopOutputFormatIO.Write#withConfiguration(HadoopConfiguration) that
   * specifies the sink.
   */
  public static <KeyT, ValueT> Write<KeyT, ValueT> write() {
    return new AutoValue_HadoopOutputFormatIO_Write.Builder<KeyT, ValueT>().build();
  }

  /**
   * A {@link PTransform} that writes to any data sink which implements Hadoop OutputFormat. For
   * e.g. Cassandra, Elasticsearch, HBase, Redis, Postgres, etc. See the class-level Javadoc on
   * {@link HadoopOutputFormatIO} for more information.
   *
   * @param <KeyT> Type of keys to be written.
   * @param <ValueT> Type of values to be written.
   * @see HadoopOutputFormatIO
   */
  @AutoValue
  public abstract static class Write<KeyT, ValueT>
      extends PTransform<PCollection<KV<KeyT, ValueT>>, PDone> {

    // Returns the Hadoop Configuration which contains specification of sink.
    @Nullable
    public abstract SerializableConfiguration getConfiguration();

    @Nullable
    public abstract TypeDescriptor<?> getOutputFormatClass();

    @Nullable
    public abstract TypeDescriptor<KeyT> getOutputFormatKeyClass();

    @Nullable
    public abstract TypeDescriptor<ValueT> getOutputFormatValueClass();

    @Nullable
    public abstract Integer getReducersCount();

    @Nullable
    public abstract Partitioner<KeyT, ValueT> getPartitioner();

    abstract Builder<KeyT, ValueT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<KeyT, ValueT> {
      abstract Builder<KeyT, ValueT> setConfiguration(SerializableConfiguration configuration);

      abstract Builder<KeyT, ValueT> setOutputFormatClass(TypeDescriptor<?> outputFormatClass);

      abstract Builder<KeyT, ValueT> setOutputFormatKeyClass(
          TypeDescriptor<KeyT> outputFormatKeyClass);

      abstract Builder<KeyT, ValueT> setOutputFormatValueClass(
          TypeDescriptor<ValueT> outputFormatValueClass);

      abstract Builder<KeyT, ValueT> setReducersCount(Integer partitionsCount);

      abstract Builder<KeyT, ValueT> setPartitioner(Partitioner<KeyT, ValueT> partitioner);

      abstract Write<KeyT, ValueT> build();
    }

    /** Write to the sink using the options provided by the given configuration. */
    @SuppressWarnings("unchecked")
    public Write<KeyT, ValueT> withConfiguration(Configuration configuration)
        throws IllegalArgumentException {

      validateConfiguration(configuration);
      fillDefaultPropertiesIfMissing(configuration);

      TypeDescriptor<?> outputFormatClass =
          TypeDescriptor.of(configuration.getClass(OUTPUT_FORMAT_CLASS_ATTR, null));
      TypeDescriptor<KeyT> outputFormatKeyClass =
          (TypeDescriptor<KeyT>)
              TypeDescriptor.of(configuration.getClass(OUTPUT_FORMAT_KEY_CLASS_ATTR, null));
      TypeDescriptor<ValueT> outputFormatValueClass =
          (TypeDescriptor<ValueT>)
              TypeDescriptor.of(configuration.getClass(OUTPUT_FORMAT_VALUE_CLASS_ATTR, null));

      int reducersCount = HadoopUtils.getReducersCountFromConfig(configuration);
      Partitioner<KeyT, ValueT> partitioner = HadoopUtils.getPartitionerFromConfig(configuration);

      Builder<KeyT, ValueT> builder =
          toBuilder().setConfiguration(new SerializableConfiguration(configuration));
      builder.setOutputFormatClass(outputFormatClass);
      builder.setOutputFormatKeyClass(outputFormatKeyClass);
      builder.setOutputFormatValueClass(outputFormatValueClass);
      builder.setReducersCount(reducersCount);
      builder.setPartitioner(partitioner);

      return builder.build();
    }

    private void fillDefaultPropertiesIfMissing(Configuration configuration) {
      configuration.setIfUnset(NUM_REDUCES, String.valueOf(HadoopUtils.DEFAULT_NUM_REDUCERS));
      configuration.setIfUnset(
          PARTITIONER_CLASS_ATTR, HadoopUtils.DEFAULT_PARTITIONER_CLASS_ATTR.getName());
    }

    /**
     * Validates that the mandatory configuration properties such as OutputFormat class,
     * OutputFormat key and value classes are provided in the Hadoop configuration.
     */
    private void validateConfiguration(Configuration configuration) {
      checkArgument(configuration != null, "Configuration can not be null");
      checkArgument(
          configuration.get(OUTPUT_FORMAT_CLASS_ATTR) != null,
          "Configuration must contain \"" + OUTPUT_FORMAT_CLASS_ATTR + "\"");
      checkArgument(
          configuration.get(OUTPUT_FORMAT_KEY_CLASS_ATTR) != null,
          "Configuration must contain \"" + OUTPUT_FORMAT_KEY_CLASS_ATTR + "\"");
      checkArgument(
          configuration.get(OUTPUT_FORMAT_VALUE_CLASS_ATTR) != null,
          "Configuration must contain \"" + OUTPUT_FORMAT_VALUE_CLASS_ATTR + "\"");
    }

    @Override
    public void validate(PipelineOptions pipelineOptions) {}

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      Configuration hadoopConfig = getConfiguration().get();
      if (hadoopConfig != null) {
        builder.addIfNotNull(
            DisplayData.item(OUTPUT_FORMAT_CLASS_ATTR, hadoopConfig.get(OUTPUT_FORMAT_CLASS_ATTR))
                .withLabel("OutputFormat Class"));
        builder.addIfNotNull(
            DisplayData.item(
                    OUTPUT_FORMAT_KEY_CLASS_ATTR, hadoopConfig.get(OUTPUT_FORMAT_KEY_CLASS_ATTR))
                .withLabel("OutputFormat Key Class"));
        builder.addIfNotNull(
            DisplayData.item(
                    OUTPUT_FORMAT_VALUE_CLASS_ATTR,
                    hadoopConfig.get(OUTPUT_FORMAT_VALUE_CLASS_ATTR))
                .withLabel("OutputFormat Value Class"));
        builder.addIfNotNull(
            DisplayData.item(PARTITIONER_CLASS_ATTR, hadoopConfig.get(PARTITIONER_CLASS_ATTR))
                .withLabel("Partitioner Class"));
        builder.addIfNotNull(
            DisplayData.item(NUM_REDUCES, hadoopConfig.get(NUM_REDUCES))
                .withLabel("Reducers Count"));
      }
    }

    private void setupJob(SerializableConfiguration configuration) {
      try {
        JobID jobId = HadoopUtils.createJobId();
        TaskAttemptContext setupTaskContext =
            HadoopUtils.createSetupTaskContext(getConfiguration().get(), jobId);
        OutputFormat<KeyT, ValueT> jobOutputFormat =
            HadoopUtils.createOutputFormatFromConfig(getConfiguration().get());
        jobOutputFormat.checkOutputSpecs(setupTaskContext);
        jobOutputFormat.getOutputCommitter(setupTaskContext).setupJob(setupTaskContext);

        HadoopUtils.setJobIdIntoConfig(jobId, configuration.get());
      } catch (Exception e) {
        throw new RuntimeException("Unable to setup job.", e);
      }
    }

    @Override
    public PDone expand(PCollection<KV<KeyT, ValueT>> input) {

      setupJob(getConfiguration());

      try {
        // TODO remove assignment during streaming rewrite
        PCollectionView<SerializableConfiguration> configView =
            createGlobalConfigCollectionView(input);
        if (input.getWindowingStrategy().equals(WindowingStrategy.globalDefault())) {
          configView = createGlobalConfigCollectionView(input);
        }

        return processJob(input, configView);

      } catch (CannotProvideCoderException e) {
        throw new IllegalStateException(e);
      }
    }

    private PDone processJob(
        PCollection<KV<KeyT, ValueT>> input, PCollectionView<SerializableConfiguration> configView) {

      VarIntCoder intCoder = VarIntCoder.of();
      IterableCoder<Integer> iterableIntCoder = IterableCoder.of(intCoder);
      TypeDescriptor<Iterable<Integer>> iterableIntType = TypeDescriptors.iterables(TypeDescriptors.integers());
      input.getPipeline().getCoderRegistry().registerCoderForType(iterableIntType, iterableIntCoder);

      VoidCoder voidCoder = VoidCoder.of();

      input
          .getPipeline()
          .getCoderRegistry()
          .registerCoderForType(TypeDescriptors.voids(), voidCoder);


      TypeDescriptor<KV<Void, Iterable<Integer>>> voidIterableIntKvType =
          TypeDescriptors.kvs(TypeDescriptors.voids(), iterableIntType);
      KvCoder<Void, Iterable<Integer>> voidIterableIntKvCoder =
          KvCoder.of(voidCoder, iterableIntCoder);
      input
          .getPipeline()
          .getCoderRegistry()
          .registerCoderForType(voidIterableIntKvType, voidIterableIntKvCoder);

      return PDone.in(
          input
              .apply(ParDo.of(new AssignTaskFn<>(configView))).setTypeDescriptor(TypeDescriptors.kvs(TypeDescriptors.integers(), input.getTypeDescriptor()))
              .apply(GroupByKey.create())
              .apply(ParDo.of(new WriteFn<>(configView))).setTypeDescriptor(TypeDescriptors.integers())
              .apply(Combine.globally(new CollectionCombiner<>())).setTypeDescriptor(iterableIntType)
              .apply(ParDo.of(new CommitJobFn<>(configView)))
              .getPipeline());
    }

    private PCollectionView<SerializableConfiguration> createGlobalConfigCollectionView(
        PCollection<KV<KeyT, ValueT>> input) throws CannotProvideCoderException {
      TypeDescriptor<SerializableConfiguration> configType =
          new TypeDescriptor<SerializableConfiguration>() {};

      return PCollectionViews.singletonView(
          input
              .getPipeline()
              .apply(Create.empty(TypeDescriptors.kvs(TypeDescriptors.voids(), configType))),
          input.getWindowingStrategy(),
          true,
          getConfiguration(),
          input.getPipeline().getCoderRegistry().getCoder(configType));
    }
  }

  private static class  CollectionCombiner<T>
      extends Combine.AccumulatingCombineFn<
          T, CollectionCombiner.CollectionAccumulator<T>, Iterable<T>> {

    private static class CollectionAccumulator<T>
        implements Combine.AccumulatingCombineFn.Accumulator<
            T, CollectionAccumulator<T>, Iterable<T>> {
      private Collection<T> collection = new ArrayList<>();

      @Override
      public void addInput(T input) {
        collection.add(input);
      }

      @Override
      public void mergeAccumulator(CollectionAccumulator<T> other) {
        collection.addAll(other.collection);
      }

      @Override
      public Iterable<T> extractOutput() {
        return collection;
      }
    }

    @Override
    public CollectionAccumulator<T> createAccumulator() {
      return new CollectionAccumulator<>();
    }
  };

  /**
   * @param <KeyT>
   * @param <ValueT>
   */
  private static class TaskIdContextHolder<KeyT, ValueT> {

    private SerializableConfiguration conf;
    private RecordWriter<KeyT, ValueT> recordWriter;
    private OutputCommitter outputCommitter;
    private OutputFormat<KeyT, ValueT> outputFormatObj;
    private TaskAttemptContext taskAttemptContext;

    TaskIdContextHolder(int taskId, SerializableConfiguration conf) {
      this.conf = conf;

      JobID jobID = HadoopUtils.getJobIdFromConfig(conf.get());
      taskAttemptContext = HadoopUtils.createTaskContext(conf.get(), jobID, taskId);
      outputFormatObj = HadoopUtils.createOutputFormatFromConfig(conf.get());
      outputCommitter =
          HadoopUtils.createOutputCommitter(outputFormatObj, conf.get(), taskAttemptContext);
      recordWriter = initRecordWriter(outputFormatObj, taskAttemptContext);
    }

    RecordWriter<KeyT, ValueT> getRecordWriter() {
      return recordWriter;
    }

    OutputCommitter getOutputCommitter() {
      return outputCommitter;
    }

    TaskAttemptContext getTaskAttemptContext() {
      return taskAttemptContext;
    }

    private RecordWriter<KeyT, ValueT> initRecordWriter(
        OutputFormat<KeyT, ValueT> outputFormatObj, TaskAttemptContext taskAttemptContext)
        throws RuntimeException {
      try {
        LOGGER.info("Creating new RecordWriter.");
        return outputFormatObj.getRecordWriter(taskAttemptContext);
      } catch (InterruptedException | IOException e) {
        throw new RuntimeException("Unable to create RecordWriter object: ", e);
      }
    }
  }

  private static class CommitJobFn<T> extends DoFn<Iterable<T>, Void> {

    PCollectionView<SerializableConfiguration> configView;

    public CommitJobFn(PCollectionView<SerializableConfiguration> configView) {
      this.configView = configView;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {

      SerializableConfiguration config = c.sideInput(configView);
      cleanupJob(config);
    }

    private void cleanupJob(SerializableConfiguration config) {
      JobID jobID = HadoopUtils.getJobIdFromConfig(config.get());
      TaskAttemptContext cleanupTaskContext =
          HadoopUtils.createCleanupTaskContext(config.get(), jobID);
      OutputFormat<?, ?> outputFormat = HadoopUtils.createOutputFormatFromConfig(config.get());
      try {
        OutputCommitter outputCommitter = outputFormat.getOutputCommitter(cleanupTaskContext);
        outputCommitter.abortJob(cleanupTaskContext, JobStatus.State.FAILED);
      } catch (Exception e) {
        throw new RuntimeException("Unable to commit job.", e);
      }
    }
  }

  private static class AssignTaskFn<KeyT, ValueT>
      extends DoFn<KV<KeyT, ValueT>, KV<Integer, KV<KeyT, ValueT>>> {

    private Map<Integer, TaskID> partitionToTaskContext = new HashMap<>();

    PCollectionView<SerializableConfiguration> configView;
    private Partitioner<KeyT, ValueT> partitioner;
    private Integer reducersCount;
    private JobID jobId;

    public AssignTaskFn(PCollectionView<SerializableConfiguration> configView) {
      this.configView = configView;
    }

    @ProcessElement
    public void processElement(
        @Element KV<KeyT, ValueT> element,
        OutputReceiver<KV<Integer, KV<KeyT, ValueT>>> receiver,
        ProcessContext c) {

      SerializableConfiguration config = c.sideInput(configView);

      TaskID taskID = createTaskIDForKV(element, config);
      int taskId = taskID.getId();
      receiver.output(KV.of(taskId, element));
    }

    private TaskID createTaskIDForKV(KV<KeyT, ValueT> kv, SerializableConfiguration config) {
      int taskContextKey =
          getPartitioner(config).getPartition(kv.getKey(), kv.getValue(), getReducersCount(config));

      return partitionToTaskContext.computeIfAbsent(
          taskContextKey, (key) -> HadoopUtils.createTaskID(getJobId(config), key));
    }

    private JobID getJobId(SerializableConfiguration config) {
      if (jobId == null) {
        jobId = HadoopUtils.getJobIdFromConfig(config.get());
      }
      return jobId;
    }

    private int getReducersCount(SerializableConfiguration config) {
      if (reducersCount == null) {
        reducersCount = HadoopUtils.getReducersCountFromConfig(config.get());
      }
      return reducersCount;
    }

    private Partitioner<KeyT, ValueT> getPartitioner(SerializableConfiguration config) {
      if (partitioner == null) {
        partitioner = HadoopUtils.getPartitionerFromConfig(config.get());
      }
      return partitioner;
    }
  }

  private static class WriteFn<KeyT, ValueT>
      extends DoFn<KV<Integer, Iterable<KV<KeyT, ValueT>>>, Integer> {

    private Map<Integer, TaskIdContextHolder> taskIdContextHolderMap;
    private PCollectionView<SerializableConfiguration> configView;

    public WriteFn(PCollectionView<SerializableConfiguration> configView) {
      this.configView = configView;
    }

    @Setup
    public void setup() {
      taskIdContextHolderMap = new HashMap<>();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {

      SerializableConfiguration conf = c.sideInput(configView);

      Integer taskID = c.element().getKey();

      TaskIdContextHolder taskContextHolder = getOrCreateTaskContextHolder(taskID, conf);

      try {

        setupWriteCommitTask(c, taskContextHolder);

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        String message =
            String.format(
                "Thread for task with id %s was interrupted",
                taskContextHolder.taskAttemptContext.getTaskAttemptID());
        logAndThrowException(e, message);
      } catch (IOException e) {
        String message =
            String.format(
                "Task with id %s failed.", taskContextHolder.taskAttemptContext.getTaskAttemptID());
        logAndThrowException(e, message);
        return;
      }

      c.output(taskID);
    }

    private void setupWriteCommitTask(ProcessContext c, TaskIdContextHolder taskContextHolder)
        throws IOException, InterruptedException {

      // setup task
      taskContextHolder.getOutputCommitter().setupTask(taskContextHolder.getTaskAttemptContext());

      // write and close
      RecordWriter recordWriter = taskContextHolder.getRecordWriter();
      for (KV<KeyT, ValueT> kv : c.element().getValue()) {
        recordWriter.write(kv.getKey(), kv.getValue());
      }
      recordWriter.close(taskContextHolder.getTaskAttemptContext());

      // commit task
      taskContextHolder.getOutputCommitter().commitTask(taskContextHolder.getTaskAttemptContext());
    }

    private TaskIdContextHolder getOrCreateTaskContextHolder(
        Integer taskId, SerializableConfiguration conf) {

      return taskIdContextHolderMap.computeIfAbsent(
          taskId, (id) -> new TaskIdContextHolder<KeyT, ValueT>(id, conf));
    }

    private void logAndThrowException(Throwable e, String message) {
      LOGGER.warn(message, e);
      throw new RuntimeException(message, e);
    }
  }
}
