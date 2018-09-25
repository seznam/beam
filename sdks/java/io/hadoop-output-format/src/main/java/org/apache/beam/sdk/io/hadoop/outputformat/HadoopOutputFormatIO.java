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

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

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
  public static final String OUTPUT_FORMAT_KEY_CLASS_ATTR = MRJobConfig.OUTPUT_KEY_CLASS;
  public static final String OUTPUT_FORMAT_VALUE_CLASS_ATTR = MRJobConfig.OUTPUT_VALUE_CLASS;
  public static final String NUM_REDUCES = MRJobConfig.NUM_REDUCES;
  public static final String PARTITIONER_CLASS_ATTR = MRJobConfig.PARTITIONER_CLASS_ATTR;

  private static final String TRANSFORM_NAME = "HadoopOutputIO";

  /**
   * Creates an uninitialized {@link HadoopOutputFormatIO.Write}. Before use, the {@code Write} must
   * be initialized with a HadoopOutputFormatIO.Write#withConfiguration(HadoopConfiguration) that
   * specifies the sink.
   */
  public static <KeyT, ValueT> Write.Builder<KeyT, ValueT> write() {
    return new AutoValue_HadoopOutputFormatIO_Write.Builder<>();
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

    public abstract Configuration getConfiguration();

    public abstract TypeDescriptor<?> getOutputFormatClass();

    public abstract TypeDescriptor<KeyT> getOutputFormatKeyClass();

    public abstract TypeDescriptor<ValueT> getOutputFormatValueClass();

    public abstract Integer getReducersCount();

    public abstract Partitioner<KeyT, ValueT> getPartitioner();

    @AutoValue.Builder
    abstract static class Builder<KeyT, ValueT> {
      abstract Builder<KeyT, ValueT> setConfiguration(Configuration configuration);

      abstract Builder<KeyT, ValueT> setOutputFormatClass(TypeDescriptor<?> outputFormatClass);

      abstract Builder<KeyT, ValueT> setOutputFormatKeyClass(
          TypeDescriptor<KeyT> outputFormatKeyClass);

      abstract Builder<KeyT, ValueT> setOutputFormatValueClass(
          TypeDescriptor<ValueT> outputFormatValueClass);

      abstract Builder<KeyT, ValueT> setReducersCount(Integer partitionsCount);

      abstract Builder<KeyT, ValueT> setPartitioner(Partitioner<KeyT, ValueT> partitioner);

      abstract Write<KeyT, ValueT> build();

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

        return setConfiguration(new Configuration(configuration))
            .setOutputFormatClass(outputFormatClass)
            .setOutputFormatKeyClass(outputFormatKeyClass)
            .setOutputFormatValueClass(outputFormatValueClass)
            .setReducersCount(reducersCount)
            .setPartitioner(partitioner)
            .build();
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
    }

    @Override
    public void validate(PipelineOptions pipelineOptions) {}

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      Configuration hadoopConfig = getConfiguration();
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


    @Override
    public PDone expand(PCollection<KV<KeyT, ValueT>> input) {

      // TODO add branch for streaming
      PCollectionView<Configuration> configView = null;
      if (input.getWindowingStrategy().equals(WindowingStrategy.globalDefault())) {
        configView = createGlobalConfigCollectionView(input);
      }

      return processJob(input, configView);
    }

    private PDone processJob(
        PCollection<KV<KeyT, ValueT>> input,
        PCollectionView<Configuration> configView) {

      TypeDescriptor<Iterable<Integer>> iterableIntType =
          TypeDescriptors.iterables(TypeDescriptors.integers());

      validateInput(input);

      return PDone.in(
          input
              .apply(
                  TRANSFORM_NAME + "/TaskAssignee",
                  ParDo.of(new AssignTaskFn<KeyT, ValueT>(configView)).withSideInputs(configView))
              .setTypeDescriptor(
                  TypeDescriptors.kvs(TypeDescriptors.integers(), input.getTypeDescriptor()))
              .apply(TRANSFORM_NAME + "/GroupByTaskId", GroupByKey.create())
              .apply(
                  TRANSFORM_NAME + "/Write",
                  ParDo.of(new WriteFn<KeyT, ValueT>(configView)).withSideInputs(configView))
              .setTypeDescriptor(TypeDescriptors.integers())
              .apply(
                  TRANSFORM_NAME + "/CollectWriteTasks",
                  Combine.globally(new IterableCombinerFn<>(TypeDescriptors.integers())))
              .setTypeDescriptor(iterableIntType)
              .apply(
                  TRANSFORM_NAME + "/CommitWriteJob",
                  ParDo.of(new CommitJobFn<Integer>(configView)).withSideInputs(configView))
              .getPipeline());
    }

    private void validateInput(PCollection<KV<KeyT, ValueT>> input) {
      TypeDescriptor<KV<KeyT, ValueT>> inputTypeDescriptor = input.getTypeDescriptor();

      checkArgument(
          inputTypeDescriptor != null,
          "Input %s must be set!",
          TypeDescriptor.class.getSimpleName());
      checkArgument(
          KV.class.equals(inputTypeDescriptor.getRawType()),
          "%s expects %s as input type.",
          Write.class.getSimpleName(),
          KV.class);
      checkArgument(
          inputTypeDescriptor.equals(
              TypeDescriptors.kvs(getOutputFormatKeyClass(), getOutputFormatValueClass())),
          "%s expects following %ss: KV(Key: %s, Value: %s) but following %ss are set: KV(Key: %s, Value: %s)",
          Write.class.getSimpleName(),
          TypeDescriptor.class.getSimpleName(),
          getOutputFormatKeyClass().getRawType(),
          getOutputFormatValueClass().getRawType(),
          TypeDescriptor.class.getSimpleName(),
          inputTypeDescriptor.resolveType(KV.class.getTypeParameters()[0]),
          inputTypeDescriptor.resolveType(KV.class.getTypeParameters()[1]));
    }

    private PCollectionView<Configuration> createGlobalConfigCollectionView(
        PCollection<KV<KeyT, ValueT>> input) {

      TypeDescriptor<Configuration> confTypeDescriptor = new TypeDescriptor<Configuration>() {};

      input.getPipeline().getCoderRegistry().registerCoderForType(confTypeDescriptor, new ConfigurationCoder());



      return input
          .getPipeline()
          .apply(TRANSFORM_NAME + "/createConfig", Create.<Configuration>of(getConfiguration())).setTypeDescriptor(confTypeDescriptor)
          .apply(TRANSFORM_NAME + "/setupJob", ParDo.of(new SetupJobFn())).setTypeDescriptor(confTypeDescriptor)
          .apply(View.asSingleton());
    }
  }

  /**
   * @param <KeyT>
   * @param <ValueT>
   */
  private static class TaskIdContextHolder<KeyT, ValueT> {

    private RecordWriter<KeyT, ValueT> recordWriter;
    private OutputCommitter outputCommitter;
    private OutputFormat<KeyT, ValueT> outputFormatObj;
    private TaskAttemptContext taskAttemptContext;

    TaskIdContextHolder(int taskId, Configuration conf) {

      JobID jobID = HadoopUtils.getJobIdFromConfig(conf);
      taskAttemptContext = HadoopUtils.createTaskContext(conf, jobID, taskId);
      outputFormatObj = HadoopUtils.createOutputFormatFromConfig(conf);
      outputCommitter = initOutputCommitter(outputFormatObj, conf, taskAttemptContext);
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

    private static OutputCommitter initOutputCommitter(
        OutputFormat<?, ?> outputFormatObj,
        Configuration conf,
        TaskAttemptContext taskAttemptContext)
        throws RuntimeException {
      OutputCommitter outputCommitter;
      try {
        outputCommitter = outputFormatObj.getOutputCommitter(taskAttemptContext);
        if (outputCommitter != null) {
          outputCommitter.setupJob(new JobContextImpl(conf, taskAttemptContext.getJobID()));
        }
      } catch (Exception e) {
        throw new IllegalStateException("Unable to create OutputCommitter object: ", e);
      }

      return outputCommitter;
    }
  }

  private static class ConfigurationCoder extends AtomicCoder<Configuration> {

    @Override
    public void encode(Configuration value, OutputStream outStream) throws IOException {
      DataOutputStream dataOutputStream = new DataOutputStream(outStream);
      value.write(dataOutputStream);
      dataOutputStream.flush();
    }

    @Override
    public Configuration decode(InputStream inStream) throws IOException {
      DataInputStream dataInputStream = new DataInputStream(inStream);
      Configuration config = new Configuration();
      config.readFields(dataInputStream);

      return config;
    }
  }

  private static class SetupJobFn extends DoFn<Configuration, Configuration> {

    @DoFn.ProcessElement
    public void processElement(@DoFn.Element Configuration serializableConfig,
        OutputReceiver<Configuration> receiver){

      Configuration hadoopConf = new Configuration();

//      serializableConfig.forEach(p -> hadoopConf.set(p.getKey(), new String(p.getValue())));


      setupJob(hadoopConf);
      receiver.output(new Configuration(hadoopConf));



    }

    private void setupJob(Configuration conf) {
      try {

        JobID jobId = HadoopUtils.createJobId();
        TaskAttemptContext setupTaskContext =
            HadoopUtils.createSetupTaskContext(conf, jobId);
        OutputFormat<?, ?> jobOutputFormat =
            HadoopUtils.createOutputFormatFromConfig(conf);
        jobOutputFormat.checkOutputSpecs(setupTaskContext);
        jobOutputFormat.getOutputCommitter(setupTaskContext).setupJob(setupTaskContext);

        conf.set(MRJobConfig.ID, jobId.getJtIdentifier());
      } catch (Exception e) {
        throw new RuntimeException("Unable to setup job.", e);
      }
    }

  }


  private static class CommitJobFn<T> extends DoFn<Iterable<T>, Void> {

    PCollectionView<Configuration> configView;

    CommitJobFn(PCollectionView<Configuration> configView) {
      this.configView = configView;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {

      Configuration config = c.sideInput(configView);
      cleanupJob(config);
    }

    private void cleanupJob(Configuration config) {
      JobID jobID = HadoopUtils.getJobIdFromConfig(config);
      TaskAttemptContext cleanupTaskContext =
          HadoopUtils.createCleanupTaskContext(config, jobID);
      OutputFormat<?, ?> outputFormat = HadoopUtils.createOutputFormatFromConfig(config);
      try {
        OutputCommitter outputCommitter = outputFormat.getOutputCommitter(cleanupTaskContext);
        outputCommitter.commitJob(cleanupTaskContext);
      } catch (Exception e) {
        throw new RuntimeException("Unable to commit job.", e);
      }
    }
  }

  private static class AssignTaskFn<KeyT, ValueT>
      extends DoFn<KV<KeyT, ValueT>, KV<Integer, KV<KeyT, ValueT>>> {

    private Map<Integer, TaskID> partitionToTaskContext = new HashMap<>();

    PCollectionView<Configuration> configView;
    private Partitioner<KeyT, ValueT> partitioner;
    private Integer reducersCount;
    private JobID jobId;

    AssignTaskFn(PCollectionView<Configuration> configView) {
      this.configView = configView;
    }

    @ProcessElement
    public void processElement(
        @Element KV<KeyT, ValueT> element,
        OutputReceiver<KV<Integer, KV<KeyT, ValueT>>> receiver,
        ProcessContext c) {

      Configuration config = c.sideInput(configView);

      TaskID taskID = createTaskIDForKV(element, config);
      int taskId = taskID.getId();
      receiver.output(KV.of(taskId, element));
    }

    private TaskID createTaskIDForKV(KV<KeyT, ValueT> kv, Configuration config) {
      int taskContextKey =
          getPartitioner(config).getPartition(kv.getKey(), kv.getValue(), getReducersCount(config));

      return partitionToTaskContext.computeIfAbsent(
          taskContextKey, (key) -> HadoopUtils.createTaskID(getJobId(config), key));
    }

    private JobID getJobId(Configuration config) {
      if (jobId == null) {
        jobId = HadoopUtils.getJobIdFromConfig(config);
      }
      return jobId;
    }

    private int getReducersCount(Configuration config) {
      if (reducersCount == null) {
        reducersCount = HadoopUtils.getReducersCountFromConfig(config);
      }
      return reducersCount;
    }

    private Partitioner<KeyT, ValueT> getPartitioner(Configuration config) {
      if (partitioner == null) {
        partitioner = HadoopUtils.getPartitionerFromConfig(config);
      }
      return partitioner;
    }
  }

  private static class WriteFn<KeyT, ValueT>
      extends DoFn<KV<Integer, Iterable<KV<KeyT, ValueT>>>, Integer> {

    private Map<Integer, TaskIdContextHolder<KeyT, ValueT>> taskIdContextHolderMap;
    private PCollectionView<Configuration> configView;

    WriteFn(PCollectionView<Configuration> configView) {
      this.configView = configView;
    }

    @Setup
    public void setup() {
      taskIdContextHolderMap = new HashMap<>();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {

      Configuration conf = c.sideInput(configView);

      Integer taskID = c.element().getKey();

      TaskIdContextHolder<KeyT, ValueT> taskContextHolder = getOrCreateTaskContextHolder(taskID, conf);

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

    private void setupWriteCommitTask(ProcessContext c, TaskIdContextHolder<KeyT, ValueT> taskContextHolder)
        throws IOException, InterruptedException {

      // setup task
      taskContextHolder.getOutputCommitter().setupTask(taskContextHolder.getTaskAttemptContext());

      // write and close
      RecordWriter<KeyT, ValueT> recordWriter = taskContextHolder.getRecordWriter();
      for (KV<KeyT, ValueT> kv : Objects.requireNonNull(c.element().getValue())) {
        recordWriter.write(kv.getKey(), kv.getValue());
      }
      recordWriter.close(taskContextHolder.getTaskAttemptContext());

      // commit task
      taskContextHolder.getOutputCommitter().commitTask(taskContextHolder.getTaskAttemptContext());
    }

    private TaskIdContextHolder<KeyT, ValueT> getOrCreateTaskContextHolder(
        Integer taskId, Configuration conf) {

      return taskIdContextHolderMap.computeIfAbsent(
          taskId, (id) -> new TaskIdContextHolder<>(id, conf));
    }

    private void logAndThrowException(Throwable e, String message) {
      LOGGER.warn(message, e);
      throw new RuntimeException(message, e);
    }
  }
}
