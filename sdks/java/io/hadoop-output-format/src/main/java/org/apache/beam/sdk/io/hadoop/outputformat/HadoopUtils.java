package org.apache.beam.sdk.io.hadoop.outputformat;

import java.lang.reflect.InvocationTargetException;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

public class HadoopUtils {

  private static final int DEFAULT_JOB_NUMBER = 0;
  public static final Class<HashPartitioner> DEFAULT_PARTITIONER_CLASS_ATTR = HashPartitioner.class;
  public static final int DEFAULT_NUM_REDUCERS = 1;

  public static JobID createJobId() {
    return new JobID(UUID.randomUUID().toString(), DEFAULT_JOB_NUMBER);
  }

  public static TaskAttemptContext createSetupTaskContext(Configuration conf, JobID jobID) {
    final TaskID taskId = new TaskID(jobID, TaskType.JOB_SETUP, 0);
    return new TaskAttemptContextImpl(conf, new TaskAttemptID(taskId, 0));
  }

  public static TaskAttemptContext createTaskContext(
      Configuration conf, JobID jobID, int taskNumber) {
    final TaskID taskId = createTaskID(jobID, taskNumber);
    return new TaskAttemptContextImpl(conf, new TaskAttemptID(taskId, 0));
  }

  public static TaskID createTaskID(JobID jobID, int taskNumber){
    return new TaskID(jobID, TaskType.REDUCE, taskNumber);
  }

  public static TaskAttemptContext createTaskContext(Configuration conf, TaskID taskId) {
    return new TaskAttemptContextImpl(conf, new TaskAttemptID(taskId, 0));
  }

  public static TaskAttemptContext createCleanupTaskContext(Configuration conf, JobID jobID) {
    final TaskID taskId = new TaskID(jobID, TaskType.JOB_CLEANUP, 0);
    return new TaskAttemptContextImpl(conf, new TaskAttemptID(taskId, 0));
  }

  /**
   * Returns instance of {@link OutputFormat} by class name stored in the configuration under key
   * {@link MRJobConfig#OUTPUT_FORMAT_CLASS_ATTR}
   *
   * @param conf Hadoop configuration
   * @param <KeyT> Key type of configuration
   * @param <ValueT> Value type of configuration
   * @return OutputFormatter
   * @throws IllegalArgumentException if particular key was not found in the config or Formatter was
   *     unable to construct.
   */
  public static <KeyT, ValueT> OutputFormat<KeyT, ValueT> createOutputFormatFromConfig(
      Configuration conf) throws IllegalArgumentException {
    try {
      String outputFormatClassName = conf.get(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR);
      if (outputFormatClassName == null) {
        throw new IllegalArgumentException(
            String.format(
                "Unable to create %s from configuration. Configuration does not contains name of %s class under %s key.",
                OutputFormat.class.getSimpleName(),
                OutputFormat.class.getSimpleName(),
                MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR));
      }
      return (OutputFormat<KeyT, ValueT>)
          conf.getClassByName(outputFormatClassName).getConstructor().newInstance();
    } catch (InstantiationException
        | IllegalAccessException
        | ClassNotFoundException
        | NoSuchMethodException
        | InvocationTargetException e) {
      throw new IllegalArgumentException("Unable to create OutputFormat object: ", e);
    }
  }

  public static OutputCommitter createOutputCommitter(
      OutputFormat<?, ?> outputFormatObj, Configuration conf, TaskAttemptContext taskAttemptContext)
      throws RuntimeException {
    OutputCommitter outputCommitter;
    try {
      outputCommitter = outputFormatObj.getOutputCommitter(taskAttemptContext);
      if (outputCommitter != null) {
        outputCommitter.setupJob(new JobContextImpl(conf, taskAttemptContext.getJobID()));
      }
    } catch (Exception e) {
      //FIXME provide more sensible exception
      throw new RuntimeException("Unable to create OutputCommitter object: ", e);
    }

    return outputCommitter;
  }

  public static JobID getJobIdFromConfig(Configuration conf) {
    return new JobID(conf.get(MRJobConfig.ID), DEFAULT_JOB_NUMBER);
  }

  public static <KeyT, ValueT> Partitioner<KeyT, ValueT> getPartitionerFromConfig(
      Configuration conf) {
    try {
      return (Partitioner<KeyT, ValueT>)
          conf.getClass(MRJobConfig.PARTITIONER_CLASS_ATTR, DEFAULT_PARTITIONER_CLASS_ATTR)
              .getConstructor()
              .newInstance();
    } catch (NoSuchMethodException
        | IllegalAccessException
        | InvocationTargetException
        | InstantiationException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static int getReducersCountFromConfig(Configuration conf) {
    return conf.getInt(MRJobConfig.NUM_REDUCES, DEFAULT_NUM_REDUCERS);
  }

  public static void setJobIdIntoConfig(JobID jobId, Configuration conf) {
    conf.set(MRJobConfig.ID, jobId.getJtIdentifier());
  }
}
