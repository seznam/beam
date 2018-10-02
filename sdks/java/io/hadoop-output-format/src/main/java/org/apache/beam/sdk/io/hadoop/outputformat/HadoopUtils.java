package org.apache.beam.sdk.io.hadoop.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.util.UUID;

class HadoopUtils {

  private static final int DEFAULT_JOB_NUMBER = 0;
  static final Class<HashPartitioner> DEFAULT_PARTITIONER_CLASS_ATTR = HashPartitioner.class;
  static final int DEFAULT_NUM_REDUCERS = 1;

  static JobID createJobId() {
    return new JobID(UUID.randomUUID().toString(), DEFAULT_JOB_NUMBER);
  }

  static JobID createJobId(String id) {
    return new JobID(id, DEFAULT_JOB_NUMBER);
  }

  static TaskAttemptContext createSetupTaskContext(Configuration conf, JobID jobID) {
    final TaskID taskId = new TaskID(jobID, TaskType.JOB_SETUP, 0);
    return new TaskAttemptContextImpl(conf, new TaskAttemptID(taskId, 0));
  }

  static TaskAttemptContext createTaskContext(Configuration conf, JobID jobID, int taskNumber) {
    final TaskID taskId = createTaskID(jobID, taskNumber);
    return new TaskAttemptContextImpl(conf, new TaskAttemptID(taskId, 0));
  }

  static TaskID createTaskID(JobID jobID, int taskNumber) {
    return new TaskID(jobID, TaskType.REDUCE, taskNumber);
  }

  static TaskAttemptContext createCleanupTaskContext(Configuration conf, JobID jobID) {
    final TaskID taskId = new TaskID(jobID, TaskType.JOB_CLEANUP, 0);
    return new TaskAttemptContextImpl(conf, new TaskAttemptID(taskId, 0));
  }

  /**
   * Returns instance of {@link OutputFormat} by class name stored in the configuration under key
   * {@link MRJobConfig#OUTPUT_FORMAT_CLASS_ATTR}
   *
   * @param conf Hadoop configuration
   * @return OutputFormatter
   * @throws IllegalArgumentException if particular key was not found in the config or Formatter was
   *     unable to construct.
   */
  @SuppressWarnings("unchecked")
  static <KeyT, ValueT> OutputFormat<KeyT, ValueT> createOutputFormatFromConfig(Configuration conf)
      throws IllegalArgumentException {
    return (OutputFormat<KeyT, ValueT>)
        createInstanceFromConfig(conf, MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, null);
  }

  @SuppressWarnings("unchecked")
  static <KeyT, ValueT> Partitioner<KeyT, ValueT> getPartitioner(Configuration conf) {
    return (Partitioner<KeyT, ValueT>)
        createInstanceFromConfig(
            conf, MRJobConfig.PARTITIONER_CLASS_ATTR, DEFAULT_PARTITIONER_CLASS_ATTR);
  }

  private static Object createInstanceFromConfig(
      Configuration conf, String configClassKey, @Nullable Class<?> defaultClass) {
    try {
      String className = conf.get(configClassKey);
      if (className == null && defaultClass == null) {
        throw new IllegalArgumentException(
            String.format(
                "Configuration does not contains any value under %s key. Unable to initialize class instance from configuration. ",
                configClassKey));
      }

      Class<?> requiredClass =
          defaultClass == null
              ? conf.getClassByName(className)
              : conf.getClass(className, defaultClass);

      return requiredClass.getConstructor().newInstance();
    } catch (InstantiationException
        | IllegalAccessException
        | ClassNotFoundException
        | NoSuchMethodException
        | InvocationTargetException e) {
      throw new IllegalArgumentException(
          String.format(
              "Unable to create instance of object from configuration under key %s.",
              configClassKey),
          e);
    }
  }

  static JobID getJobId(Configuration conf) {
    return new JobID(conf.get(MRJobConfig.ID), DEFAULT_JOB_NUMBER);
  }

  static int getReducersCount(Configuration conf) {
    return conf.getInt(MRJobConfig.NUM_REDUCES, DEFAULT_NUM_REDUCERS);
  }
}
