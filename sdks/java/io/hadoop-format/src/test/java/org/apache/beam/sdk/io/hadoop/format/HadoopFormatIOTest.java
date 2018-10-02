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
package org.apache.beam.sdk.io.hadoop.format;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.hadoop.inputformat.Employee;
import org.apache.beam.sdk.io.hadoop.inputformat.TestEmployeeDataSet;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

/** Unit tests for {@link HadoopFormatIO}. */

// @Ignore
@RunWith(MockitoJUnitRunner.class)
public class HadoopFormatIOTest {

  private static final int REDUCERS_COUNT = 2;
  private static Configuration conf;

  @Rule public final transient TestPipeline p = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    conf = loadTestConfiguration(EmployeeOutputFormat.class, Text.class, Employee.class);
    OutputCommitter mockedOutputCommitter = Mockito.mock(OutputCommitter.class);
    EmployeeOutputFormat.initWrittenOutput(mockedOutputCommitter);
  }

  private static Configuration loadTestConfiguration(
      Class<?> outputFormatClassName, Class<?> keyClass, Class<?> valueClass) {
    Configuration conf = new Configuration();
    conf.setClass(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, outputFormatClassName, OutputFormat.class);
    conf.setClass(MRJobConfig.OUTPUT_KEY_CLASS, keyClass, Object.class);
    conf.setClass(MRJobConfig.OUTPUT_VALUE_CLASS, valueClass, Object.class);
    conf.setInt(MRJobConfig.NUM_REDUCES, REDUCERS_COUNT);
    return conf;
  }

  /**
   * This test validates {@link HadoopFormatIO.Write Write} transform object creation fails with
   * null configuration. {@link HadoopFormatIO.Write.Builder#withConfiguration(Configuration)
   * withConfiguration(Configuration)} method checks configuration is null and throws exception if
   * it is null.
   */
  @Test
  public void testWriteObjectCreationFailsIfConfigurationIsNull() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Configuration can not be null");
    HadoopFormatIO.<Text, Employee>write().withConfiguration(null);
  }

  /**
   * This test validates functionality of {@link
   * HadoopFormatIO.Write.Builder#withConfiguration(Configuration) withConfiguration(Configuration)}
   * function when Hadoop OutputFormat class is not provided by the user in configuration.
   */
  @Test
  public void testWriteValidationFailsMissingOutputFormatInConf() {
    Configuration configuration = new Configuration();
    configuration.setClass(HadoopFormatIO.OUTPUT_KEY_CLASS, Text.class, Object.class);
    configuration.setClass(HadoopFormatIO.OUTPUT_VALUE_CLASS, Employee.class, Object.class);

    HadoopFormatIO.Write<Text, Employee> writeWithWrongConfig =
        HadoopFormatIO.<Text, Employee>write().withConfiguration(configuration);

    p.apply(Create.of(TestEmployeeDataSet.getEmployeeData()))
        .setTypeDescriptor(
            TypeDescriptors.kvs(new TypeDescriptor<Text>() {}, new TypeDescriptor<Employee>() {}))
        .apply("Write", writeWithWrongConfig);

    thrown.expect(Pipeline.PipelineExecutionException.class);
    thrown.expectMessage("Configuration must contain \"mapreduce.job.outputformat.class\"");

    p.run().waitUntilFinish();
  }

  /**
   * This test validates functionality of {@link
   * HadoopFormatIO.Write.Builder#withConfiguration(Configuration) withConfiguration(Configuration)}
   * function when key class is not provided by the user in configuration.
   */
  @Test
  public void testWriteValidationFailsMissingKeyClassInConf() {
    Configuration configuration = new Configuration();
    configuration.setClass(
        HadoopFormatIO.OUTPUT_FORMAT_CLASS_ATTR, TextOutputFormat.class, OutputFormat.class);
    configuration.setClass(HadoopFormatIO.OUTPUT_VALUE_CLASS, Employee.class, Object.class);

    p.apply(Create.of(TestEmployeeDataSet.getEmployeeData()))
        .setTypeDescriptor(
            TypeDescriptors.kvs(new TypeDescriptor<Text>() {}, new TypeDescriptor<Employee>() {}))
        .apply("Write", HadoopFormatIO.<Text, Employee>write().withConfiguration(configuration));

    thrown.expect(Pipeline.PipelineExecutionException.class);
    thrown.expectMessage("Configuration must contain \"mapreduce.job.output.key.class\"");

    p.run().waitUntilFinish();
  }

  /**
   * This test validates functionality of {@link
   * HadoopFormatIO.Write.Builder#withConfiguration(Configuration) withConfiguration(Configuration)}
   * function when value class is not provided by the user in configuration.
   */
  @Test
  public void testWriteValidationFailsMissingValueClassInConf() {
    Configuration configuration = new Configuration();
    configuration.setClass(
        HadoopFormatIO.OUTPUT_FORMAT_CLASS_ATTR, TextOutputFormat.class, OutputFormat.class);
    configuration.setClass(HadoopFormatIO.OUTPUT_KEY_CLASS, Text.class, Object.class);

    p.apply(Create.of(TestEmployeeDataSet.getEmployeeData()))
        .setTypeDescriptor(
            TypeDescriptors.kvs(new TypeDescriptor<Text>() {}, new TypeDescriptor<Employee>() {}))
        .apply("Write", HadoopFormatIO.<Text, Employee>write().withConfiguration(configuration));

    thrown.expect(Pipeline.PipelineExecutionException.class);
    thrown.expectMessage("Configuration must contain \"mapreduce.job.output.value.class\"");

    p.run().waitUntilFinish();
  }

  @Test
  public void testWritingData() throws IOException {
    List<KV<Text, Employee>> data = TestEmployeeDataSet.getEmployeeData();
    PCollection<KV<Text, Employee>> input =
        p.apply(Create.of(data))
            .setTypeDescriptor(
                TypeDescriptors.kvs(
                    new TypeDescriptor<Text>() {}, new TypeDescriptor<Employee>() {}));

    input.apply("Write", HadoopFormatIO.<Text, Employee>write().withConfiguration(conf));
    p.run();

    List<KV<Text, Employee>> writtenOutput = EmployeeOutputFormat.getWrittenOutput();
    assertEquals(data.size(), writtenOutput.size());
    assertTrue(data.containsAll(writtenOutput));
    assertTrue(writtenOutput.containsAll(data));

    Mockito.verify(EmployeeOutputFormat.getOutputCommitter()).commitJob(Mockito.any());
    Mockito.verify(EmployeeOutputFormat.getOutputCommitter(), Mockito.times(REDUCERS_COUNT))
        .commitTask(Mockito.any());
  }

  @Test
  public void testWritingDataFailInvalidKeyType() {

    List<KV<String, Employee>> data = new ArrayList<>();
    data.add(KV.of("key", new Employee("name", "address")));
    PCollection<KV<String, Employee>> input =
        p.apply("CreateData", Create.of(data))
            .setTypeDescriptor(
                TypeDescriptors.kvs(
                    new TypeDescriptor<String>() {}, new TypeDescriptor<Employee>() {}));

    thrown.expect(Pipeline.PipelineExecutionException.class);
    thrown.expectMessage(String.class.getName());

    input.apply("Write", HadoopFormatIO.<String, Employee>write().withConfiguration(conf));
    p.run().waitUntilFinish();
  }

  @Test
  public void testWritingDataFailInvalidValueType() {

    List<KV<Text, Text>> data = new ArrayList<>();
    data.add(KV.of(new Text("key"), new Text("value")));
    TypeDescriptor<Text> textTypeDescriptor = new TypeDescriptor<Text>() {};
    PCollection<KV<Text, Text>> input =
        p.apply(Create.of(data))
            .setTypeDescriptor(TypeDescriptors.kvs(textTypeDescriptor, textTypeDescriptor));

    thrown.expect(Pipeline.PipelineExecutionException.class);
    thrown.expectMessage(Text.class.getName());

    input.apply("Write", HadoopFormatIO.<Text, Text>write().withConfiguration(conf));

    p.run().waitUntilFinish();
  }

  /**
   * This test validates functionality of {@link
   * HadoopFormatIO.Write#populateDisplayData(DisplayData.Builder)
   * populateDisplayData(DisplayData.Builder)}.
   */
  @Test
  public void testWriteDisplayData() {
    HadoopFormatIO.Write<String, String> write =
        HadoopFormatIO.<String, String>write().withConfiguration(conf);
    DisplayData displayData = DisplayData.from(write);

    assertThat(
        displayData,
        hasDisplayItem(
            HadoopFormatIO.OUTPUT_FORMAT_CLASS_ATTR,
            conf.get(HadoopFormatIO.OUTPUT_FORMAT_CLASS_ATTR)));
    assertThat(
        displayData,
        hasDisplayItem(HadoopFormatIO.OUTPUT_KEY_CLASS, conf.get(HadoopFormatIO.OUTPUT_KEY_CLASS)));
    assertThat(
        displayData,
        hasDisplayItem(
            HadoopFormatIO.OUTPUT_VALUE_CLASS, conf.get(HadoopFormatIO.OUTPUT_VALUE_CLASS)));
    assertThat(
        displayData,
        hasDisplayItem(
            HadoopFormatIO.PARTITIONER_CLASS_ATTR,
            HadoopUtils.DEFAULT_PARTITIONER_CLASS_ATTR.getName()));
  }
}
