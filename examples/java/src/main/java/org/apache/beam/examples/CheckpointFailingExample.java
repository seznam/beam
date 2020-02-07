/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.examples;

import org.apache.beam.examples.hadoop.HadoopSerializationCoder;
import org.apache.beam.examples.hadoop.SequenceFileSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.MatchConfiguration;
import org.apache.beam.sdk.io.FileIO.ReadMatches.DirectoryTreatment;
import org.apache.beam.sdk.io.ReadAllViaFileBasedSource;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.Watch.Growth;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 1. ./gradlew :examples:java:shadowJar
 *
 * 2. copy 200MB inputFile  https://drive.google.com/open?id=1NSUK18LfLPLklLGK1XiErg7qeb159GWq
 * and fat jar in build/libs to hdfs or to location where you gonna start cluster
 *
 * 3. edit in runningScript inputFilePath
 * <a href="file:../resources/runningScript.sh">runningScript</a>
 *
 * 4. run script to start the job
 */
public class CheckpointFailingExample {

  private static final Logger LOG = LoggerFactory.getLogger(CheckpointFailingExample.class);

  public static void main(String[] args) {

    CheckpointFailingExampleOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation()
            .as(CheckpointFailingExampleOptions.class);

    MatchConfiguration matchConfiguration =
        MatchConfiguration.create(EmptyMatchTreatment.ALLOW)
            .continuously(Duration.standardSeconds(60), Growth.never());

    long desiredBundleSizeBytes = 1024 * 1024 * 2;
    StaticValueProvider<String> inputPattern = StaticValueProvider.of(options.getInputFilePath());
    Pipeline pipeline = Pipeline.create(options);
    PCollection<Metadata> metadataPColl =
        pipeline
            .apply("Create-filepattern-", Create.ofProvider(inputPattern, StringUtf8Coder.of()))
            .apply("Match-All-", FileIO.matchAll().withConfiguration(matchConfiguration));

    metadataPColl
        .apply(
            "Read-Matches-",
            FileIO.readMatches().withDirectoryTreatment(DirectoryTreatment.PROHIBIT))
        .apply(
            "Via-ReadFiles-",
            new ReadAllViaFileBasedSource<>(
                desiredBundleSizeBytes,
                createSequenceFileSourceFn(desiredBundleSizeBytes),
                KvCoder.of(
                    new HadoopSerializationCoder<>(NullWritable.class, WritableSerialization.class),
                    new HadoopSerializationCoder<>(
                        BytesWritable.class, WritableSerialization.class))))
        .apply("Get-Value-", Values.create())
        .apply(ParDo.of(new DoFn<BytesWritable, BytesWritable>() {
          int count;
          @ProcessElement
          public void process(ProcessContext ctxt) {
            count++;
            if (count % 10_000 == 0) {
              LOG.info("Read {} elements", count);
            }
            long i= 0;
            while (i < 100_000L){
              i++; // simulate some parsing and processing
            }
            ctxt.output(ctxt.element());
          }
        }));

    pipeline.run().waitUntilFinish();
  }

  /** Create SequenceFileSource for each split. */
  private static SimpleFunction<String, FileBasedSource<KV<NullWritable, BytesWritable>>>
      createSequenceFileSourceFn(final long desiredBundleSizeBytes) {

    return new SimpleFunction<String, FileBasedSource<KV<NullWritable, BytesWritable>>>(
        (input) ->
            new SequenceFileSource<>(
                StaticValueProvider.of(input),
                NullWritable.class,
                WritableSerialization.class,
                BytesWritable.class,
                WritableSerialization.class,
                desiredBundleSizeBytes)) {};
  }

  public interface CheckpointFailingExampleOptions extends PipelineOptions {


    @Description("Path pattern of the file to read from")
    @Default.String("hdfs:///tmp/folder/*")
    String getInputFilePath();

    void setInputFilePath(String value);
  }
}
