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
package org.apache.beam.sdk.extensions.euphoria.testing;

import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.values.PValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple {@link Pipeline.PipelineVisitor}. It does structural dump of traversed pipeline to logs.
 */
public class LogDumpPipelineVisitor implements Pipeline.PipelineVisitor {
  private static final Logger LOG = LoggerFactory.getLogger(LogDumpPipelineVisitor.class);

  private static final char NESTING_CHAR = '-';

  @VisibleForTesting StringBuilder nesting = new StringBuilder();

  @VisibleForTesting StringBuilder outBuff = new StringBuilder();

  @Override
  public void enterPipeline(Pipeline p) {
    nesting.setLength(0);
    outBuff.setLength(0);
    outBuff.append(String.format("pipeline: '%s'%n", p.toString()));
  }

  @Override
  public CompositeBehavior enterCompositeTransform(Node node) {
    nesting.append(NESTING_CHAR);

    outBuff.append(nesting).append(String.format("composite T: '%s'%n", node.getFullName()));

    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(Node node) {
    nesting.setLength(nesting.length() - 1);
  }

  @Override
  public void visitPrimitiveTransform(Node node) {
    outBuff.append(nesting).append(String.format("T: '%s'%n", node.getFullName()));
  }

  @Override
  public void visitValue(PValue value, Node producer) {
    outBuff
        .append(nesting)
        .append(
            String.format(
                "value class: '%s', producer: '%s'%n",
                value.getClass().getSimpleName(), producer.getFullName()));
  }

  @Override
  public void leavePipeline(Pipeline pipeline) {
    LOG.warn("Traversed pipeline structure: \n" + outBuff.toString());
  }
}
