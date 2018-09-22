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
package org.apache.beam.sdk.extensions.kryo;

import com.esotericsoftware.kryo.ClassResolver;
import com.esotericsoftware.kryo.Kryo;
import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Implementation of {@link CoderProvider}, which provides {@link KryoCoder} for any type registered
 * to {@link Kryo} by user-provided {@link KryoRegistrar}.
 */
public class KryoCoderProvider extends CoderProvider {

  /**
   * Create a new {@link KryoCoderProvider}.
   *
   * @param pipelineOptions Options used for coder setup. See {@link KryoOptions} for more details.
   * @return A newly created {@link KryoCoderProvider}
   */
  public static KryoCoderProvider of(PipelineOptions pipelineOptions) {
    return of(pipelineOptions, Collections.emptyList());
  }

  /**
   * Create a new {@link KryoCoderProvider}.
   *
   * @param pipelineOptions Options used for coder setup. See {@link KryoOptions} for more details.
   * @param registrars {@link KryoRegistrar}s which are used to register classes with underlying
   *     kryo instance
   * @return A newly created {@link KryoCoderProvider}
   */
  public static KryoCoderProvider of(PipelineOptions pipelineOptions, KryoRegistrar... registrars) {
    return of(pipelineOptions, Arrays.asList(registrars));
  }

  /**
   * Create a new {@link KryoCoderProvider}.
   *
   * @param pipelineOptions Options used for coder setup. See {@link KryoOptions} for more details.
   * @param registrars {@link KryoRegistrar}s which are used to register classes with underlying
   *     kryo instance
   * @return A newly created {@link KryoCoderProvider}
   */
  public static KryoCoderProvider of(
      PipelineOptions pipelineOptions, List<KryoRegistrar> registrars) {
    final KryoOptions kryoOptions = pipelineOptions.as(KryoOptions.class);
    return new KryoCoderProvider(KryoCoder.of(kryoOptions, registrars));
  }

  /** {@link KryoRegistrar}s associated with this provider instance. */
  private final KryoCoder<?> coder;

  private KryoCoderProvider(KryoCoder<?> coder) {
    this.coder = coder;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Coder<T> coderFor(
      TypeDescriptor<T> typeDescriptor, List<? extends Coder<?>> componentCoders)
      throws CannotProvideCoderException {
    if (hasRegistration(typeDescriptor)) {
      return (Coder) coder;
    }
    throw new CannotProvideCoderException(
        String.format(
            "Cannot provide [%s], given type descriptor's [%s] raw type is not registered in Kryo.",
            KryoCoder.class.getSimpleName(), typeDescriptor));
  }

  private <T> boolean hasRegistration(TypeDescriptor<T> typeDescriptor) {
    final KryoState kryoState = KryoState.get(coder);
    final Class<? super T> rawType = typeDescriptor.getRawType();
    final Kryo kryo = kryoState.getKryo();
    final ClassResolver classResolver = kryo.getClassResolver();
    return classResolver.getRegistration(rawType) != null;
  }

  /**
   * Create a new {@link KryoCoderProvider} with the provided registrar.
   *
   * @param registrar registrar to append to the list of already registered registrars.
   * @return a new {@link KryoCoderProvider}
   */
  public KryoCoderProvider withRegistrar(KryoRegistrar registrar) {
    return new KryoCoderProvider(coder.withRegistrar(registrar));
  }

  /**
   * Builds {@link KryoCoderProvider} and register it to given {@link Pipeline}.
   *
   * @param pipeline Pipeline whose coder registry will be used to register {@link
   *     KryoCoderProvider} under build.
   */
  public void registerTo(Pipeline pipeline) {
    pipeline.getCoderRegistry().registerCoderProvider(this);
  }

  @VisibleForTesting
  KryoCoder<?> getCoder() {
    return coder;
  }
}
