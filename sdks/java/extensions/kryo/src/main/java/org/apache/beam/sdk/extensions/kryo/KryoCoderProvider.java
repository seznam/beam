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

import static java.util.Objects.requireNonNull;

import com.esotericsoftware.kryo.ClassResolver;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Implementation of {@link CoderProvider}, which provides {@link KryoCoder} for any type registered
 * to {@link Kryo} by user-provided {@link KryoRegistrar}.
 */
public class KryoCoderProvider extends CoderProvider {

  private final IdentifiedRegistrar kryoRegistrar;

  /**
   * Starts build of {@link KryoCoderProvider} with given {@link KryoRegistrar}.
   *
   * @param registrar user defined implementation of {@link KryoRegistrar}
   * @return next step of building
   */
  public static FinalBuilder of(KryoRegistrar registrar) {
    return new Builder(registrar);
  }

  private KryoCoderProvider(IdentifiedRegistrar kryoRegistrar) {
    requireNonNull(kryoRegistrar);
    this.kryoRegistrar = kryoRegistrar;
  }

  @Override
  public <T> Coder<T> coderFor(
      TypeDescriptor<T> typeDescriptor, List<? extends Coder<?>> componentCoders)
      throws CannotProvideCoderException {

    return createKryoCoderIfRawTypeRegistered(typeDescriptor);
  }

  private <T> KryoCoder<T> createKryoCoderIfRawTypeRegistered(TypeDescriptor<T> typeDescriptor)
      throws CannotProvideCoderException {

    Class<? super T> rawType = typeDescriptor.getRawType();
    Kryo kryo = KryoFactory.getOrCreateKryo(kryoRegistrar);
    ClassResolver classResolver = kryo.getClassResolver();

    Registration registration = classResolver.getRegistration(rawType);
    if (registration == null) {
      throw new CannotProvideCoderException(
          String.format(
              "Cannot provide %s, given type descriptor's '%s' raw type is not registered in Kryo.",
              KryoCoder.class.getSimpleName(), typeDescriptor));
    }

    return KryoCoder.of(kryoRegistrar);
  }

  @VisibleForTesting
  IdentifiedRegistrar getKryoRegistrar() {
    return kryoRegistrar;
  }

  // ----------------- Builder steps

  /** Last step when building {@link KryoCoderProvider}. */
  public interface FinalBuilder {

    /**
     * Builds {@link KryoCoderProvider}.
     *
     * @return the build {@link KryoCoderProvider} instance.
     */
    KryoCoderProvider build();

    /**
     * Builds {@link KryoCoderProvider} and register it to given {@link Pipeline}.
     *
     * @param pipeline Pipeline whose coder registry will be used to register {@link
     *     KryoCoderProvider} under build.
     */
    void buildAndRegister(Pipeline pipeline);
  }

  // ----------------- Builder

  /** A builders chain implementation. Starts with {@link KryoCoderProvider#of(KryoRegistrar)}. */
  public static class Builder implements FinalBuilder {
    private final KryoRegistrar registrar;

    public Builder(KryoRegistrar registrar) {
      this.registrar = requireNonNull(registrar);
    }

    @Override
    public KryoCoderProvider build() {
      IdentifiedRegistrar registrarWithId = IdentifiedRegistrar.of(registrar);
      return new KryoCoderProvider(registrarWithId);
    }

    @Override
    public void buildAndRegister(Pipeline pipeline) {
      KryoCoderProvider provider = build();
      pipeline.getCoderRegistry().registerCoderProvider(provider);
    }
  }
}
