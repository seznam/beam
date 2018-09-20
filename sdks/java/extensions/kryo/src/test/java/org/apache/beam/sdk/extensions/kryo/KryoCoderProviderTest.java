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

import java.util.Collections;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

/** A collections of {@link KryoCoderProvider} unit tests. */
public class KryoCoderProviderTest {

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testBuilding() {
    KryoCoderProvider provider = KryoCoderProvider.of((kryo) -> {}).build();
    Assert.assertNotNull(provider);
  }

  @Test
  public void testBuildingAndRegister() {
    KryoCoderProvider.of((kryo) -> {}).buildAndRegister(pipeline);
  }

  @Test
  public void testItProvidesCodersToRegisteredClasses() throws CannotProvideCoderException {
    KryoCoderProvider provider =
        KryoCoderProvider.of(
                (kryo) -> {
                  kryo.register(FirstTestClass.class);
                  kryo.register(SecondTestClass.class);
                  kryo.register(ThirdTestClass.class);
                })
            .build();

    assertProviderReturnsKryoCoderForClass(provider, FirstTestClass.class);
    assertProviderReturnsKryoCoderForClass(provider, SecondTestClass.class);
    assertProviderReturnsKryoCoderForClass(provider, ThirdTestClass.class);
  }

  @Test(expected = CannotProvideCoderException.class)
  public void testDoNotProvideCOderForUnregisteredClasses() throws CannotProvideCoderException {
    KryoCoderProvider provider =
        KryoCoderProvider.of(
                (kryo) -> {
                  kryo.register(FirstTestClass.class);
                  kryo.register(SecondTestClass.class);
                  kryo.register(ThirdTestClass.class);
                })
            .build();

    provider.coderFor(TypeDescriptor.of(NeverRegisteredClass.class), Collections.emptyList());
  }

  @Test
  public void testProviderRegisteredToPipeline() throws CannotProvideCoderException {
    KryoCoderProvider.of(
            (kryo) -> {
              kryo.register(FirstTestClass.class);
            })
        .buildAndRegister(pipeline);

    Coder<FirstTestClass> coderToAssert =
        pipeline.getCoderRegistry().getCoder(FirstTestClass.class);

    Assert.assertNotNull(coderToAssert);
    Assert.assertTrue(coderToAssert instanceof KryoCoder);
    KryoCoder<FirstTestClass> casted = (KryoCoder<FirstTestClass>) coderToAssert;
    IdentifiedRegistrar coderRegistrar = casted.getRegistrar();
    Assert.assertNotNull(coderRegistrar);
  }

  private <T> void assertProviderReturnsKryoCoderForClass(KryoCoderProvider provider, Class<T> type)
      throws CannotProvideCoderException {
    IdentifiedRegistrar providerRegistrar = provider.getKryoRegistrar();
    Assert.assertNotNull(providerRegistrar);
    Coder<T> coderToAssert = provider.coderFor(TypeDescriptor.of(type), Collections.emptyList());

    Assert.assertNotNull(coderToAssert);
    Assert.assertTrue(coderToAssert instanceof KryoCoder);
    KryoCoder<T> casted = (KryoCoder<T>) coderToAssert;
    IdentifiedRegistrar coderRegistrar = casted.getRegistrar();
    Assert.assertNotNull(coderRegistrar);
    Assert.assertEquals(providerRegistrar.getId(), coderRegistrar.getId());
  }

  private static class FirstTestClass {}

  private static class SecondTestClass {}

  private static class ThirdTestClass {}

  private static class NeverRegisteredClass {}
}
