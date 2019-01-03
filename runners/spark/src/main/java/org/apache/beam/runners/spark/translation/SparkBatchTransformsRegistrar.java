package org.apache.beam.runners.spark.translation;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.beam.runners.core.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.runners.core.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.runners.spark.translation.TransformTranslator.SparkFlatMapElementsTranslator;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.PTransform;

/** Registers classes specialized by the Spark runner.
 * <p>
 * Not that this class should be non-inner as requested in {@link AutoService} javadoc.
 * </p>
 * */
@AutoService(TransformPayloadTranslatorRegistrar.class)
public class SparkBatchTransformsRegistrar implements TransformPayloadTranslatorRegistrar {
  @Override
  public Map<
      ? extends Class<? extends PTransform>,
      ? extends TransformPayloadTranslator>
  getTransformPayloadTranslators() {
    return ImmutableMap.of(FlatMapElements.class, new SparkFlatMapElementsTranslator());
  }
}

