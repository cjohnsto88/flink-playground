package com.craig.flink.playground.job;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.extension.*;
import org.junit.platform.commons.support.ReflectionSupport;
import org.junit.platform.commons.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.testcontainers.containers.KafkaContainer;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class MyExtension implements ParameterResolver, BeforeEachCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(MyExtension.class);

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return false;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return null;
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        Optional<Class<?>> testClass = context.getTestClass();

        List<Field> fields = testClass.map(findKafkaContainerFields())
                                      .orElse(Collections.emptyList());

        for (Field field : fields) {
            KafkaContainer kafkaContainer = ReflectionSupport.tryToReadFieldValue(field, context.getRequiredTestInstance())
                                                             .andThenTry(obj -> (KafkaContainer) obj)
                                                             .getOrThrow(Function.identity());

            String bootstrapServers = kafkaContainer.getBootstrapServers();

            LOGGER.info(bootstrapServers);

            Map<String, Object> consumerProperties = KafkaTestUtils.consumerProps(bootstrapServers, "test-group", "true");
            ConsumerFactory<?, ?> consumerFactory = (kd, vd) -> new KafkaConsumer<>(consumerProperties, kd, vd);
        }

        fields.forEach(field -> LOGGER.info("{}", field));
    }

    private static @NotNull Function<Class<?>, List<Field>> findKafkaContainerFields() {
        return clazz -> ReflectionUtils.findFields(clazz, field -> field.getType().isAssignableFrom(KafkaContainer.class), ReflectionUtils.HierarchyTraversalMode.BOTTOM_UP);
    }

    @FunctionalInterface
    public interface ConsumerFactory<K, V> {

        Consumer<K, V> createConsumer(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer);

    }
}
