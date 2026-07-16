package us.dot.its.jpo.ode.kafka;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import us.dot.its.jpo.ode.kafka.producer.KafkaProduceMetrics;

/**
 * Test configuration for Micrometer and Kafka produce metrics beans used by slice tests
 * that load {@code KafkaProducerConfig}.
 */
@TestConfiguration
public class TestMetricsConfig {

  @Bean
  public MeterRegistry meterRegistry() {
    return new SimpleMeterRegistry();
  }

  @Bean
  public KafkaProduceMetrics kafkaProduceMetrics(MeterRegistry meterRegistry) {
    return new KafkaProduceMetrics(meterRegistry);
  }
}
