package us.dot.its.jpo.ode.kafka.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaProduceMetricsTest {

  private SimpleMeterRegistry registry;
  private KafkaProduceMetrics metrics;

  @BeforeEach
  void setUp() {
    registry = new SimpleMeterRegistry();
    metrics = new KafkaProduceMetrics(registry);
  }

  @Test
  void recordTopicIncrementsTopicCounter() {
    metrics.recordTopic("topic.OdeBsmJson");
    metrics.recordTopic("topic.OdeBsmJson");

    assertEquals(2.0, registry.get("kafka.produced.messages")
        .tag("topic", "topic.OdeBsmJson")
        .counter()
        .count());
  }

  @Test
  void recordRsuIncrementsRsuCounterWithoutTopicDoubleCount() {
    metrics.recordRsu("topic.OdeBsmJson", "192.168.1.10");
    metrics.recordRsu("topic.OdeBsmJson", "192.168.1.10");

    assertEquals(2.0, registry.get("kafka.produced.rsu.messages")
        .tag("topic", "topic.OdeBsmJson")
        .tag("rsu_ip", "192.168.1.10")
        .counter()
        .count());
    // RSU path must not also bump the topic-only counter (template owns that).
    assertNull(registry.find("kafka.produced.messages").counter());
  }

  @Test
  void recordRsuIgnoresBlankOriginIp() {
    metrics.recordRsu("topic.OdeBsmJson", " ");
    metrics.recordRsu("topic.OdeBsmJson", null);

    assertNull(registry.find("kafka.produced.rsu.messages").counter());
  }

  @Test
  void recordTopicIgnoresNullTopic() {
    metrics.recordTopic(null);
    assertNull(registry.find("kafka.produced.messages").counter());
  }
}
