package us.dot.its.jpo.ode.kafka.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.Observation;
import java.lang.reflect.Method;
import java.util.Set;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.ProducerFactory;

class InterceptingKafkaTemplateMetricsTest {

  @Test
  void doSendRecordsTopicCounterAndSkipsDisabledTopicsWithoutJsonParse() throws Exception {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    KafkaProduceMetrics metrics = new KafkaProduceMetrics(registry);

    @SuppressWarnings("unchecked")
    ProducerFactory<String, String> factory = mock(ProducerFactory.class);
    when(factory.transactionCapable()).thenReturn(false);

    InterceptingKafkaTemplate<String, String> template =
        new InterceptingKafkaTemplate<>(factory, Set.of("blocked-topic"), metrics);

    Method doSend = InterceptingKafkaTemplate.class.getDeclaredMethod(
        "doSend", ProducerRecord.class, Observation.class);
    doSend.setAccessible(true);
    Observation observation = mock(Observation.class);

    ProducerRecord<String, String> blocked =
        new ProducerRecord<>("blocked-topic", "k",
            "{\"metadata\":{\"originIp\":\"10.0.0.1\"}}");
    doSend.invoke(template, blocked, observation);
    assertNull(registry.find("kafka.produced.messages").counter());
    assertNull(registry.find("kafka.produced.rsu.messages").counter());

    ProducerRecord<String, String> enabled =
        new ProducerRecord<>("topic.OdeBsmJson", "k",
            "{\"metadata\":{\"originIp\":\"10.0.0.1\"}}");
    try {
      doSend.invoke(template, enabled, observation);
    } catch (Exception ignored) {
      // super.doSend may fail without a live producer; metrics are recorded first.
    }

    assertEquals(1.0, registry.get("kafka.produced.messages")
        .tag("topic", "topic.OdeBsmJson")
        .counter()
        .count());
    // Must not invent RSU labels by parsing JSON in the template.
    assertNull(registry.find("kafka.produced.rsu.messages").counter());
  }
}
