package us.dot.its.jpo.ode.kafka.producer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.stereotype.Component;

/**
 * Cached Prometheus counters for Kafka produce metrics.
 *
 * <p>Hot paths (FFMLib decode) should call {@link #record(String, String)} with a known
 * {@code originIp} instead of re-parsing JSON in {@link InterceptingKafkaTemplate}.
 */
@Component
public class KafkaProduceMetrics {

  private final MeterRegistry meterRegistry;
  private final ConcurrentMap<String, Counter> topicCounters = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Counter> rsuCounters = new ConcurrentHashMap<>();

  public KafkaProduceMetrics(MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
  }

  /**
   * Records topic-level produce count (used by {@link InterceptingKafkaTemplate} for all
   * producers — no JSON parsing).
   */
  public void recordTopic(String topic) {
    if (topic == null) {
      return;
    }
    topicCounters
        .computeIfAbsent(topic, this::newTopicCounter)
        .increment();
  }

  /**
   * Records RSU-tagged produce count using an already-known {@code originIp} (FFMLib decode
   * hot path). Does not re-parse message JSON.
   */
  public void recordRsu(String topic, String originIp) {
    if (topic == null || originIp == null || originIp.isBlank()) {
      return;
    }
    String key = topic + '\0' + originIp;
    rsuCounters
        .computeIfAbsent(key, ignored -> newRsuCounter(topic, originIp))
        .increment();
  }

  private Counter newTopicCounter(String topic) {
    return Counter.builder("kafka.produced.messages")
        .description("Number of produced Kafka messages")
        .tag("topic", topic)
        .register(meterRegistry);
  }

  private Counter newRsuCounter(String topic, String originIp) {
    return Counter.builder("kafka.produced.rsu.messages")
        .description("Number of produced Kafka messages by RSU")
        .tag("topic", topic)
        .tag("rsu_ip", originIp)
        .register(meterRegistry);
  }
}
