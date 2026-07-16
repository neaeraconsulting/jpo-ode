package us.dot.its.jpo.ode.kafka.producer;

import io.micrometer.observation.Observation;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.lang.NonNull;

/**
 * KafkaTemplate that blocks publishes to configured disabled topics and records a cheap
 * topic-level produce counter.
 *
 * <p>RSU-tagged counters ({@code kafka.produced.rsu.messages}) are incremented on the FFMLib
 * decode path via {@link KafkaProduceMetrics} using {@code metadata.originIp} — not by
 * re-parsing JSON here.
 *
 * @param <K> the type of message key
 * @param <V> the type of message value
 */
@Slf4j
public class InterceptingKafkaTemplate<K, V> extends KafkaTemplate<K, V> {

  private final Set<String> disabledTopics;
  private final KafkaProduceMetrics produceMetrics;

  public InterceptingKafkaTemplate(
      ProducerFactory<K, V> producerFactory,
      Set<String> disabledTopics,
      KafkaProduceMetrics produceMetrics) {
    super(producerFactory);
    this.disabledTopics = disabledTopics;
    this.produceMetrics = produceMetrics;
  }

  /**
   * Send the producer record if the producerRecord's topic is not contained in
   * the disabledTopics.
   *
   * @param producerRecord the producer record.
   * @param observation    the observation.
   * @return a Future for the {@link RecordMetadata RecordMetadata}.
   */
  @Override
  protected CompletableFuture<SendResult<K, V>> doSend(
      final ProducerRecord<K, V> producerRecord,
      @NonNull Observation observation) {
    if (disabledTopics != null && disabledTopics.contains(producerRecord.topic())) {
      log.debug("Blocked attempt to send data to disabled topic {}", producerRecord.topic());
      return new CompletableFuture<>();
    }

    // Topic-only counter — no JSON parsing. RSU labels are recorded on the decode path.
    if (produceMetrics != null) {
      produceMetrics.recordTopic(producerRecord.topic());
    }

    return super.doSend(producerRecord, observation);
  }
}
