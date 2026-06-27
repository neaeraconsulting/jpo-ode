package us.dot.its.jpo.ode.codec.ffmlib;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import us.dot.its.jpo.ode.model.OdeAsn1Data;
import us.dot.its.jpo.ode.model.OdeObject;

/**
 * Default {@link RawEncodedDecodeService} implementation that forwards prepared ASN.1 data
 * to the {@code topic.Asn1DecoderInput} Kafka topic for the external asn1_codec microservice.
 *
 * <p>Active when {@code ode.ffmlib.enabled} is {@code false} or not set (the default).
 */
@Service
@ConditionalOnProperty(name = "ode.ffmlib.enabled", havingValue = "false", matchIfMissing = true)
public class KafkaAsn1CoderDecodeService implements RawEncodedDecodeService {

  private final KafkaTemplate<String, OdeObject> kafkaTemplate;
  private final String decoderInputTopic;

  public KafkaAsn1CoderDecodeService(
      KafkaTemplate<String, OdeObject> kafkaTemplate,
      @Value("${ode.kafka.topics.asn1.decoder-input}") String decoderInputTopic) {
    this.kafkaTemplate = kafkaTemplate;
    this.decoderInputTopic = decoderInputTopic;
  }

  @Override
  public void decode(OdeAsn1Data asn1Data, String key) {
    kafkaTemplate.send(decoderInputTopic, key, asn1Data);
  }
}
