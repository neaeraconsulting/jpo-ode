package us.dot.its.jpo.ode.kafka.listeners.asn1;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import us.dot.its.jpo.ode.codec.ffmlib.FfmlibDecodeService;
import us.dot.its.jpo.ode.config.SerializationConfig;
import us.dot.its.jpo.ode.kafka.KafkaConsumerConfig;
import us.dot.its.jpo.ode.kafka.OdeKafkaProperties;
import us.dot.its.jpo.ode.kafka.TestMetricsConfig;
import us.dot.its.jpo.ode.kafka.listeners.json.RawEncodedJsonService;
import us.dot.its.jpo.ode.kafka.listeners.json.RawEncodedSRMJsonRouter;
import us.dot.its.jpo.ode.kafka.producer.KafkaProducerConfig;
import us.dot.its.jpo.ode.kafka.topics.RawEncodedJsonTopics;
import us.dot.its.jpo.ode.model.OdeAsn1Data;
import us.dot.its.jpo.ode.test.utilities.EmbeddedKafkaHolder;
import us.dot.its.jpo.ode.udp.controller.UDPReceiverProperties;

@SpringBootTest(
    classes = {KafkaProducerConfig.class, KafkaConsumerConfig.class, RawEncodedSRMJsonRouter.class,
        RawEncodedJsonService.class, SerializationConfig.class, TestMetricsConfig.class},
    properties = {"ode.kafka.topics.raw-encoded-json.srm=topic.RawEncodedSRMJsonRouterTest"})
@EnableConfigurationProperties
@ContextConfiguration(classes = {UDPReceiverProperties.class, OdeKafkaProperties.class,
    RawEncodedJsonTopics.class, KafkaProperties.class})
@DirtiesContext
class RawEncodedSRMJsonRouterTest {

  @Value("${ode.kafka.topics.raw-encoded-json.srm}")
  private String rawEncodedSrmJson;

  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;

  @MockBean
  FfmlibDecodeService decodeService;

  @Test
  void testListen() throws Exception {
    EmbeddedKafkaHolder.addTopics(rawEncodedSrmJson);

    try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(
        "us/dot/its/jpo/ode/kafka/listeners/asn1/decoder-input-srm.json")) {
      assert inputStream != null;
      var json = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
      kafkaTemplate.send(rawEncodedSrmJson, json);
    }

    verify(decodeService, timeout(5000)).decode(any(OdeAsn1Data.class), any());
  }
}
