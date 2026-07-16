package us.dot.its.jpo.ode.codec.ffmlib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.tomcat.util.buf.HexUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import us.dot.its.jpo.asn.j2735.r2024.BasicSafetyMessage.BasicSafetyMessageMessageFrame;
import us.dot.its.jpo.asn.j2735.r2024.MessageFrame.DSRCmsgID;
import us.dot.its.jpo.asn.j2735.r2024.MessageFrame.MessageFrame;
import us.dot.its.jpo.ode.codec.ffmlib.FfmlibMessageFrameCodec.IntermediateDecodeResult;
import us.dot.its.jpo.ode.codec.ffmlib.FfmlibMessageFrameCodec.IntermediateEncoding;
import us.dot.its.jpo.ode.kafka.producer.KafkaProduceMetrics;
import us.dot.its.jpo.ode.kafka.topics.JsonTopics;
import us.dot.its.jpo.ode.model.OdeAsn1Data;
import us.dot.its.jpo.ode.model.OdeAsn1Payload;
import us.dot.its.jpo.ode.model.OdeHexByteArray;
import us.dot.its.jpo.ode.model.OdeMessageFrameMetadata;
import us.dot.its.jpo.ode.uper.SupportedMessageType;

@ExtendWith(MockitoExtension.class)
class FfmlibDecodeServiceTest {

  private static final String BSM_TOPIC = "topic.OdeBsmJson";
  private static final String BSM_HEX =
      "001480ADDA7CDE5517E962C66947240CB711E804C8B106B7DB7B12B3056B8AA1AA4E838D00400F86822A3CD398D89E1BB8405B72C3C7A398C3CAFF63338526C646F4FFF524AD9E404039D5DA2FA62FEB57E305B552C7BE088B61E52A6BFC8CAF5AF64414F3E4513FEC189F8B5E1138B824A48B29BA1F43CB12CE296BCA3DFA8F651AB44AB1B81B633B797D5645DAA4EDADAB4AC22A0BC38AB361443395BAA2C81CC4538E7413E9C8C3F696BB2C9B6B0000";

  @Mock
  private FfmlibMessageFrameCodec ffmlibCodec;
  @Mock
  private JsonTopics jsonTopics;
  @Mock
  private KafkaTemplate<String, String> kafkaTemplate;
  @Mock
  private XmlMapper simpleXmlMapper;
  @Mock
  private ObjectMapper simpleObjectMapper;

  private SimpleMeterRegistry meterRegistry;
  private KafkaProduceMetrics produceMetrics;
  private FfmlibDecodeService decodeService;

  @BeforeEach
  void setUp() {
    meterRegistry = new SimpleMeterRegistry();
    produceMetrics = new KafkaProduceMetrics(meterRegistry);
    FfmlibProperties properties = new FfmlibProperties();
    properties.setUdpDecodeWorkers(2);
    properties.setUdpDecodeQueueCapacity(64);
    decodeService = new FfmlibDecodeService(
        ffmlibCodec,
        properties,
        jsonTopics,
        kafkaTemplate,
        produceMetrics,
        simpleXmlMapper,
        simpleObjectMapper,
        meterRegistry);
  }

  @AfterEach
  void tearDown() {
    decodeService.shutdown();
  }

  @Test
  void decodeOdeAsn1DataPublishesJsonAndRecordsRsuMetric() throws Exception {
    stubSuccessfulDecode();

    OdeMessageFrameMetadata metadata = new OdeMessageFrameMetadata();
    metadata.setOriginIp("10.0.0.5");
    metadata.setSchemaVersion(9);
    OdeAsn1Data asn1Data = new OdeAsn1Data(metadata, new OdeAsn1Payload(new OdeHexByteArray(BSM_HEX)));

    decodeService.decode(asn1Data, "key-1");

    ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);
    verify(kafkaTemplate).send(eq(BSM_TOPIC), eq("key-1"), jsonCaptor.capture());
    assertTrue(jsonCaptor.getValue().contains("10.0.0.5"));
    assertTrue(jsonCaptor.getValue().contains("asnDecodeLatencyMs")
        || metadata.getAsnDecodeLatencyMs() != null);

    assertEquals(1.0, meterRegistry.get("kafka.produced.rsu.messages")
        .tag("topic", BSM_TOPIC)
        .tag("rsu_ip", "10.0.0.5")
        .counter()
        .count());
    assertEquals(1.0, meterRegistry.get("ode.ffmlib.decode.total").timer().count(), 0.0);
  }

  @Test
  void decodeUdpPacketUsesWorkerPoolAndKnownMessageTypeTopic() throws Exception {
    stubSuccessfulDecode();

    byte[] bytes = HexUtils.fromHexString(BSM_HEX);
    DatagramPacket packet = new DatagramPacket(
        bytes, bytes.length, InetAddress.getByName("127.0.0.1"), 5000);

    decodeService.decode(packet, SupportedMessageType.BSM);

    verify(kafkaTemplate, timeout(5000)).send(eq(BSM_TOPIC), isNull(), any(String.class));
    verify(ffmlibCodec, timeout(5000).atLeastOnce()).uperToIntermediate(any(byte[].class));
    assertEquals(1.0, meterRegistry.get("ode.ffmlib.decode.submitted").counter().count());
    assertEquals(1.0, meterRegistry.get("kafka.produced.rsu.messages")
        .tag("topic", BSM_TOPIC)
        .tag("rsu_ip", "127.0.0.1")
        .counter()
        .count());
  }

  @Test
  void decodeUdpPacketIncrementsRejectedWhenQueueIsFull() throws Exception {
    decodeService.shutdown();

    FfmlibProperties saturated = new FfmlibProperties();
    saturated.setUdpDecodeWorkers(1);
    saturated.setUdpDecodeQueueCapacity(1);
    decodeService = new FfmlibDecodeService(
        ffmlibCodec,
        saturated,
        jsonTopics,
        kafkaTemplate,
        produceMetrics,
        simpleXmlMapper,
        simpleObjectMapper,
        meterRegistry);

    CountDownLatch enteredNative = new CountDownLatch(1);
    CountDownLatch blocker = new CountDownLatch(1);
    AtomicInteger nativeCalls = new AtomicInteger();
    when(ffmlibCodec.uperToIntermediate(any())).thenAnswer(invocation -> {
      nativeCalls.incrementAndGet();
      enteredNative.countDown();
      blocker.await(5, TimeUnit.SECONDS);
      return new IntermediateDecodeResult("<MessageFrame/>", IntermediateEncoding.XER);
    });

    byte[] bytes = HexUtils.fromHexString(BSM_HEX);
    DatagramPacket packet = new DatagramPacket(
        bytes, bytes.length, InetAddress.getLoopbackAddress(), 5000);

    // Occupy the single worker, then fill the one-slot queue, then force a rejection.
    decodeService.decode(packet, SupportedMessageType.BSM);
    assertTrue(enteredNative.await(5, TimeUnit.SECONDS), "worker should enter native decode");
    decodeService.decode(packet, SupportedMessageType.BSM);
    decodeService.decode(packet, SupportedMessageType.BSM);

    assertTrue(meterRegistry.get("ode.ffmlib.decode.queue_rejected").counter().count() >= 1.0);
    blocker.countDown();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void stubSuccessfulDecode() throws Exception {
    when(jsonTopics.getBsm()).thenReturn(BSM_TOPIC);
    when(ffmlibCodec.uperToIntermediate(any()))
        .thenReturn(new IntermediateDecodeResult("<MessageFrame/>", IntermediateEncoding.XER));

    MessageFrame frame = mock(BasicSafetyMessageMessageFrame.class);
    DSRCmsgID msgId = mock(DSRCmsgID.class);
    // Import path resolves topic from messageId; UDP path uses knownType and may skip these.
    lenient().when(frame.getMessageId()).thenReturn(msgId);
    lenient().when(msgId.name()).thenReturn(Optional.of("basicSafetyMessage"));
    when(simpleXmlMapper.readValue(any(String.class), eq(MessageFrame.class))).thenReturn(frame);
  }
}
