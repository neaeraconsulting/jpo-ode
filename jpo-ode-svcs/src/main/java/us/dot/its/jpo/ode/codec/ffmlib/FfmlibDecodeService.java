package us.dot.its.jpo.ode.codec.ffmlib;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PreDestroy;
import java.net.DatagramPacket;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import us.dot.its.jpo.asn.j2735.r2024.MessageFrame.DSRCmsgID;
import us.dot.its.jpo.asn.j2735.r2024.MessageFrame.MessageFrame;
import us.dot.its.jpo.ode.codec.ffmlib.FfmlibMessageFrameCodec.IntermediateDecodeResult;
import us.dot.its.jpo.ode.codec.ffmlib.FfmlibMessageFrameCodec.IntermediateEncoding;
import us.dot.its.jpo.ode.kafka.producer.KafkaProduceMetrics;
import us.dot.its.jpo.ode.kafka.topics.JsonTopics;
import us.dot.its.jpo.ode.model.OdeAsn1Data;
import us.dot.its.jpo.ode.model.OdeHexByteArray;
import us.dot.its.jpo.ode.model.OdeLogMetadata.RecordType;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;
import us.dot.its.jpo.ode.model.OdeMessageFrameMetadata;
import us.dot.its.jpo.ode.model.OdeMessageFrameMetadata.Source;
import us.dot.its.jpo.ode.model.OdeMessageFramePayload;
import us.dot.its.jpo.ode.model.OdeMsgMetadata.GeneratedBy;
import us.dot.its.jpo.ode.model.RxSource;
import us.dot.its.jpo.ode.udp.InvalidPayloadException;
import us.dot.its.jpo.ode.udp.UdpHexDecoder;
import us.dot.its.jpo.ode.udp.UdpHexDecoder.UdpDecodeInput;
import us.dot.its.jpo.ode.uper.SupportedMessageType;
import us.dot.its.jpo.ode.util.CodecUtils;
import us.dot.its.jpo.ode.util.JsonUtils;

/**
 * Primary in-process J2735 UPER decode service backed by the FFMLib native codec.
 *
 * <p>UDP path: cheap prep runs on the receiver thread, then native decode / POJO / JSON / Kafka
 * publish run on a bounded worker pool (one ThreadLocal native codec per worker).
 *
 * <p>Import path ({@link #decode(OdeAsn1Data, String)}) runs synchronously on the Kafka listener
 * thread.
 */
@Slf4j
@Service
public class FfmlibDecodeService {

  private record MessageTypeConfig(
      RecordType recordType, Source source, GeneratedBy generatedBy, boolean includeRxDetails) {}

  private static final Map<SupportedMessageType, MessageTypeConfig> MSG_TYPE_CONFIGS = Map.of(
      SupportedMessageType.BSM,  new MessageTypeConfig(RecordType.bsmTx,  Source.EV,  GeneratedBy.OBU,     true),
      SupportedMessageType.TIM,  new MessageTypeConfig(RecordType.timMsg, Source.RSU, GeneratedBy.RSU,     false),
      SupportedMessageType.MAP,  new MessageTypeConfig(RecordType.mapTx,  Source.RSU, GeneratedBy.RSU,     false),
      SupportedMessageType.SPAT, new MessageTypeConfig(RecordType.spatTx, Source.RSU, GeneratedBy.RSU,     false),
      SupportedMessageType.SSM,  new MessageTypeConfig(RecordType.ssmTx,  Source.RSU, GeneratedBy.RSU,     false),
      SupportedMessageType.SRM,  new MessageTypeConfig(RecordType.srmTx,  Source.RSU, GeneratedBy.OBU,     false),
      SupportedMessageType.PSM,  new MessageTypeConfig(RecordType.psmTx,  Source.RSU, GeneratedBy.UNKNOWN, false),
      SupportedMessageType.SDSM, new MessageTypeConfig(RecordType.sdsmTx, Source.RSU, GeneratedBy.RSU,     false),
      SupportedMessageType.RTCM, new MessageTypeConfig(RecordType.rtcmTx, Source.RSU, GeneratedBy.RSU,     false),
      SupportedMessageType.RSM,  new MessageTypeConfig(RecordType.rsmTx,  Source.RSU, GeneratedBy.RSU,     false)
  );

  private final FfmlibMessageFrameCodec ffmlibCodec;
  private final JsonTopics jsonTopics;
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final KafkaProduceMetrics produceMetrics;
  private final XmlMapper simpleXmlMapper;
  private final ObjectMapper simpleObjectMapper;
  private final Timer prepTimer;
  private final Timer nativeTimer;
  private final Timer pojoTimer;
  private final Timer jsonTimer;
  private final Timer sendTimer;
  private final Timer totalTimer;
  private final Counter queueRejectedCounter;
  private final Counter submittedCounter;
  private final ExecutorService udpDecodeExecutor;

  public FfmlibDecodeService(
      FfmlibMessageFrameCodec ffmlibCodec,
      FfmlibProperties properties,
      JsonTopics jsonTopics,
      KafkaTemplate<String, String> kafkaTemplate,
      KafkaProduceMetrics produceMetrics,
      @Qualifier("simpleXmlMapper") XmlMapper simpleXmlMapper,
      @Qualifier("simpleObjectMapper") ObjectMapper simpleObjectMapper,
      MeterRegistry meterRegistry) {
    this.ffmlibCodec = ffmlibCodec;
    this.jsonTopics = jsonTopics;
    this.kafkaTemplate = kafkaTemplate;
    this.produceMetrics = produceMetrics;
    this.simpleXmlMapper = simpleXmlMapper;
    this.simpleObjectMapper = simpleObjectMapper;
    this.prepTimer = Timer.builder("ode.ffmlib.decode.stage")
        .tag("stage", "prep").register(meterRegistry);
    this.nativeTimer = Timer.builder("ode.ffmlib.decode.stage")
        .tag("stage", "native").register(meterRegistry);
    this.pojoTimer = Timer.builder("ode.ffmlib.decode.stage")
        .tag("stage", "pojo").register(meterRegistry);
    this.jsonTimer = Timer.builder("ode.ffmlib.decode.stage")
        .tag("stage", "json").register(meterRegistry);
    this.sendTimer = Timer.builder("ode.ffmlib.decode.stage")
        .tag("stage", "send").register(meterRegistry);
    this.totalTimer = Timer.builder("ode.ffmlib.decode.total").register(meterRegistry);
    this.queueRejectedCounter = Counter.builder("ode.ffmlib.decode.queue_rejected")
        .description("UDP decode tasks dropped because the worker queue was full")
        .register(meterRegistry);
    this.submittedCounter = Counter.builder("ode.ffmlib.decode.submitted")
        .description("UDP decode tasks accepted onto the worker queue")
        .register(meterRegistry);
    this.udpDecodeExecutor = createUdpDecodeExecutor(properties);
  }

  private static ExecutorService createUdpDecodeExecutor(FfmlibProperties properties) {
    int workers = properties.getUdpDecodeWorkers();
    if (workers <= 0) {
      workers = Math.max(2, Runtime.getRuntime().availableProcessors());
    }
    int queueCapacity = Math.max(1, properties.getUdpDecodeQueueCapacity());
    AtomicInteger seq = new AtomicInteger();
    ThreadFactory factory = r -> {
      Thread t = new Thread(r, "ffmlib-udp-decode-" + seq.incrementAndGet());
      t.setDaemon(true);
      return t;
    };
    log.info("FFMLib UDP decode pool: workers={}, queueCapacity={}", workers, queueCapacity);
    return new ThreadPoolExecutor(
        workers,
        workers,
        0L,
        TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(queueCapacity),
        factory,
        new ThreadPoolExecutor.AbortPolicy());
  }

  /**
   * Decodes a UDP packet: prep on the caller (receiver) thread, then heavy work on the
   * decode worker pool.
   */
  public void decode(DatagramPacket packet, SupportedMessageType msgType)
      throws InvalidPayloadException {
    MessageTypeConfig config = MSG_TYPE_CONFIGS.get(msgType);
    if (config == null) {
      log.warn("FFMLib decode: unsupported message type {}", msgType);
      return;
    }

    long prepStart = System.nanoTime();
    UdpDecodeInput input = UdpHexDecoder.prepareDecodeInput(
        packet, msgType, config.recordType(), config.source(), config.generatedBy(),
        config.includeRxDetails());
    prepTimer.record(System.nanoTime() - prepStart, TimeUnit.NANOSECONDS);

    // Hand off owned copies (uper bytes + metadata) — safe after receive buffer reuse.
    final OdeMessageFrameMetadata metadata = input.metadata();
    final byte[] uperBytes = input.uperBytes();
    try {
      udpDecodeExecutor.execute(() -> runPublishDecoded(metadata, uperBytes, null, msgType));
      submittedCounter.increment();
    } catch (java.util.concurrent.RejectedExecutionException ex) {
      queueRejectedCounter.increment();
      log.warn("FFMLib UDP decode queue full; dropping {} packet from {}",
          msgType, metadata.getOriginIp());
    }
  }

  /**
   * Decodes an already-parsed {@link OdeAsn1Data} — used by the file-import path through
   * {@code RawEncoded*JsonRouter} and {@code topic.OdeRawEncoded*Json}.
   */
  public void decode(OdeAsn1Data asn1Data, String key) {
    try {
      long prepStart = System.nanoTime();
      OdeHexByteArray hexBytes = (OdeHexByteArray) asn1Data.getPayload().getData();
      byte[] uperBytes = CodecUtils.fromHex(hexBytes.getBytes());
      prepTimer.record(System.nanoTime() - prepStart, TimeUnit.NANOSECONDS);

      // totalTimer is recorded inside runPublishDecoded (same as the UDP worker path).
      runPublishDecoded(
          (OdeMessageFrameMetadata) asn1Data.getMetadata(), uperBytes, key, null);
    } catch (ClassCastException e) {
      log.error("FFMLib decode failed (unexpected payload type) for key {}: {}", key, e.getMessage(),
          e);
    } catch (Exception e) {
      log.error("FFMLib decode unexpected error for key {}: {}", key, e.getMessage(), e);
    }
  }

  private void runPublishDecoded(
      OdeMessageFrameMetadata metadata,
      byte[] uperBytes,
      String key,
      SupportedMessageType knownType) {
    long totalStart = System.nanoTime();
    try {
      publishDecoded(metadata, uperBytes, key, knownType);
    } finally {
      totalTimer.record(System.nanoTime() - totalStart, TimeUnit.NANOSECONDS);
    }
  }

  private void publishDecoded(
      OdeMessageFrameMetadata metadata,
      byte[] uperBytes,
      String key,
      SupportedMessageType knownType) {
    try {
      long nativeStart = System.nanoTime();
      IntermediateDecodeResult intermediate = ffmlibCodec.uperToIntermediate(uperBytes);
      long nativeNanos = System.nanoTime() - nativeStart;
      nativeTimer.record(nativeNanos, TimeUnit.NANOSECONDS);

      long pojoStart = System.nanoTime();
      MessageFrame<?> messageFrame = parseMessageFrame(intermediate);
      long pojoNanos = System.nanoTime() - pojoStart;
      pojoTimer.record(pojoNanos, TimeUnit.NANOSECONDS);

      long asnDecodeLatencyMs = (nativeNanos + pojoNanos) / 1_000_000;
      metadata.setAsnDecodeLatencyMs(Long.valueOf(asnDecodeLatencyMs));
      metadata.setEncodings(null);
      if (metadata.getReceivedMessageDetails() != null
          && metadata.getReceivedMessageDetails().getRxSource() == null) {
        metadata.getReceivedMessageDetails().setRxSource(RxSource.NA);
      }
      if (metadata.getSchemaVersion() <= 4) {
        metadata.setReceivedMessageDetails(null);
      }

      OdeMessageFramePayload payload = new OdeMessageFramePayload(messageFrame);
      OdeMessageFrameData frameData = new OdeMessageFrameData(metadata, payload);

      long jsonStart = System.nanoTime();
      String json = JsonUtils.toJson(frameData, false);
      jsonTimer.record(System.nanoTime() - jsonStart, TimeUnit.NANOSECONDS);

      String topic = resolveJsonTopic(knownType, messageFrame);
      if (topic == null) {
        log.warn("FFMLib decode: no topic mapped for message, key {} dropped.", key);
        return;
      }

      // RSU metric from known metadata — avoids JSON re-parse in KafkaTemplate.
      produceMetrics.recordRsu(topic, metadata.getOriginIp());

      long sendStart = System.nanoTime();
      kafkaTemplate.send(topic, key, json);
      sendTimer.record(System.nanoTime() - sendStart, TimeUnit.NANOSECONDS);

      if (log.isDebugEnabled()) {
        log.debug(
            "FFMLib decode key={} encoding={} native={}us pojo={}us totalAsn={}ms topic={}",
            key,
            intermediate.encoding(),
            nativeNanos / 1000,
            pojoNanos / 1000,
            asnDecodeLatencyMs,
            topic);
      }
    } catch (JsonProcessingException e) {
      log.error("FFMLib decode failed (JSON/XML processing) for key {}: {}", key, e.getMessage(), e);
    } catch (Exception e) {
      log.error("FFMLib decode unexpected error for key {}: {}", key, e.getMessage(), e);
    }
  }

  private MessageFrame<?> parseMessageFrame(IntermediateDecodeResult intermediate)
      throws JsonProcessingException {
    if (intermediate.encoding() == IntermediateEncoding.JER) {
      return simpleObjectMapper.readValue(intermediate.text(), MessageFrame.class);
    }
    return simpleXmlMapper.readValue(intermediate.text(), MessageFrame.class);
  }

  private String resolveJsonTopic(SupportedMessageType knownType, MessageFrame<?> messageFrame) {
    if (knownType != null) {
      String topic = resolveJsonTopic(knownType);
      if (topic != null) {
        return topic;
      }
    }
    DSRCmsgID msgId = messageFrame.getMessageId();
    String messageName = msgId.name().orElse("Unknown");
    return resolveJsonTopicByMessageName(messageName);
  }

  private String resolveJsonTopic(SupportedMessageType msgType) {
    return switch (msgType) {
      case BSM -> jsonTopics.getBsm();
      case TIM -> jsonTopics.getTim();
      case MAP -> jsonTopics.getMap();
      case SPAT -> jsonTopics.getSpat();
      case PSM -> jsonTopics.getPsm();
      case SSM -> jsonTopics.getSsm();
      case SRM -> jsonTopics.getSrm();
      case SDSM -> jsonTopics.getSdsm();
      case RTCM -> jsonTopics.getRtcm();
      case RSM -> jsonTopics.getRsm();
    };
  }

  private String resolveJsonTopicByMessageName(String messageName) {
    return switch (messageName) {
      case "basicSafetyMessage" -> jsonTopics.getBsm();
      case "travelerInformation" -> jsonTopics.getTim();
      case "mapData" -> jsonTopics.getMap();
      case "signalPhaseAndTimingMessage" -> jsonTopics.getSpat();
      case "personalSafetyMessage" -> jsonTopics.getPsm();
      case "signalStatusMessage" -> jsonTopics.getSsm();
      case "signalRequestMessage" -> jsonTopics.getSrm();
      case "sensorDataSharingMessage" -> jsonTopics.getSdsm();
      case "rtcmCorrections" -> jsonTopics.getRtcm();
      case "roadSafetyMessage" -> jsonTopics.getRsm();
      default -> null;
    };
  }

  @PreDestroy
  public void shutdown() {
    log.info("Shutting down FFMLib UDP decode worker pool...");
    udpDecodeExecutor.shutdown();
    try {
      if (!udpDecodeExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
        udpDecodeExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      udpDecodeExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
