package us.dot.its.jpo.ode.codec.ffmlib;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import us.dot.its.jpo.asn.j2735.r2024.MessageFrame.DSRCmsgID;
import us.dot.its.jpo.asn.j2735.r2024.MessageFrame.MessageFrame;
import us.dot.its.jpo.ode.kafka.topics.JsonTopics;
import us.dot.its.jpo.ode.model.OdeAsn1Data;
import us.dot.its.jpo.ode.model.OdeHexByteArray;
import us.dot.its.jpo.ode.model.OdeMessageFrameData;
import us.dot.its.jpo.ode.model.OdeMessageFrameMetadata;
import us.dot.its.jpo.ode.model.OdeMessageFramePayload;
import us.dot.its.jpo.ode.model.RxSource;
import us.dot.its.jpo.ode.util.CodecUtils;
import us.dot.its.jpo.ode.util.JsonUtils;

/**
 * FFMLib-backed {@link RawEncodedDecodeService} that decodes J2735 UPER messages entirely
 * in-process, bypassing the {@code topic.Asn1DecoderInput} / {@code topic.Asn1DecoderOutput}
 * Kafka topics and the external asn1_codec microservice.
 *
 * <p>The decode pipeline per message:
 * <ol>
 *   <li>Extract UPER bytes from the {@code OdeAsn1Payload} hex array.</li>
 *   <li>Decode to XER via {@link FfmlibMessageFrameCodec#uperToXer(byte[])}.</li>
 *   <li>Deserialize XER to a typed {@link MessageFrame} POJO.</li>
 *   <li>Merge with the original {@link OdeMessageFrameMetadata}.</li>
 *   <li>Publish the resulting {@code OdeMessageFrameData} JSON directly to the appropriate
 *       {@code topic.Ode{Type}Json} topic.</li>
 * </ol>
 *
 * <p>Active when {@code ode.ffmlib.enabled=true}.
 */
@Slf4j
@Service
@ConditionalOnProperty(name = "ode.ffmlib.enabled", havingValue = "true")
public class FfmlibDecodeService implements RawEncodedDecodeService {

  private final FfmlibMessageFrameCodec ffmlibCodec;
  private final JsonTopics jsonTopics;
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final XmlMapper simpleXmlMapper;

  public FfmlibDecodeService(
      FfmlibMessageFrameCodec ffmlibCodec,
      JsonTopics jsonTopics,
      KafkaTemplate<String, String> kafkaTemplate,
      @Qualifier("simpleXmlMapper") XmlMapper simpleXmlMapper) {
    this.ffmlibCodec = ffmlibCodec;
    this.jsonTopics = jsonTopics;
    this.kafkaTemplate = kafkaTemplate;
    this.simpleXmlMapper = simpleXmlMapper;
  }

  @Override
  public void decode(OdeAsn1Data asn1Data, String key) {
    try {
      // Extract raw UPER bytes (stored as hex string in OdeHexByteArray)
      OdeHexByteArray hexBytes = (OdeHexByteArray) asn1Data.getPayload().getData();
      byte[] uperBytes = CodecUtils.fromHex(hexBytes.getBytes());

      // Decode UPER → XER via FFMLib (no Kafka round-trip needed)
      long decodeStart = System.nanoTime();
      String xer = ffmlibCodec.uperToXer(uperBytes);
      long asnDecodeLatencyMs = (System.nanoTime() - decodeStart) / 1_000_000;
      log.debug("FFMLib decoded XER for key {} in {}ms: {}", key, asnDecodeLatencyMs, xer);

      // Deserialize XER to typed MessageFrame POJO
      MessageFrame<?> messageFrame = simpleXmlMapper.readValue(xer, MessageFrame.class);

      // Reuse the OdeMessageFrameMetadata already populated by the router's
      // RawEncodedJsonService call, mirroring the post-processing in
      // OdeMessageFrameDataCreatorHelper.
      OdeMessageFrameMetadata metadata = (OdeMessageFrameMetadata) asn1Data.getMetadata();
      metadata.setAsnDecodeLatencyMs(asnDecodeLatencyMs);
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
      String json = JsonUtils.toJson(frameData, false);

      DSRCmsgID msgId = messageFrame.getMessageId();
      String messageName = msgId.name().orElse("Unknown");
      String topic = resolveJsonTopic(messageName);
      if (topic == null) {
        log.warn("FFMLib decode: no topic mapped for message type '{}', key {} dropped.", messageName, key);
        return;
      }

      log.debug("FFMLib decode publishing to topic '{}' for key '{}'", topic, key);
      kafkaTemplate.send(topic, key, json);

    } catch (JsonProcessingException e) {
      log.error("FFMLib decode failed (JSON processing) for key {}: {}", key, e.getMessage(), e);
    } catch (ClassCastException e) {
      log.error("FFMLib decode failed (unexpected payload type) for key {}: {}", key, e.getMessage(), e);
    } catch (Exception e) {
      log.error("FFMLib decode unexpected error for key {}: {}", key, e.getMessage(), e);
    }
  }

  private String resolveJsonTopic(String messageName) {
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
}
