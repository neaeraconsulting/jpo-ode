package us.dot.its.jpo.ode.codec.ffmlib;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.ode.model.OdeAsn1Data;
import us.dot.its.jpo.ode.model.OdeMsgMetadata;
import us.dot.its.jpo.ode.model.OdeMsgPayload;
import us.dot.its.jpo.ode.util.CodecUtils;
import us.dot.its.jpo.ode.util.JsonUtils;
import us.dot.its.jpo.ode.util.XmlUtils;
import us.dot.its.jpo.ode.util.XmlUtils.XmlUtilsException;

/**
 * In-process replacement for the external asn1_codec encoder microservice.
 *
 * <p>Listens on {@code topic.Asn1EncoderInput} (the same topic the external encoder reads),
 * encodes the contained J2735 MessageFrame from XER to UPER using the FFMLib, and publishes
 * the result to {@code topic.Asn1EncoderOutput} in the exact format that
 * {@link us.dot.its.jpo.ode.kafka.listeners.asn1.Asn1EncodedDataRouter} expects.
 * This leaves the signing, RSU deposition, and SDX delivery logic entirely unchanged.
 *
 * <p>Active (and exclusively responsible for encoding) when {@code ode.ffmlib.enabled=true}.
 * When the flag is off this bean is not created and the external asn1_codec handles encoding.
 *
 * <p><strong>ASD (AdvisorySituationData) note:</strong> The second TIM encoding round that
 * packages a signed TIM inside an ASD structure is also handled here when the payload data
 * contains an {@code AdvisorySituationData} node; however, the FFMLib {@code xerToUper} call
 * operates on that node's XER content directly. If the FFMLib does not support ASD encoding,
 * the second round will fail and SDX delivery will be unavailable while in FFMLib mode.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "ode.ffmlib.enabled", havingValue = "true")
public class FfmlibEncoderBridge {

  private static final String MESSAGE_FRAME = "MessageFrame";
  private static final String ADVISORY_SITUATION_DATA = "AdvisorySituationData";
  private static final String BYTES = "bytes";

  private final FfmlibMessageFrameCodec ffmlibCodec;
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final String encoderOutputTopic;

  public FfmlibEncoderBridge(
      FfmlibMessageFrameCodec ffmlibCodec,
      KafkaTemplate<String, String> kafkaTemplate,
      @Value("${ode.kafka.topics.asn1.encoder-output}") String encoderOutputTopic) {
    this.ffmlibCodec = ffmlibCodec;
    this.kafkaTemplate = kafkaTemplate;
    this.encoderOutputTopic = encoderOutputTopic;
  }

  /**
   * Consumes an encoder-input message, encodes the J2735 payload via FFMLib, and publishes
   * the encoded result to the encoder-output topic.
   */
  @KafkaListener(id = "FfmlibEncoderBridge", topics = "${ode.kafka.topics.asn1.encoder-input}")
  public void encode(ConsumerRecord<String, String> record) {
    log.debug("FfmlibEncoderBridge received encoder input, key={}", record.key());
    try {
      JSONObject inputObj = XmlUtils.toJSONObject(record.value())
          .getJSONObject(OdeAsn1Data.class.getSimpleName());

      JSONObject metadata = inputObj.getJSONObject(OdeMsgMetadata.METADATA_STRING);
      JSONObject payloadData = inputObj.getJSONObject(OdeMsgPayload.PAYLOAD_STRING)
          .getJSONObject(OdeMsgPayload.DATA_STRING);

      String encodedHex;
      String resultDataKey;

      if (payloadData.has(ADVISORY_SITUATION_DATA)) {
        // Second encoding round: ASD wrapping a signed TIM
        String asdXer = XmlUtils.findXmlContentString(record.value(), ADVISORY_SITUATION_DATA);
        if (asdXer == null) {
          log.error("FfmlibEncoderBridge: AdvisorySituationData XML not found in encoder input, key={}", record.key());
          return;
        }
        byte[] uper = ffmlibCodec.xerToUper(asdXer);
        encodedHex = CodecUtils.toHex(uper);
        resultDataKey = ADVISORY_SITUATION_DATA;
        log.debug("FfmlibEncoderBridge encoded ASD ({} bytes), key={}", uper.length, record.key());
      } else if (payloadData.has(MESSAGE_FRAME)) {
        // First encoding round: plain MessageFrame
        String messageFrameXer = XmlUtils.findXmlContentString(record.value(), MESSAGE_FRAME);
        if (messageFrameXer == null) {
          log.error("FfmlibEncoderBridge: MessageFrame XML not found in encoder input, key={}", record.key());
          return;
        }
        byte[] uper = ffmlibCodec.xerToUper(messageFrameXer);
        encodedHex = CodecUtils.toHex(uper);
        resultDataKey = MESSAGE_FRAME;
        log.debug("FfmlibEncoderBridge encoded MessageFrame ({} bytes), key={}", uper.length, record.key());
      } else {
        log.warn("FfmlibEncoderBridge: encoder input has neither MessageFrame nor AdvisorySituationData, key={}", record.key());
        return;
      }

      String outputXml = buildEncoderOutputXml(metadata, resultDataKey, encodedHex);
      kafkaTemplate.send(encoderOutputTopic, record.key(), outputXml);

    } catch (XmlUtilsException e) {
      log.error("FfmlibEncoderBridge XML parsing error for key {}: {}", record.key(), e.getMessage(), e);
    } catch (Exception e) {
      log.error("FfmlibEncoderBridge unexpected error for key {}: {}", record.key(), e.getMessage(), e);
    }
  }

  /**
   * Constructs the encoder output XML in the format expected by {@code Asn1EncodedDataRouter}:
   * <pre>{@code
   * <OdeAsn1Data>
   *   <metadata>...(original metadata)...</metadata>
   *   <payload><data><{dataKey}><bytes>hexBytes</bytes></{dataKey}></data></payload>
   * </OdeAsn1Data>
   * }</pre>
   */
  private String buildEncoderOutputXml(JSONObject metadata, String dataKey, String hexBytes)
      throws XmlUtilsException {
    ObjectNode metadataNode = JsonUtils.toObjectNode(metadata.toString());

    ObjectNode bytesNode = JsonUtils.newNode();
    bytesNode.put(BYTES, hexBytes);

    ObjectNode dataKeyNode = JsonUtils.newNode();
    dataKeyNode.set(dataKey, bytesNode);

    ObjectNode dataNode = JsonUtils.newNode();
    dataNode.set(OdeMsgPayload.DATA_STRING, dataKeyNode);

    ObjectNode payloadNode = JsonUtils.newNode();
    payloadNode.set(OdeMsgPayload.DATA_STRING, dataKeyNode);

    ObjectNode message = JsonUtils.newNode();
    message.set(OdeMsgMetadata.METADATA_STRING, metadataNode);
    message.set(OdeMsgPayload.PAYLOAD_STRING, payloadNode);

    ObjectNode root = JsonUtils.newNode();
    root.set(OdeAsn1Data.ODE_ASN1_DATA, message);

    String xml = XmlUtils.toXmlStatic(root);
    return xml.replace("<ObjectNode>", "").replace("</ObjectNode>", "");
  }
}
