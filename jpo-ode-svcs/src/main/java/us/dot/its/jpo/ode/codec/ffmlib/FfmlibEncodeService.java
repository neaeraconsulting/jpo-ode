package us.dot.its.jpo.ode.codec.ffmlib;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.stereotype.Service;
import us.dot.its.jpo.ode.model.OdeAsn1Data;
import us.dot.its.jpo.ode.model.OdeMsgMetadata;
import us.dot.its.jpo.ode.model.OdeMsgPayload;
import us.dot.its.jpo.ode.util.CodecUtils;
import us.dot.its.jpo.ode.util.JsonUtils;
import us.dot.its.jpo.ode.util.XmlUtils;
import us.dot.its.jpo.ode.util.XmlUtils.XmlUtilsException;

/**
 * In-process J2735 XER→UPER encode service backed by FFMLib.
 *
 * <p>Replaces the former Kafka hop through {@code topic.Asn1EncoderInput} /
 * {@code topic.Asn1EncoderOutput} (and the external asn1_codec AEM process).
 */
@Slf4j
@Service
public class FfmlibEncodeService {

  private static final String MESSAGE_FRAME = "MessageFrame";
  private static final String ADVISORY_SITUATION_DATA = "AdvisorySituationData";
  private static final String BYTES = "bytes";

  private final FfmlibMessageFrameCodec ffmlibCodec;

  public FfmlibEncodeService(FfmlibMessageFrameCodec ffmlibCodec) {
    this.ffmlibCodec = ffmlibCodec;
  }

  /**
   * Encodes an {@code OdeAsn1Data} XML document (MessageFrame or AdvisorySituationData payload)
   * to the encoder-output XML shape expected by {@code Asn1EncodedDataRouter}.
   *
   * @param odeAsn1Xml encoder-input style XML ({@code <OdeAsn1Data>...})
   * @return encoder-output style XML with hex UPER bytes in the payload
   */
  public String encodeOdeAsn1Xml(String odeAsn1Xml) throws Exception {
    JSONObject inputObj = XmlUtils.toJSONObject(odeAsn1Xml)
        .getJSONObject(OdeAsn1Data.class.getSimpleName());

    JSONObject metadata = inputObj.getJSONObject(OdeMsgMetadata.METADATA_STRING);
    JSONObject payloadData = inputObj.getJSONObject(OdeMsgPayload.PAYLOAD_STRING)
        .getJSONObject(OdeMsgPayload.DATA_STRING);

    String encodedHex;
    String resultDataKey;

    if (payloadData.has(ADVISORY_SITUATION_DATA)) {
      String asdXer = XmlUtils.findXmlContentString(odeAsn1Xml, ADVISORY_SITUATION_DATA);
      if (asdXer == null) {
        throw new IllegalArgumentException("AdvisorySituationData XML not found in encode input");
      }
      byte[] uper = ffmlibCodec.xerToUper(asdXer);
      encodedHex = CodecUtils.toHex(uper);
      resultDataKey = ADVISORY_SITUATION_DATA;
      log.debug("FFMLib encoded ASD ({} bytes)", uper.length);
    } else if (payloadData.has(MESSAGE_FRAME)) {
      String messageFrameXer = XmlUtils.findXmlContentString(odeAsn1Xml, MESSAGE_FRAME);
      if (messageFrameXer == null) {
        throw new IllegalArgumentException("MessageFrame XML not found in encode input");
      }
      byte[] uper = ffmlibCodec.xerToUper(messageFrameXer);
      encodedHex = CodecUtils.toHex(uper);
      resultDataKey = MESSAGE_FRAME;
      log.debug("FFMLib encoded MessageFrame ({} bytes)", uper.length);
    } else {
      throw new IllegalArgumentException(
          "Encode input has neither MessageFrame nor AdvisorySituationData");
    }

    return buildEncoderOutputXml(metadata, resultDataKey, encodedHex);
  }

  private String buildEncoderOutputXml(JSONObject metadata, String dataKey, String hexBytes)
      throws XmlUtilsException, JsonUtils.JsonUtilsException {
    ObjectNode metadataNode = JsonUtils.toObjectNode(metadata.toString());

    ObjectNode bytesNode = JsonUtils.newNode();
    bytesNode.put(BYTES, hexBytes);

    ObjectNode dataKeyNode = JsonUtils.newNode();
    dataKeyNode.set(dataKey, bytesNode);

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
