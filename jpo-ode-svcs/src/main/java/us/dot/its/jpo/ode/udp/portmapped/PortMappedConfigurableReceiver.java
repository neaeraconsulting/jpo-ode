package us.dot.its.jpo.ode.udp.portmapped;

import io.netty.handler.codec.UnsupportedMessageTypeException;
import java.net.DatagramPacket;
import lombok.extern.slf4j.Slf4j;
import org.apache.tomcat.util.buf.HexUtils;
import org.springframework.kafka.core.KafkaTemplate;
import us.dot.its.jpo.ode.kafka.topics.RawEncodedJsonTopics;
import us.dot.its.jpo.ode.udp.AbstractUdpReceiverPublisher;
import us.dot.its.jpo.ode.udp.InvalidPayloadException;
import us.dot.its.jpo.ode.udp.UdpHexDecoder;
import us.dot.its.jpo.ode.udp.controller.UDPReceiverProperties.ReceiverProperties;

/**
 * GenericReceiver is a class that listens for UDP packets and processes them based on the
 * determined message type. It extends AbstractUdpReceiverPublisher to take advantage of the
 * runnable interface for running the receiver service in a separate thread.
 *
 * </p>The class is designed to handle all {@link us.dot.its.jpo.ode.uper.SupportedMessageType}
 * message types encoded in UDP packets such as and routes them to the appropriate Kafka topic.
 */
@Slf4j
public class PortMappedConfigurableReceiver extends AbstractUdpReceiverPublisher {

  private final KafkaTemplate<String, String> publisher;
  private final RawEncodedJsonTopics rawEncodedJsonTopics;
  private final PortMappedIngestConfig.PortMappedIngestSource ingestConfig;

  /**
   * Constructs a new GenericReceiver with the specified properties, Kafka template, and raw encoded
   * JSON topics.
   *
   * @param props                the receiver properties containing configuration settings such as
   *                             port and buffer size
   * @param kafkaTemplate        the KafkaTemplate used for publishing messages
   * @param rawEncodedJsonTopics the configuration object containing the topics used to publish
   *                             messages
   * @param ingestConfig         the configuration object containing the ingest settings
   */
    public PortMappedConfigurableReceiver(ReceiverProperties props, KafkaTemplate<String, String> kafkaTemplate,
      RawEncodedJsonTopics rawEncodedJsonTopics, PortMappedIngestConfig.PortMappedIngestSource ingestConfig) {
    super(props.getReceiverPort(), props.getBufferSize());

    this.publisher = kafkaTemplate;
    this.rawEncodedJsonTopics = rawEncodedJsonTopics;
    this.ingestConfig = ingestConfig;
  }

  @Override
  public void run() {
    log.debug("");

    byte[] buffer;
    do {
      buffer = new byte[bufferSize];
      // packet should be recreated on each loop to prevent latent data in buffer
      DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
      try {
        System.out.println("Waiting for UDP "+this.ingestConfig.getPort()+ " packets for type "+this.ingestConfig.getType()+" and intersection "+this.ingestConfig.getIntersectionId()+"...");
        socket.receive(packet);
        byte[] payload = packet.getData();
        if ((packet.getLength() <= 0) || (payload == null)) {
          log.debug("Skipping empty payload");
          continue;
        }

        senderIp = this.ingestConfig.getOriginIp();
        senderPort = packet.getPort();
        log.debug("Packet received from {}:{}", senderIp, senderPort);

        String payloadHexString = HexUtils.toHexString(payload).toLowerCase();
        log.debug("Raw Payload {}", payloadHexString);

        routeMessageByMessageType(this.ingestConfig.getType(), packet);

      } catch (UnsupportedMessageTypeException e) {
        log.error("Unsupported Message Type", e);
      } catch (InvalidPayloadException e) {
        log.error("Error decoding packet", e);
      } catch (Exception e) {
        log.error("Error receiving packet", e);
      }
    } while (!isStopped());
  }

  private void routeMessageByMessageType(
      String messageType,
      DatagramPacket packet
  ) throws InvalidPayloadException, UnsupportedMessageTypeException {
    log.debug("Detected Message Type {}", messageType);
    switch (messageType) {
      case "MAP" -> {
        String mapJson = UdpHexDecoder.buildJsonMapFromPacket(packet);
        log.debug("Sending Data to Topic {}", mapJson);
        if (mapJson != null) {
          publisher.send(rawEncodedJsonTopics.getMap(), mapJson);
        }
      }
      case "SPAT" -> {
        String spatJson = UdpHexDecoder.buildJsonSpatFromPacket(packet);
        if (spatJson != null) {
          publisher.send(rawEncodedJsonTopics.getSpat(), spatJson);
        }
      }
      case "TIM" -> {
        String timJson = UdpHexDecoder.buildJsonTimFromPacket(packet);
        if (timJson != null) {
          publisher.send(rawEncodedJsonTopics.getTim(), timJson);
        }
      }
      case "BSM" -> {
        String bsmJson = UdpHexDecoder.buildJsonBsmFromPacket(packet);
        if (bsmJson != null) {
          publisher.send(rawEncodedJsonTopics.getBsm(), bsmJson);
        }
      }
      case "SSM" -> {
        String ssmJson = UdpHexDecoder.buildJsonSsmFromPacket(packet);
        if (ssmJson != null) {
          publisher.send(rawEncodedJsonTopics.getSsm(), ssmJson);
        }
      }
      case "SRM" -> {
        String srmJson = UdpHexDecoder.buildJsonSrmFromPacket(packet);
        if (srmJson != null) {
          publisher.send(rawEncodedJsonTopics.getSrm(), srmJson);
        }
      }
      case "PSM" -> {
        String psmJson = UdpHexDecoder.buildJsonPsmFromPacket(packet);
        if (psmJson != null) {
          publisher.send(rawEncodedJsonTopics.getPsm(), psmJson);
        }
      }
      case "SDSM" -> {
        String sdsmJson = UdpHexDecoder.buildJsonSdsmFromPacket(packet);
        if (sdsmJson != null) {
          publisher.send(rawEncodedJsonTopics.getSdsm(), sdsmJson);
        }
      }
      case "RTCM" -> {
        String rtcmJson = UdpHexDecoder.buildJsonRtcmFromPacket(packet);
        if (rtcmJson != null) {
          publisher.send(rawEncodedJsonTopics.getRtcm(), rtcmJson);
        }
      }
      case "RSM" -> {
        String rsmJson = UdpHexDecoder.buildJsonRsmFromPacket(packet);
        if (rsmJson != null) {
          publisher.send(rawEncodedJsonTopics.getRsm(), rsmJson);
        }
      }
      default -> throw new UnsupportedMessageTypeException(messageType);
    }
  }
}
