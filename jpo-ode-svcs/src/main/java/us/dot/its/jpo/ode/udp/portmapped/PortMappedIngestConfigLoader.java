package us.dot.its.jpo.ode.udp.portmapped;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.kafka.core.KafkaTemplate;
import us.dot.its.jpo.ode.kafka.topics.RawEncodedJsonTopics;
import us.dot.its.jpo.ode.udp.AbstractUdpReceiverPublisher;
import us.dot.its.jpo.ode.udp.bsm.BsmReceiver;
import us.dot.its.jpo.ode.udp.controller.UDPReceiverProperties;
import us.dot.its.jpo.ode.udp.controller.UDPReceiverProperties.ReceiverProperties;
import us.dot.its.jpo.ode.udp.generic.GenericReceiver;
import us.dot.its.jpo.ode.udp.map.MapReceiver;
import us.dot.its.jpo.ode.udp.psm.PsmReceiver;
import us.dot.its.jpo.ode.udp.rsm.RsmReceiver;
import us.dot.its.jpo.ode.udp.rtcm.RtcmReceiver;
import us.dot.its.jpo.ode.udp.sdsm.SdsmReceiver;
import us.dot.its.jpo.ode.udp.spat.SpatReceiver;
import us.dot.its.jpo.ode.udp.srm.SrmReceiver;
import us.dot.its.jpo.ode.udp.ssm.SsmReceiver;
import us.dot.its.jpo.ode.udp.tim.TimReceiver;

@Slf4j
public class PortMappedIngestConfigLoader {

  public static final String CONFIG_FILE = "configurable_ingest_config.json";

  private final ObjectMapper objectMapper = new ObjectMapper();

  public Optional<PortMappedIngestConfig> loadConfig() {
    Resource resource = new ClassPathResource(CONFIG_FILE);
    if (!resource.exists()) {
      log.info("Configurable ingest config not found at classpath:{}, skipping configurable UDP ingest.",
          CONFIG_FILE);
      return Optional.empty();
    }

    try (InputStream inputStream = resource.getInputStream()) {
      return Optional.of(objectMapper.readValue(inputStream, PortMappedIngestConfig.class));
    } catch (IOException e) {
      log.warn("Failed to read configurable ingest config from classpath:{}, skipping configurable UDP ingest.",
          CONFIG_FILE, e);
      return Optional.empty();
    }
  }

  public List<AbstractUdpReceiverPublisher> loadReceivers(UDPReceiverProperties udpProps,
      RawEncodedJsonTopics rawEncodedJsonTopics, KafkaTemplate<String, String> kafkaTemplate) {
        log.debug("Loading configurable UDP receivers from config...");

    
    Optional<PortMappedIngestConfig> configOptional = loadConfig();
    if (configOptional.isEmpty()) {
      return Collections.emptyList();
    }

    PortMappedIngestConfig config = configOptional.get();
    if (config.getSources() == null || config.getSources().isEmpty()) {
      return Collections.emptyList();
    }

    List<AbstractUdpReceiverPublisher> receivers = new ArrayList<>();
    for (PortMappedIngestConfig.PortMappedIngestSource source : config.getSources()) {

      String type = normalizeType(source.getType());
      if (type == null) {
        System.out.println("Skipping configurable UDP ingest source with missing or invalid type. Source: " + source);
        log.warn("Skipping configurable UDP ingest source with missing type. Source: {}", source);
        continue;
      }else{
        source.setType(type);
      }

      PortMappedConfigurableReceiver receiver = new PortMappedConfigurableReceiver(
          buildReceiverProperties(udpProps, source),
          kafkaTemplate,
          rawEncodedJsonTopics,
          source
      );
      receivers.add(receiver);
    }
    return receivers;
  }

  private String normalizeType(String type) {
    if (type == null || type.isBlank()) {
      return null;
    }

    return type.trim().toUpperCase(Locale.ROOT);
  }

  private ReceiverProperties buildReceiverProperties(UDPReceiverProperties udpProps,PortMappedIngestConfig.PortMappedIngestSource source) {
    ReceiverProperties baseProps = switch (source.getType()) {
      case "BSM" -> udpProps.getBsm();
      case "TIM" -> udpProps.getTim();
      case "SSM" -> udpProps.getSsm();
      case "SRM" -> udpProps.getSrm();
      case "SPAT" -> udpProps.getSpat();
      case "MAP" -> udpProps.getMap();
      case "PSM" -> udpProps.getPsm();
      case "SDSM" -> udpProps.getSdsm();
      case "RTCM" -> udpProps.getRtcm();
      case "RSM" -> udpProps.getRsm();
      case "GENERIC" -> udpProps.getGeneric();
      default -> null;
    };

    if (baseProps == null) {
      return null;
    }

    ReceiverProperties props = new ReceiverProperties();
    props.setBufferSize(baseProps.getBufferSize());
    props.setReceiverPort(source.getPort());
    return props;
  }
}
