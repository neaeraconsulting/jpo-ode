package us.dot.its.jpo.ode.udp.portmapped;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class PortMappedIngestConfig {

  private List<PortMappedIngestSource> sources;

  @Data
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class PortMappedIngestSource {

    @JsonProperty("intersection_name")
    private String intersectionName;

    @JsonProperty("intersection_id")
    private String intersectionId;

    @JsonProperty("origin_ip")
    private String originIp;

    private int port;

    private String type;
  }
}
