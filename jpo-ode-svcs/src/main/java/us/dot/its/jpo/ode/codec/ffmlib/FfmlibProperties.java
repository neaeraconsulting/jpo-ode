package us.dot.its.jpo.ode.codec.ffmlib;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration properties for the FFMLib in-process ASN.1 codec.
 * When {@code enabled} is true, the ODE bypasses the external asn1_codec microservice
 * and uses the FFMLib MessageFrameCodec directly for UPER encode/decode operations.
 */
@Configuration
@ConfigurationProperties(prefix = "ode.ffmlib")
@Data
public class FfmlibProperties {

  /**
   * When true, the FFMLib is used for in-process UPER decode and encode,
   * bypassing the Asn1DecoderInput/Asn1DecoderOutput Kafka topics for decoding
   * and the external asn1_codec container for encoding.
   * Defaults to false for backwards compatibility.
   */
  private boolean enabled = false;

  /**
   * Explicit path to the native shared library (asnapplication.dll / libasnapplication.so).
   * If blank, the library is auto-detected from the working directory, target/libs/, or libs/.
   */
  private String nativeLibraryPath = "";
}
