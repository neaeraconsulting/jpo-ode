package us.dot.its.jpo.ode.codec.ffmlib;

import us.dot.its.jpo.ode.model.OdeAsn1Data;

/**
 * Strategy interface for processing raw ASN.1 encoded ODE messages after header stripping
 * and UPER encoding metadata has been applied.
 *
 * <p>Two implementations exist:
 * <ul>
 *   <li>{@link KafkaAsn1CoderDecodeService} — default; publishes to the Asn1DecoderInput
 *       Kafka topic for the external asn1_codec microservice to consume.</li>
 *   <li>{@link FfmlibDecodeService} — active when {@code ode.ffmlib.enabled=true}; decodes
 *       in-process using the FFMLib and publishes directly to the appropriate ODE JSON topic.</li>
 * </ul>
 */
public interface RawEncodedDecodeService {

  /**
   * Decodes the given ASN.1 data and routes the resulting ODE message frame JSON to the
   * appropriate downstream topic.
   *
   * @param asn1Data the prepared ASN.1 message (header stripped, UPER encoding metadata set)
   * @param key      the Kafka record key to preserve through the pipeline
   */
  void decode(OdeAsn1Data asn1Data, String key);
}
