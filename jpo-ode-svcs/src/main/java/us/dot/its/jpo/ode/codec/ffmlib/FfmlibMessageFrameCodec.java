package us.dot.its.jpo.ode.codec.ffmlib;

import j2735ffm.AsnEncoding;
import j2735ffm.MessageFrameCodec;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Spring-managed wrapper around the FFMLib {@link MessageFrameCodec}.
 *
 * <p>One native codec instance per thread is maintained via {@link ThreadLocal}
 * to avoid contention on the underlying FFM state.
 *
 * <p>Buffer sizes are configurable via {@link FfmlibProperties}. Both encode and decode need a
 * large {@code textBufferSize} (XER/JER text), so decode-only threads cannot drop the large text
 * buffer — the former "ENCODE_BUFFER_SIZE" name was misleading. {@code uperBufferSize} stays
 * modest because UPER is compact.
 */
@Slf4j
@Component
public class FfmlibMessageFrameCodec {

  private final Path libPath;
  private final long textBufferSize;
  private final long uperBufferSize;
  private final long errorBufferSize;
  private final boolean preferJer;
  private final ThreadLocal<MessageFrameCodec> codec;

  public FfmlibMessageFrameCodec(FfmlibProperties properties) {
    this.libPath = resolveLibraryPath(properties.getNativeLibraryPath());
    this.textBufferSize = properties.getTextBufferSize();
    this.uperBufferSize = properties.getUperBufferSize();
    this.errorBufferSize = properties.getErrorBufferSize();
    this.preferJer = resolvePreferJer(properties.getIntermediateEncoding());
    log.info(
        "FFMLib MessageFrameCodec will load native library from: {} "
            + "(textBufferSize={}, uperBufferSize={}, errorBufferSize={}, preferJer={})",
        libPath.toAbsolutePath(), textBufferSize, uperBufferSize, errorBufferSize, preferJer);
    codec = ThreadLocal.withInitial(this::newCodec);
  }

  private MessageFrameCodec newCodec() {
    try {
      return new MessageFrameCodec(textBufferSize, uperBufferSize, errorBufferSize, libPath);
    } catch (RuntimeException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new IllegalStateException("Unable to initialize FFMLib MessageFrameCodec", ex);
    }
  }

  /**
   * Decodes UPER-encoded bytes to an intermediate text encoding (JER when supported and
   * configured, otherwise XER).
   *
   * @param uperBytes raw UPER-encoded message bytes (header already stripped)
   * @return decode result with text and encoding used
   */
  public IntermediateDecodeResult uperToIntermediate(byte[] uperBytes) {
    if (preferJer && AsnEncoding.JER.isSupported()) {
      byte[] jerBytes = codec.get().convertGeneral(
          uperBytes, MessageFrameCodec.MESSAGE_FRAME_PDU, AsnEncoding.UPER, AsnEncoding.JER);
      return new IntermediateDecodeResult(
          new String(jerBytes, StandardCharsets.UTF_8), IntermediateEncoding.JER);
    }
    return new IntermediateDecodeResult(codec.get().uperToXer(uperBytes), IntermediateEncoding.XER);
  }

  /**
   * Decodes UPER-encoded bytes to XER (XML) representation of a J2735 MessageFrame.
   *
   * @param uperBytes raw UPER-encoded message bytes (header already stripped)
   * @return XER XML string containing the decoded MessageFrame
   */
  public String uperToXer(byte[] uperBytes) {
    return codec.get().uperToXer(uperBytes);
  }

  /**
   * Encodes an XER (XML) representation of a J2735 MessageFrame to UPER bytes.
   *
   * @param xer XER XML string containing the MessageFrame to encode
   * @return UPER-encoded byte array
   */
  public byte[] xerToUper(String xer) {
    return codec.get().xerToUper(xer);
  }

  boolean isPreferJer() {
    return preferJer;
  }

  private static boolean resolvePreferJer(String intermediateEncoding) {
    if (intermediateEncoding == null || intermediateEncoding.isBlank()
        || "auto".equalsIgnoreCase(intermediateEncoding)) {
      boolean jerSupported = AsnEncoding.JER.isSupported();
      if (!jerSupported) {
        log.info(
            "FFMLib JER is not supported by this native library build "
                + "(AsnEncoding.JER.isSupported()=false); using XER intermediate encoding. "
                + "When a future j2735-2024-ffm-lib enables JER, set ode.ffmlib.intermediate-encoding=auto "
                + "to pick it up automatically.");
      }
      return jerSupported;
    }
    if ("jer".equalsIgnoreCase(intermediateEncoding)) {
      if (!AsnEncoding.JER.isSupported()) {
        log.warn(
            "ode.ffmlib.intermediate-encoding=jer requested but JER is not supported by the "
                + "native library; falling back to XER");
        return false;
      }
      return true;
    }
    return false;
  }

  private static Path resolveLibraryPath(String configuredPath) {
    if (configuredPath != null && !configuredPath.isBlank()) {
      return Paths.get(configuredPath);
    }
    String osName = System.getProperty("os.name", "").toLowerCase();
    String fileName = osName.contains("win") ? "asnapplication.dll" : "libasnapplication.so";
    Path[] candidates = {
        Paths.get(fileName),
        Paths.get("target", "libs", fileName),
        Paths.get("libs", fileName)
    };
    for (Path candidate : candidates) {
      if (Files.exists(candidate)) {
        log.debug("Found FFMLib native library at: {}", candidate.toAbsolutePath());
        return candidate;
      }
    }
    log.warn("FFMLib native library '{}' not found in standard locations; using default path '{}'",
        fileName, candidates[0]);
    return candidates[0];
  }

  /** Intermediate text encoding produced by native UPER decode. */
  public enum IntermediateEncoding {
    XER,
    JER
  }

  /**
   * Result of native UPER → text conversion.
   *
   * @param text decoded XER or JER text
   * @param encoding which encoding {@code text} uses
   */
  public record IntermediateDecodeResult(String text, IntermediateEncoding encoding) {
  }
}
