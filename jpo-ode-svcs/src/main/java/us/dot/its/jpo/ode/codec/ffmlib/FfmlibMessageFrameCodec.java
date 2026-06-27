package us.dot.its.jpo.ode.codec.ffmlib;

import j2735ffm.MessageFrameCodec;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Spring-managed wrapper around the FFMLib {@link MessageFrameCodec}.
 * Created only when {@code ode.ffmlib.enabled=true}.
 *
 * <p>One native codec instance per thread is maintained via {@link ThreadLocal}
 * to avoid contention on the underlying JNI/FFM state.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "ode.ffmlib.enabled", havingValue = "true")
public class FfmlibMessageFrameCodec {

  private static final long ENCODE_BUFFER_SIZE = 262144L;
  private static final long DECODE_BUFFER_SIZE = 8192L;
  private static final long STRING_CACHE_SIZE = 512L;

  private final ThreadLocal<MessageFrameCodec> codec;

  public FfmlibMessageFrameCodec(FfmlibProperties properties) {
    Path libPath = resolveLibraryPath(properties.getNativeLibraryPath());
    log.info("FFMLib MessageFrameCodec will load native library from: {}", libPath.toAbsolutePath());
    codec = ThreadLocal.withInitial(() -> {
      try {
        return new MessageFrameCodec(ENCODE_BUFFER_SIZE, DECODE_BUFFER_SIZE, STRING_CACHE_SIZE, libPath);
      } catch (RuntimeException ex) {
        throw ex;
      } catch (Exception ex) {
        throw new IllegalStateException("Unable to initialize FFMLib MessageFrameCodec", ex);
      }
    });
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
}
