package io.kafbat.ui.serde.sbe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kafbat.ui.serde.api.DeserializeResult;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.Map;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Runs in a JVM without {@code --add-exports java.base/jdk.internal.misc=ALL-UNNAMED}, as kafka-ui does,
 * so the message is a fixture rather than encoded here with Agrona's buffers.
 */
@Tag("no-unsafe")
class NoUnsafeDecodingTest {

  /**
   * The Order that {@code TestMessages.order(2)} encodes with the generated codecs.
   */
  private static final String ORDER_HEX = "3f0001002a000200feffffffffffffff4254432d555344004101053930000000000000fe"
      + "00000000000000800001fb000000000000000000000000000000030000008007000000220001006300000000000000010f0000"
      + "000000000000000000000000000156454e55453100000900000068656c6c6f20e29c93";

  @Test
  void agronaUnsafeReallyIsUnavailableHere() {
    assertThrows(Throwable.class, ExpandableArrayBuffer::new);
  }

  @Test
  void decodesWithoutUnsafe() throws Exception {
    final SbeSerde serde = new SbeSerde();
    final MapPropertyResolver empty = new MapPropertyResolver(Map.of());
    final Path schema = Path.of(getClass().getResource("/sbe/trading.xml").toURI());
    serde.configure(new MapPropertyResolver(Map.of(
        "schemaFiles", schema.toString(),
        "decimalConventions", "fix,bigDecimal")), empty, empty);

    final DeserializeResult result = serde.deserialize(HexFormat.of().parseHex(ORDER_HEX));

    assertEquals(DeserializeResult.Type.JSON, result.getType(), String.valueOf(result.getAdditionalProperties()));
    final JsonNode json = new ObjectMapper().readTree(result.getResult());
    assertEquals("Order", json.get("$message").asText());
    assertEquals("123.45", json.get("price").asText());
    assertEquals("-0.005", json.get("quantity").asText());
    assertEquals("1.5", json.get("fills").get(0).get("fill").get("price").asText());
    assertEquals("hello ✓", json.get("note").asText());
  }
}
