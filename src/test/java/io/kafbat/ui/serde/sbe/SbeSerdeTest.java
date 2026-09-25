package io.kafbat.ui.serde.sbe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kafbat.ui.serde.api.DeserializeResult;
import io.kafbat.ui.serde.api.Serde;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;

class SbeSerdeTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void decodesEveryFieldTypeWithBothDecimalConventions() throws Exception {
    final DeserializeResult result = serde(Map.of("decimalConventions", "fix,bigDecimal"))
        .deserialize(TestMessages.order(2));

    assertEquals(DeserializeResult.Type.JSON, result.getType());
    final JsonNode json = MAPPER.readTree(result.getResult());
    assertEquals("Order", json.get("$message").asText());
    assertEquals(new BigInteger("18446744073709551614"), json.get("orderId").bigIntegerValue());
    assertEquals("BTC-USD", json.get("symbol").asText());
    assertEquals("ASK", json.get("side").asText());
    assertEquals("FILLED", json.get("status").asText());
    assertEquals(List.of("hidden", "postOnly"), strings(json.get("flags")));
    assertEquals("123.45", json.get("price").asText());
    assertTrue(json.get("stopPrice").isNull());
    assertEquals("-0.005", json.get("quantity").asText());
    assertTrue(json.get("displayQuantity").isNull());
    assertEquals("TEST", json.get("source").asText());
    assertEquals(7, json.get("tag").asInt());

    final JsonNode fill = json.get("fills").get(0);
    assertEquals(99, fill.get("executionId").asLong());
    assertEquals("1.5", fill.get("fill").get("price").asText());
    assertEquals("VENUE1", fill.get("fill").get("venue").asText());
    assertEquals(List.of("price", "venue"), fieldNames(fill.get("fill")));
    assertEquals("hello ✓", json.get("note").asText());

    assertEquals(Map.of("sbeMessage", "Order", "sbeSchemaId", 42, "sbeTemplateId", 1, "sbeVersion", 2),
        result.getAdditionalProperties());
  }

  @Test
  void fixDecimalsAreConvertedByDefault() throws Exception {
    final JsonNode json = MAPPER.readTree(serde(Map.of()).deserialize(TestMessages.order(2)).getResult());

    assertEquals("123.45", json.get("price").asText());
    assertTrue(json.get("quantity").isObject(), "bigDecimal is opt-in");
  }

  @Test
  void decimalsCanBeLeftAsRawFields() throws Exception {
    final JsonNode json = MAPPER.readTree(
        serde(Map.of("decimalConventions", "")).deserialize(TestMessages.order(2)).getResult());

    assertEquals(12345, json.get("price").get("mantissa").asLong());
    assertEquals(-2, json.get("price").get("exponent").asInt());
    assertEquals(1, json.get("fills").get(0).get("fill").get("priceLength").asInt());
  }

  @Test
  void fieldAddedInLaterVersionIsNullForOlderMessages() throws Exception {
    final DeserializeResult result = serde(Map.of()).deserialize(TestMessages.order(1));

    final JsonNode json = MAPPER.readTree(result.getResult());
    assertTrue(json.get("tag").isNull());
    assertEquals("hello ✓", json.get("note").asText(), "fields after the block still line up");
    assertEquals(1, result.getAdditionalProperties().get("sbeVersion"));
  }

  @Test
  void skipsConfiguredPrefix() throws Exception {
    final byte[] message = TestMessages.order(2);
    final byte[] prefixed = new byte[message.length + 4];
    System.arraycopy(message, 0, prefixed, 4, message.length);

    final JsonNode json = MAPPER.readTree(serde(Map.of("messageOffset", 4)).deserialize(prefixed).getResult());

    assertEquals("Order", json.get("$message").asText());
  }

  @Test
  void decodesMultipleMessagesPerRecordAsArray() throws Exception {
    final byte[] order = TestMessages.order(2);
    final byte[] heartbeat = TestMessages.heartbeat(1234L);
    final byte[] both = Arrays.copyOf(order, order.length + heartbeat.length);
    System.arraycopy(heartbeat, 0, both, order.length, heartbeat.length);

    final JsonNode json = MAPPER.readTree(
        serde(Map.of("multipleMessagesPerRecord", true)).deserialize(both).getResult());

    assertEquals(2, json.size());
    assertEquals("Order", json.get(0).get("$message").asText());
    assertEquals("Heartbeat", json.get(1).get("$message").asText());
    assertEquals(1234L, json.get(1).get("timestamp").asLong());
  }

  @Test
  void reportsTrailingBytesWhenDecodingOneMessage() {
    final byte[] heartbeat = TestMessages.heartbeat(1L);

    final DeserializeResult result = serde(Map.of()).deserialize(Arrays.copyOf(heartbeat, heartbeat.length + 3));

    assertEquals(3, result.getAdditionalProperties().get("sbeTrailingBytes"));
  }

  @Test
  void unknownSchemaIdFallsBackToHex() {
    final byte[] data = TestMessages.heartbeat(1L);
    data[4] = 7; // schemaId, little-endian uint16 at offset 4

    final DeserializeResult result = serde(Map.of()).deserialize(data);

    assertEquals(DeserializeResult.Type.STRING, result.getType());
    assertTrue(result.getResult().startsWith("0800"), result.getResult());
    assertTrue(result.getAdditionalProperties().get("sbeError").toString().contains("No SBE schema with id 7"));
  }

  @Test
  void unknownTemplateIdFallsBackToHex() {
    final byte[] data = TestMessages.heartbeat(1L);
    data[2] = 9; // templateId

    final DeserializeResult result = serde(Map.of()).deserialize(data);

    assertTrue(result.getAdditionalProperties().get("sbeError").toString().contains("template id 9"));
  }

  @Test
  void truncatedRecordFallsBackToHex() {
    final byte[] order = TestMessages.order(2);

    for (final int length : new int[] {3, 20, order.length - 1}) {
      final DeserializeResult result = serde(Map.of()).deserialize(Arrays.copyOf(order, length));
      assertEquals(DeserializeResult.Type.STRING, result.getType(), "length " + length);
      assertTrue(result.getAdditionalProperties().containsKey("sbeError"), "length " + length);
    }
  }

  @Test
  void varDataLengthBeyondRecordFallsBackToHexWithoutAllocatingIt() {
    final DeserializeResult result = deserializeWithinHeap(serde(Map.of()), orderWithNoteLength(0x7FFFFFF0));

    assertEquals(DeserializeResult.Type.STRING, result.getType());
    assertTrue(result.getAdditionalProperties().containsKey("sbeError"));
  }

  @Test
  void varDataLengthThatOverflowsTheEndPositionFallsBackToHex() {
    // SBE adds the length to the offset without checking it, so the end position wraps negative
    // and would otherwise pass as a message that ends inside the record
    final DeserializeResult result =
        deserializeWithinHeap(serde(Map.of()), orderWithNoteLength(Integer.MAX_VALUE));

    assertEquals(DeserializeResult.Type.STRING, result.getType());
    assertTrue(result.getAdditionalProperties().containsKey("sbeError"));
    assertFalse(result.getAdditionalProperties().containsKey("sbeTrailingBytes"));
  }

  @Test
  void uint32LengthAboveIntMaxFallsBackToHex() {
    final DeserializeResult result = serde(Map.of()).deserialize(orderWithNoteLength(0x80000000));

    assertEquals(DeserializeResult.Type.STRING, result.getType());
    assertTrue(result.getAdditionalProperties().get("sbeError").toString().contains("UINT32"),
        result.getAdditionalProperties().toString());
  }

  @Test
  void groupEntriesThatTakeNoBytesFallBackToHex() {
    final byte[] order = TestMessages.order(2);
    final int fillsHeader = 8 + ByteBuffer.wrap(order).order(ByteOrder.LITTLE_ENDIAN).getShort(0);
    final ByteBuffer buffer = ByteBuffer.allocate(fillsHeader + 4 + 4 + 30).order(ByteOrder.LITTLE_ENDIAN);
    buffer.put(order, 0, fillsHeader);
    buffer.putShort((short) 0).putShort((short) 0xFFFF); // fills: block length 0, 65,535 entries
    // every entry reads its 34 bytes of fields from the same place, where the note follows too
    buffer.putInt(30).put("x".repeat(30).getBytes(StandardCharsets.US_ASCII));

    // otherwise these 76 bytes would decode as 65,535 fills
    final DeserializeResult result = serde(Map.of()).deserialize(buffer.array());

    assertEquals(DeserializeResult.Type.STRING, result.getType());
    assertTrue(result.getAdditionalProperties().containsKey("sbeError"));
  }

  @Test
  void decimalsWithWideExponentsAreConverted() throws Exception {
    final JsonNode json = MAPPER.readTree(edgeCaseSerde(Map.of()).deserialize(wideDecimals(-2, 3, 1)).getResult());

    assertEquals("123.45", json.get("price").asText());
    assertEquals("5000", json.get("size").asText());
    assertEquals("1.5", json.get("quantity").asText());
  }

  @Test
  void decimalExponentsTooLargeToWriteOutAreLeftAsRawFields() throws Exception {
    for (final int exponent : new int[] {Integer.MAX_VALUE, Integer.MIN_VALUE}) {
      final DeserializeResult result =
          deserializeWithinHeap(edgeCaseSerde(Map.of()), wideDecimals(exponent, 3, exponent));

      assertEquals(DeserializeResult.Type.JSON, result.getType(), "exponent " + exponent);
      final JsonNode json = MAPPER.readTree(result.getResult());
      assertEquals(exponent, json.get("price").get("exponent").asInt(), "exponent " + exponent);
      assertEquals(exponent, json.get("quantity").get("exponent").asInt(), "exponent " + exponent);
      assertEquals("5000", json.get("size").asText(), "exponent " + exponent);
    }
  }

  @Test
  void decimalExponentsUpToTheLimitAreConverted() throws Exception {
    final int limit = DecimalConvention.DEFAULT_MAX_EXPONENT;
    for (final int exponent : new int[] {limit, -limit}) {
      final JsonNode json = MAPPER.readTree(
          edgeCaseSerde(Map.of()).deserialize(wideDecimals(exponent, 3, exponent)).getResult());

      assertDecimal(new BigDecimal(BigInteger.valueOf(12345), -exponent), json.get("price"));
      assertDecimal(new BigDecimal(BigInteger.valueOf(15), exponent), json.get("quantity"));
    }
  }

  @Test
  void decimalExponentsJustBeyondTheLimitAreLeftAsRawFields() throws Exception {
    final int limit = DecimalConvention.DEFAULT_MAX_EXPONENT;
    for (final int exponent : new int[] {limit + 1, -limit - 1}) {
      final JsonNode json = MAPPER.readTree(
          edgeCaseSerde(Map.of()).deserialize(wideDecimals(exponent, 3, exponent)).getResult());

      assertEquals(exponent, json.get("price").get("exponent").asInt(), "exponent " + exponent);
      assertEquals(exponent, json.get("quantity").get("exponent").asInt(), "exponent " + exponent);
    }
  }

  @Test
  void maxDecimalExponentIsConfigurable() throws Exception {
    final JsonNode json = MAPPER.readTree(
        edgeCaseSerde(Map.of("maxDecimalExponent", 2)).deserialize(wideDecimals(-2, 3, 1)).getResult());

    assertEquals("123.45", json.get("price").asText());
    assertEquals(3, json.get("size").get("exponent").asInt());
  }

  @Test
  void rejectsNegativeMaxDecimalExponent() {
    final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> serde(Map.of("maxDecimalExponent", -1)));
    assertTrue(e.getMessage().contains("maxDecimalExponent"), e.getMessage());
  }

  @Test
  void decimalExponentBeyondIntRangeIsNotTruncated() throws Exception {
    final long exponent = (1L << 32) + 2; // 2 if truncated to an int

    final JsonNode json = MAPPER.readTree(
        deserializeWithinHeap(edgeCaseSerde(Map.of()), wideDecimals(-2, exponent, 1)).getResult());

    final JsonNode size = json.get("size");
    assertTrue(size.isObject(), size.toString());
    assertEquals(exponent, size.get("exponent").asLong());
  }

  @Test
  void unknownCharacterEncodingFallsBackToHex() {
    // SBE doesn't check characterEncoding when it parses the schema, so this only fails while decoding
    final DeserializeResult result = edgeCaseSerde(Map.of()).deserialize(unknownCharset("hi"));

    assertEquals(DeserializeResult.Type.STRING, result.getType());
    assertTrue(result.getAdditionalProperties().get("sbeError").toString().contains("X-NO-SUCH-CHARSET"),
        result.getAdditionalProperties().toString());
  }

  @Test
  void rejectsSchemasWithTheSameId() {
    final String schema = schemaPath().toString();

    final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> serde(Map.of("schemaFiles", List.of(schema, schema))));
    assertTrue(e.getMessage().contains("both have id 42"), e.getMessage());
  }

  @Test
  void requiresSchemaFiles() {
    final SbeSerde serde = new SbeSerde();
    final MapPropertyResolver empty = new MapPropertyResolver(Map.of());

    assertThrows(IllegalArgumentException.class, () -> serde.configure(empty, empty, empty));
  }

  @Test
  void rejectsUnknownDecimalConvention() {
    final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> serde(Map.of("decimalConventions", "fix,banana")));
    assertTrue(e.getMessage().contains("banana"), e.getMessage());
  }

  @Test
  void isDeserializeOnly() {
    final SbeSerde serde = serde(Map.of());

    assertTrue(serde.canDeserialize("any", Serde.Target.VALUE));
    assertFalse(serde.canSerialize("any", Serde.Target.VALUE));
    assertTrue(serde.getDescription().orElseThrow().contains("id 42"));
  }

  private static SbeSerde serde(final Map<String, Object> overrides) {
    final Map<String, Object> properties = new HashMap<>();
    properties.put("schemaFiles", schemaPath().toString());
    properties.putAll(overrides);
    final SbeSerde serde = new SbeSerde();
    final MapPropertyResolver empty = new MapPropertyResolver(Map.of());
    serde.configure(new MapPropertyResolver(properties), empty, empty);
    return serde;
  }

  /**
   * A serde over {@code edge-cases.xml}, with both decimal conventions.
   */
  private static SbeSerde edgeCaseSerde(final Map<String, Object> overrides) {
    final Map<String, Object> properties = new HashMap<>();
    properties.put("schemaFiles", resourcePath("/sbe/edge-cases.xml").toString());
    properties.put("decimalConventions", "fix,bigDecimal");
    properties.putAll(overrides);
    return serde(properties);
  }

  /**
   * Compares by value, since a plain decimal string loses the scale.
   */
  private static void assertDecimal(final BigDecimal expected, final JsonNode actual) {
    assertTrue(actual.isTextual(), actual.toString());
    assertEquals(0, expected.compareTo(new BigDecimal(actual.asText())), actual.asText());
  }

  /**
   * Deserializes a record whose sizes, taken at face value, need gigabytes: more than the test worker's heap
   * (512 MB by default).
   */
  private static DeserializeResult deserializeWithinHeap(final SbeSerde serde, final byte[] data) {
    try {
      return serde.deserialize(data);
    } catch (final OutOfMemoryError e) {
      // caught here, as JUnit rethrows it and that aborts the whole test run
      throw new AssertionError("sized an allocation from the record instead of checking it", e);
    }
  }

  /**
   * An Order whose {@code note} (the last field: uint32 length, then "hello ✓" as 9 bytes) claims {@code length}.
   */
  private static byte[] orderWithNoteLength(final int length) {
    final byte[] data = TestMessages.order(2);
    ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).putInt(data.length - 9 - 4, length);
    return data;
  }

  /**
   * A WideDecimals message from {@code edge-cases.xml}: price 123.45 and size 5000 at the default exponents
   * of -2 and 3, and quantity 1.5 at a scale of 1.
   */
  private static byte[] wideDecimals(final int priceExponent, final long sizeExponent, final int quantityScale) {
    final ByteBuffer buffer = edgeCaseMessage(1, 37, 37);
    buffer.putLong(12345L).putInt(priceExponent);
    buffer.putLong(5L).putLong(sizeExponent);
    buffer.put((byte) 1).put(new byte[] {15, 0, 0, 0}).putInt(quantityScale);
    return buffer.array();
  }

  /**
   * An UnknownCharset message from {@code edge-cases.xml}.
   */
  private static byte[] unknownCharset(final String text) {
    final byte[] bytes = text.getBytes(StandardCharsets.US_ASCII);
    final ByteBuffer buffer = edgeCaseMessage(2, 0, 2 + bytes.length);
    buffer.putShort((short) bytes.length).put(bytes);
    return buffer.array();
  }

  private static ByteBuffer edgeCaseMessage(final int templateId, final int blockLength, final int bodyLength) {
    final ByteBuffer buffer = ByteBuffer.allocate(8 + bodyLength).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putShort((short) blockLength).putShort((short) templateId).putShort((short) 43).putShort((short) 1);
    return buffer;
  }

  private static Path schemaPath() {
    return resourcePath("/sbe/trading.xml");
  }

  private static Path resourcePath(final String name) {
    try {
      return Path.of(SbeSerdeTest.class.getResource(name).toURI());
    } catch (final URISyntaxException e) {
      throw new IllegalStateException(e);
    }
  }

  private static List<String> strings(final JsonNode array) {
    return StreamSupport.stream(array.spliterator(), false).map(JsonNode::asText).toList();
  }

  private static List<String> fieldNames(final JsonNode object) {
    return object.properties().stream().map(Map.Entry::getKey).toList();
  }
}
