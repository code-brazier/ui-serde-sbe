package io.kafbat.ui.serde.sbe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Runs against e2e/compose.yaml: the published Kafbat UI image with the plugin jar mounted, configured through
 * environment variables. Messages go in through Kafka and are read back through Kafbat UI's REST API, so this
 * covers plugin loading, property binding and serde selection by topic, which unit tests can't.
 */
class KafkaUiE2eTest {

  private static final String BOOTSTRAP = "localhost:" + System.getProperty("e2e.kafkaPort", "19092");
  private static final String UI = "http://localhost:" + System.getProperty("e2e.uiPort", "18080");
  private static final Duration TIMEOUT = Duration.ofSeconds(60);

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

  @BeforeAll
  static void produce() throws Exception {
    final Map<String, Object> config = Map.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(config)) {
      send(producer, "sbe-orders", TestMessages.order(2));
      send(producer, "sbe-orders", TestMessages.order(1));

      final byte[] order = TestMessages.order(2);
      send(producer, "sbe-bad", Arrays.copyOf(order, order.length - 5));
      final byte[] unknownTemplate = TestMessages.heartbeat(1L);
      unknownTemplate[2] = 9;
      send(producer, "sbe-bad", unknownTemplate);

      send(producer, "sbe-batched", concat(new byte[] {1, 2, 3, 4}, order, TestMessages.heartbeat(1234L)));

      send(producer, "sbe-quotes", quote("EURUSD", 10_873L));
      producer.flush();
    }
  }

  @Test
  void decodesOrdersWithThePluginChosenByTopicPattern() throws Exception {
    final List<JsonNode> messages = messages("sbe-orders", 2);

    for (final JsonNode message : messages) {
      assertEquals("SBE", message.get("valueSerde").asText());
    }
    final JsonNode current = value(messages.get(0));
    assertEquals("Order", current.get("$message").asText());
    assertEquals("BTC-USD", current.get("symbol").asText());
    assertEquals("ASK", current.get("side").asText());
    assertEquals("123.45", current.get("price").asText());
    assertEquals("-0.005", current.get("quantity").asText(), "decimalConventions bound from the environment");
    assertEquals(7, current.get("tag").asInt());
    assertEquals("1.5", current.get("fills").get(0).get("fill").get("price").asText());
    assertEquals("hello ✓", current.get("note").asText());

    final JsonNode properties = messages.get(0).get("valueDeserializeProperties");
    assertEquals("Order", properties.get("sbeMessage").asText());
    assertEquals(42, properties.get("sbeSchemaId").asInt());

    final JsonNode olderVersion = value(messages.get(1));
    assertTrue(olderVersion.get("tag").isNull());
    assertEquals(1, messages.get(1).get("valueDeserializeProperties").get("sbeVersion").asInt());
  }

  @Test
  void undecodableRecordsAreShownAsHexWithTheReason() throws Exception {
    final List<JsonNode> messages = messages("sbe-bad", 2);

    for (final JsonNode message : messages) {
      assertEquals("SBE", message.get("valueSerde").asText());
      assertTrue(message.get("value").asText().matches("[0-9a-f]+"), message.toString());
    }
    assertTrue(error(messages.get(0)).contains("truncated") || error(messages.get(0)).contains("out of bounds"),
        error(messages.get(0)));
    assertTrue(error(messages.get(1)).contains("template id 9"), error(messages.get(1)));
  }

  @Test
  void decodesPrefixedBatchWithItsOwnSerdeInstance() throws Exception {
    final JsonNode message = messages("sbe-batched", 1).get(0);

    assertEquals("SBE batched", message.get("valueSerde").asText());
    final JsonNode batch = value(message);
    assertEquals(2, batch.size());
    assertEquals("Order", batch.get(0).get("$message").asText());
    assertEquals("Heartbeat", batch.get(1).get("$message").asText());
    assertEquals(1234L, batch.get(1).get("timestamp").asLong());
  }

  @Test
  void schemasSharingAnIdAreKeptApartBySerdeInstance() throws Exception {
    final JsonNode message = messages("sbe-quotes", 1).get(0);

    assertEquals("SBE quotes", message.get("valueSerde").asText());
    final JsonNode quote = value(message);
    assertEquals("Quote", quote.get("$message").asText());
    assertEquals("EURUSD", quote.get("symbol").asText());
    assertEquals(10_873L, quote.get("bid").asLong());
  }

  @Test
  void pluginIsOfferedAsThePreferredSerde() throws Exception {
    final JsonNode suggestions = getJson("/api/clusters/local/topics/sbe-orders/serdes?use=DESERIALIZE");

    JsonNode sbe = null;
    for (final JsonNode serde : suggestions.get("value")) {
      if ("SBE".equals(serde.get("name").asText())) {
        sbe = serde;
      }
    }
    assertTrue(sbe != null, suggestions.toString());
    assertTrue(sbe.get("preferred").asBoolean(), suggestions.toString());
    assertTrue(sbe.get("description").asText().contains("id 42"), sbe.toString());
  }

  @Test
  void kafkaUiLogsNoPluginErrors() throws Exception {
    final Process logs = new ProcessBuilder(
        "docker", "compose", "-f", System.getProperty("e2e.composeFile"), "logs", "--no-color", "kafka-ui")
        .redirectErrorStream(true)
        .start();
    final String output = new String(logs.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    assertEquals(0, logs.waitFor(), output);

    final List<String> problems = output.lines()
        .filter(line -> line.contains("ERROR") || line.contains("Exception") || line.contains("Error:"))
        .filter(line -> line.toLowerCase().contains("serde") || line.contains("agrona") || line.contains("sbe"))
        .toList();
    assertEquals(List.of(), problems);
  }

  /**
   * Polls until the topic has the expected number of messages, since Kafbat UI may not see a new topic at once.
   */
  private static List<JsonNode> messages(final String topic, final int expected) throws Exception {
    final long deadline = System.nanoTime() + TIMEOUT.toNanos();
    List<JsonNode> messages = List.of();
    while (System.nanoTime() < deadline) {
      messages = readMessages(topic);
      if (messages.size() >= expected) {
        return messages;
      }
      Thread.sleep(500);
    }
    throw new AssertionError("Expected %d messages on %s, got %s".formatted(expected, topic, messages));
  }

  private static List<JsonNode> readMessages(final String topic) throws IOException, InterruptedException {
    final String path = "/api/clusters/local/topics/%s/messages/v2?mode=EARLIEST&limit=100"
        .formatted(URLEncoder.encode(topic, StandardCharsets.UTF_8));
    final HttpResponse<Stream<String>> response = HTTP.send(
        HttpRequest.newBuilder(URI.create(UI + path)).header("Accept", "text/event-stream").timeout(TIMEOUT).build(),
        HttpResponse.BodyHandlers.ofLines());
    if (response.statusCode() != 200) {
      return List.of();
    }
    final List<JsonNode> messages = new ArrayList<>();
    try (Stream<String> lines = response.body()) {
      for (final String line : (Iterable<String>) lines::iterator) {
        if (!line.startsWith("data:")) {
          continue;
        }
        final JsonNode event = MAPPER.readTree(line.substring("data:".length()));
        if ("MESSAGE".equals(event.path("type").asText())) {
          messages.add(event.get("message"));
        }
      }
    }
    messages.sort((a, b) -> Long.compare(a.get("offset").asLong(), b.get("offset").asLong()));
    return messages;
  }

  private static JsonNode getJson(final String path) throws IOException, InterruptedException {
    final HttpResponse<String> response = HTTP.send(
        HttpRequest.newBuilder(URI.create(UI + path)).timeout(TIMEOUT).build(),
        HttpResponse.BodyHandlers.ofString());
    assertEquals(200, response.statusCode(), response.body());
    return MAPPER.readTree(response.body());
  }

  private static JsonNode value(final JsonNode message) throws IOException {
    return MAPPER.readTree(message.get("value").asText());
  }

  private static String error(final JsonNode message) {
    return message.path("valueDeserializeProperties").path("sbeError").asText();
  }

  private static void send(final KafkaProducer<byte[], byte[]> producer, final String topic, final byte[] value)
      throws Exception {
    producer.send(new ProducerRecord<>(topic, value)).get();
  }

  private static byte[] concat(final byte[]... parts) {
    final ByteBuffer buffer = ByteBuffer.allocate(Arrays.stream(parts).mapToInt(p -> p.length).sum());
    for (final byte[] part : parts) {
      buffer.put(part);
    }
    return buffer.array();
  }

  /**
   * A Quote from e2e/schemas/quotes.xml, encoded by hand: it's too small to need generated codecs.
   */
  private static byte[] quote(final String symbol, final long bid) {
    final ByteBuffer buffer = ByteBuffer.allocate(8 + 16).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putShort((short) 16).putShort((short) 1).putShort((short) 42).putShort((short) 1);
    buffer.put(Arrays.copyOf(symbol.getBytes(StandardCharsets.US_ASCII), 8));
    buffer.putLong(bid);
    return buffer.array();
  }
}
