package io.github.codebrazier.kafbat.sbe;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kafbat.ui.serde.api.DeserializeResult;
import io.kafbat.ui.serde.api.PropertyResolver;
import io.kafbat.ui.serde.api.RecordHeaders;
import io.kafbat.ui.serde.api.SchemaDescription;
import io.kafbat.ui.serde.api.Serde;


import java.nio.charset.UnsupportedCharsetException;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Decodes Simple Binary Encoding (SBE) messages to JSON, driven by the SBE XML schema at runtime,
 * so no generated codecs are needed.
 *
 * <p>Serde properties:
 * <ul>
 *   <li>{@code schemaFiles} (required): SBE XML schema file(s). Messages are matched to a schema by the
 *   schema id in their header, so the ids must be unique within one serde.</li>
 *   <li>{@code messageOffset} (default 0): bytes to skip before the first SBE message header.</li>
 *   <li>{@code multipleMessagesPerRecord} (default false): decode consecutive messages until the end of the
 *   record, producing a JSON array.</li>
 *   <li>{@code decimalConventions} (default {@code fix}): how decimals are encoded, any of {@code fix},
 *   {@code bigDecimal}; empty to show decimals as their raw fields.</li>
 *   <li>{@code maxDecimalExponent} (default 1000): decimals with an exponent beyond plus or minus this are shown
 *   as their raw fields, since written out in full they'd be mostly zeros.</li>
 * </ul>
 */
public class SbeSerde implements Serde {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private SbeRecordDecoder decoder;
  private String description;

  @Override
  public void configure(final PropertyResolver serdeProperties,
                        final PropertyResolver kafkaClusterProperties,
                        final PropertyResolver globalProperties) {
    final List<Path> schemaFiles = serdeProperties.getListProperty("schemaFiles", String.class)
        .orElseThrow(() -> new IllegalArgumentException("SBE serde requires the 'schemaFiles' property"))
        .stream()
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .map(Path::of)
        .toList();
    final int messageOffset = serdeProperties.getProperty("messageOffset", Integer.class).orElse(0);
    if (messageOffset < 0) {
      throw new IllegalArgumentException("SBE serde 'messageOffset' must not be negative");
    }
    final boolean multipleMessages = serdeProperties.getProperty("multipleMessagesPerRecord", Boolean.class)
        .orElse(false);
    final Set<DecimalConvention> conventions = EnumSet.noneOf(DecimalConvention.class);
    serdeProperties.getListProperty("decimalConventions", String.class)
        .orElse(List.of(DecimalConvention.FIX.configName()))
        .stream()
        .filter(s -> !s.isBlank())
        .map(DecimalConvention::fromConfig)
        .forEach(conventions::add);
    final int maxDecimalExponent = serdeProperties.getProperty("maxDecimalExponent", Integer.class)
        .orElse(DecimalConvention.DEFAULT_MAX_EXPONENT);
    if (maxDecimalExponent < 0) {
      throw new IllegalArgumentException("SBE serde 'maxDecimalExponent' must not be negative");
    }

    final SbeSchemas schemas = SbeSchemas.load(schemaFiles);
    this.decoder = new SbeRecordDecoder(schemas, messageOffset, multipleMessages, conventions,
        maxDecimalExponent);
    this.description = "SBE: " + schemas.describe();
  }

  @Override
  public Optional<String> getDescription() {
    return Optional.ofNullable(description);
  }

  @Override
  public Optional<SchemaDescription> getSchema(final String topic, final Target type) {
    return Optional.empty();
  }

  @Override
  public boolean canDeserialize(final String topic, final Target type) {
    return true;
  }

  @Override
  public boolean canSerialize(final String topic, final Target type) {
    return false;
  }

  @Override
  public Serializer serializer(final String topic, final Target type) {
    throw new UnsupportedOperationException("SBE serde does not support producing messages");
  }

  @Override
  public Deserializer deserializer(final String topic, final Target type) {
    return (RecordHeaders headers, byte[] data) -> deserialize(data);
  }

  DeserializeResult deserialize(final byte[] data) {
    if (data == null) {
      return new DeserializeResult(null, DeserializeResult.Type.STRING, Map.of());
    }
    try {
      final SbeRecordDecoder.Decoded decoded = decoder.decode(data);
      final Map<String, Object> properties = new LinkedHashMap<>();
      properties.put("sbeMessage", decoded.firstMessageName());
      properties.put("sbeSchemaId", decoded.firstHeader().schemaId());
      properties.put("sbeTemplateId", decoded.firstHeader().templateId());
      properties.put("sbeVersion", decoded.firstHeader().version());
      if (decoded.trailingBytes() > 0) {
        properties.put("sbeTrailingBytes", decoded.trailingBytes());
      }
      return new DeserializeResult(MAPPER.writeValueAsString(decoded.json()), DeserializeResult.Type.JSON,
          properties);
    } catch (final SbeRecordDecoder.DecodeException | IndexOutOfBoundsException | IllegalStateException
                   | JsonProcessingException | UnsupportedCharsetException e) {
      // show the bytes rather than failing the whole page of messages
      return new DeserializeResult(HexFormat.of().formatHex(data), DeserializeResult.Type.STRING,
          Map.of("sbeError", String.valueOf(e.getMessage())));
    }
  }
}
