package io.github.codebrazier.kafbat.sbe;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.List;
import java.util.Set;
import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.ir.Token;
import uk.co.real_logic.sbe.otf.OtfHeaderDecoder;
import uk.co.real_logic.sbe.otf.OtfMessageDecoder;

/**
 * Decodes the SBE message(s) in one Kafka record value (or key) into JSON.
 */
final class SbeRecordDecoder {

  record Decoded(JsonNode json, Header firstHeader, String firstMessageName, int trailingBytes) {
  }

  record Header(int schemaId, int templateId, int version, int blockLength) {
  }

  static final class DecodeException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    DecodeException(final String message) {
      super(message);
    }
  }

  private final SbeSchemas schemas;
  private final int messageOffset;
  private final boolean multipleMessages;
  private final Set<DecimalConvention> decimalConventions;
  private final int maxDecimalExponent;

  SbeRecordDecoder(final SbeSchemas schemas, final int messageOffset, final boolean multipleMessages,
                   final Set<DecimalConvention> decimalConventions, final int maxDecimalExponent) {
    this.schemas = schemas;
    this.messageOffset = messageOffset;
    this.multipleMessages = multipleMessages;
    this.decimalConventions = decimalConventions;
    this.maxDecimalExponent = maxDecimalExponent;
  }

  Decoded decode(final byte[] data) {
    final ByteArrayReadBuffer buffer = new ByteArrayReadBuffer(data);
    final OtfHeaderDecoder headerDecoder = schemas.headerDecoder();

    int position = messageOffset;
    final ArrayNode messages = JsonNodeFactory.instance.arrayNode();
    Header firstHeader = null;
    String firstName = null;
    do {
      if (position + headerDecoder.encodedLength() > data.length) {
        throw new DecodeException("Record is too short for an SBE message header at offset %d (%d bytes)"
            .formatted(position, data.length));
      }
      final Header header = new Header(
          headerDecoder.getSchemaId(buffer, position),
          headerDecoder.getTemplateId(buffer, position),
          headerDecoder.getSchemaVersion(buffer, position),
          headerDecoder.getBlockLength(buffer, position));

      final Ir ir = schemas.forSchemaId(header.schemaId());
      if (ir == null) {
        throw new DecodeException("No SBE schema with id %d is configured (have: %s)"
            .formatted(header.schemaId(), schemas.describe()));
      }
      final List<Token> tokens = ir.getMessage(header.templateId());
      if (tokens == null) {
        throw new DecodeException("SBE schema %d has no message with template id %d"
            .formatted(header.schemaId(), header.templateId()));
      }

      // the root fields follow the BEGIN_MESSAGE token
      final int rootMinBlockLength = SbeSchemas.minBlockLength(tokens, 1, header.version());
      if (header.blockLength() < rootMinBlockLength) {
        throw new DecodeException("SBE message %s has block length %d, but version %d needs at least %d"
            .formatted(tokens.get(0).name(), header.blockLength(), header.version(), rootMinBlockLength));
      }

      final JsonTreeListener listener = new JsonTreeListener(new GroupListener(tokens, header.version()));
      position = OtfMessageDecoder.decode(buffer, position + headerDecoder.encodedLength(),
          header.version(), header.blockLength(), tokens, listener);
      if (position < 0) {
        throw new DecodeException("SBE message %s has lengths that overflow".formatted(tokens.get(0).name()));
      }
      if (position > data.length) {
        throw new DecodeException("SBE message %s is truncated: needs %d bytes, record has %d"
            .formatted(tokens.get(0).name(), position, data.length));
      }
      messages.add(DecimalConvention.apply(listener.result(), decimalConventions, maxDecimalExponent));

      if (firstHeader == null) {
        firstHeader = header;
        firstName = tokens.get(0).name();
      }
    } while (multipleMessages && position < data.length);

    final JsonNode json = multipleMessages ? messages : messages.get(0);
    return new Decoded(json, firstHeader, firstName, data.length - position);
  }
}
