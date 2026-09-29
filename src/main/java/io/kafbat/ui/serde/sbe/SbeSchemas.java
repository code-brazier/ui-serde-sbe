package io.kafbat.ui.serde.sbe;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.xml.sax.InputSource;
import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.ir.Signal;
import uk.co.real_logic.sbe.ir.Token;
import uk.co.real_logic.sbe.otf.OtfHeaderDecoder;
import uk.co.real_logic.sbe.xml.IrGenerator;
import uk.co.real_logic.sbe.xml.MessageSchema;
import uk.co.real_logic.sbe.xml.ParserOptions;
import uk.co.real_logic.sbe.xml.XmlSchemaParser;

/**
 * The schemas one serde instance decodes with, looked up by the schema id in each message header.
 */
final class SbeSchemas {

  private final Map<Integer, Ir> bySchemaId;
  private final OtfHeaderDecoder headerDecoder;

  private SbeSchemas(final Map<Integer, Ir> bySchemaId, final OtfHeaderDecoder headerDecoder) {
    this.bySchemaId = bySchemaId;
    this.headerDecoder = headerDecoder;
  }

  static SbeSchemas load(final List<Path> schemaFiles) {
    if (schemaFiles.isEmpty()) {
      throw new IllegalArgumentException("At least one SBE schema file must be configured");
    }
    final Map<Integer, Ir> bySchemaId = new HashMap<>();
    final Map<Integer, Path> sources = new HashMap<>();
    String headerLayout = null;
    Path headerSource = null;
    for (final Path file : schemaFiles) {
      final Ir ir = parse(file);
      final Path previous = sources.putIfAbsent(ir.id(), file);
      if (previous != null) {
        throw new IllegalArgumentException(("SBE schemas %s and %s both have id %d, so messages can't be told apart."
            + " Register a separate serde for each, restricted to its topics with topicValuesPattern.")
            .formatted(previous, file, ir.id()));
      }
      final String layout = headerLayout(ir);
      if (headerLayout == null) {
        headerLayout = layout;
        headerSource = file;
      } else if (!headerLayout.equals(layout)) {
        throw new IllegalArgumentException(
            "SBE schemas %s and %s have different message headers; register a separate serde for each"
                .formatted(headerSource, file));
      }
      bySchemaId.put(ir.id(), ir);
    }
    // every schema has the same header layout, so any of them can decode it
    final Ir any = bySchemaId.values().iterator().next();
    return new SbeSchemas(Map.copyOf(bySchemaId), new OtfHeaderDecoder(any.headerStructure()));
  }

  OtfHeaderDecoder headerDecoder() {
    return headerDecoder;
  }

  Ir forSchemaId(final int schemaId) {
    return bySchemaId.get(schemaId);
  }

  String describe() {
    return bySchemaId.values().stream()
        .map(ir -> "%s (id %d, v%d)".formatted(
            ir.packageName() != null ? ir.packageName() : "schema", ir.id(), ir.version()))
        .sorted()
        .collect(Collectors.joining(", "));
  }

  static int minBlockLength(final List<Token> tokens, final int from, final int actingVersion) {
    int min = 0;
    for (int i = from; i < tokens.size() && tokens.get(i).signal() == Signal.BEGIN_FIELD; ) {
      final Token field = tokens.get(i);
      if (field.encodedLength() > 0 && field.version() <= actingVersion) {
        min = Math.max(min, field.offset() + field.encodedLength());
      }

      // skip over the field's type tokens
      i += field.componentTokenCount();
    }
    return min;
  }

  private static Ir parse(final Path file) {
    if (!Files.isReadable(file)) {
      throw new IllegalArgumentException("SBE schema file is not readable: " + file);
    }
    final ByteArrayOutputStream errors = new ByteArrayOutputStream();
    try (PrintStream errorStream = new PrintStream(errors, true, StandardCharsets.UTF_8)) {
      final ParserOptions options = ParserOptions.builder()
          .xIncludeAware(true)
          .stopOnError(true)
          .errorPrintStream(errorStream)
          .build();
      // a system id lets relative xi:include hrefs resolve against the schema's own directory
      final InputSource source = new InputSource(file.toUri().toString());
      final MessageSchema schema = XmlSchemaParser.parse(source, options);
      return new IrGenerator().generate(schema);
    } catch (final Exception e) {
      final String details = errors.toString(StandardCharsets.UTF_8).trim();
      throw new IllegalArgumentException("Could not parse SBE schema " + file
          + (details.isEmpty() ? "" : ":\n" + details), e);
    }
  }

  private static String headerLayout(final Ir ir) {
    return ir.headerStructure().tokens().stream()
        .map(t -> t.name() + ":" + t.signal() + ":" + t.offset() + ":" + t.encodedLength() + ":" + encodingOf(t))
        .collect(Collectors.joining(","));
  }

  private static String encodingOf(final Token token) {
    return token.encoding() == null ? "" : token.encoding().primitiveType() + "/" + token.encoding().byteOrder();
  }
}
