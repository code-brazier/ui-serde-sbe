package io.github.codebrazier.kafbat.sbe;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * SBE has no standard decimal type, so decimals arrive as composites of plain integers. A convention recognises
 * one such encoding by the shape of the decoded JSON and replaces it with the decimal value as a plain string
 * (a string, because JavaScript numbers would lose precision in the UI).
 */
enum DecimalConvention {

  /**
   * The FIX/SBE convention: a composite of exactly {@code mantissa} and {@code exponent},
   * with value {@code mantissa * 10^exponent}. A null mantissa means the whole decimal is null.
   */
  FIX("fix") {
    @Override
    Optional<JsonNode> convert(final ObjectNode node, final int maxExponent) {
      if (!hasExactly(node, "mantissa", "exponent")) {
        return Optional.empty();
      }
      final JsonNode mantissa = node.get("mantissa");
      final JsonNode exponent = node.get("exponent");
      if (mantissa.isNull()) {
        return Optional.of(NODES.nullNode());
      }
      if (!mantissa.isIntegralNumber() || !exponentWithin(exponent, maxExponent)) {
        return Optional.empty();
      }
      return Optional.of(decimal(new BigDecimal(mantissa.bigIntegerValue(), -exponent.intValue())));
    }
  },

  /**
   * Java {@link BigDecimal}'s own representation: the unscaled value as big-endian two's-complement bytes
   * ({@link BigInteger#toByteArray()}) and the scale. On the wire that's {@code length} (bytes used), a fixed-size
   * {@code mantissa} byte array, and {@code exponent} as the scale. A length of 0 means null.
   * Also matches the same three values written as sibling fields {@code xLength}, {@code xMantissa},
   * {@code xExponent}, which are collapsed into a single field {@code x}.
   */
  BIG_DECIMAL("bigDecimal") {
    @Override
    Optional<JsonNode> convert(final ObjectNode node, final int maxExponent) {
      if (!hasExactly(node, "length", "mantissa", "exponent")) {
        return Optional.empty();
      }
      return scaled(node.get("length"), node.get("mantissa"), node.get("exponent"), maxExponent);
    }

    @Override
    void convertSiblings(final ObjectNode node, final int maxExponent) {
      final List<String> prefixes = new ArrayList<>();
      node.fieldNames().forEachRemaining(name -> {
        if (name.endsWith("Mantissa") && name.length() > "Mantissa".length()) {
          prefixes.add(name.substring(0, name.length() - "Mantissa".length()));
        }
      });
      for (final String prefix : prefixes) {
        final Optional<JsonNode> value = scaled(node.get(prefix + "Length"), node.get(prefix + "Mantissa"),
            node.get(prefix + "Exponent"), maxExponent);
        value.ifPresent(v -> replaceTriple(node, prefix, v));
      }
    }
  };

  /**
   * The default for {@code maxDecimalExponent}. Real encodings stay far below it (FIX-style exponents within
   * about ±18), and it keeps a converted value to about a kilobyte.
   */
  static final int DEFAULT_MAX_EXPONENT = 1000;

  private static final JsonNodeFactory NODES = JsonNodeFactory.instance;

  private final String configName;

  DecimalConvention(final String configName) {
    this.configName = configName;
  }

  String configName() {
    return configName;
  }

  static DecimalConvention fromConfig(final String name) {
    for (final DecimalConvention convention : values()) {
      if (convention.configName.equalsIgnoreCase(name.trim())) {
        return convention;
      }
    }
    throw new IllegalArgumentException("Unknown decimal convention '%s', expected one of: %s"
        .formatted(name, Arrays.stream(values()).map(DecimalConvention::configName).toList()));
  }

  /**
   * Returns the decimal this object encodes, or empty if it isn't this convention's shape
   * or its exponent is beyond {@code maxExponent}.
   */
  abstract Optional<JsonNode> convert(ObjectNode node, int maxExponent);

  /**
   * Rewrites decimals encoded as separate fields of {@code node}, in place.
   */
  void convertSiblings(final ObjectNode node, final int maxExponent) {
  }

  /**
   * Applies the conventions to every object in the tree, deepest first. Decimals whose exponent is beyond
   * ±{@code maxExponent} are left as their raw fields: written out in full they'd be mostly zeros, and a corrupt
   * exponent could need gigabytes.
   */
  static JsonNode apply(final JsonNode node, final Set<DecimalConvention> conventions, final int maxExponent) {
    if (conventions.isEmpty()) {
      return node;
    }
    if (node.isArray()) {
      for (int i = 0; i < node.size(); i++) {
        ((ArrayNode) node).set(i, apply(node.get(i), conventions, maxExponent));
      }
      return node;
    }
    if (!node.isObject()) {
      return node;
    }
    final ObjectNode object = (ObjectNode) node;
    for (final Map.Entry<String, JsonNode> field : object.properties()) {
      field.setValue(apply(field.getValue(), conventions, maxExponent));
    }
    for (final DecimalConvention convention : conventions) {
      convention.convertSiblings(object, maxExponent);
    }
    for (final DecimalConvention convention : conventions) {
      final Optional<JsonNode> converted = convention.convert(object, maxExponent);
      if (converted.isPresent()) {
        return converted.get();
      }
    }
    return object;
  }

  private static boolean hasExactly(final ObjectNode node, final String... names) {
    if (node.size() != names.length) {
      return false;
    }
    for (final String name : names) {
      if (!node.has(name)) {
        return false;
      }
    }
    return true;
  }

  private static Optional<JsonNode> scaled(final JsonNode length, final JsonNode mantissa, final JsonNode exponent,
                                           final int maxExponent) {
    if (length == null || mantissa == null || exponent == null
        || !length.isIntegralNumber() || !mantissa.isArray() || !exponentWithin(exponent, maxExponent)) {
      return Optional.empty();
    }
    final int used = length.intValue();
    if (used < 0 || used > mantissa.size()) {
      return Optional.empty();
    }
    if (used == 0) {
      return Optional.of(NODES.nullNode());
    }
    final byte[] bytes = new byte[used];
    for (int i = 0; i < used; i++) {
      final JsonNode b = mantissa.get(i);
      if (!b.isIntegralNumber()) {
        return Optional.empty();
      }
      bytes[i] = (byte) b.intValue();
    }
    return Optional.of(decimal(new BigDecimal(new BigInteger(bytes), exponent.intValue())));
  }

  /**
   * Whether {@code exponent} is an integer within ±{@code maxExponent}, compared as a long so that a wider value
   * isn't truncated first. After this, {@code intValue()} and negating it are safe.
   */
  private static boolean exponentWithin(final JsonNode exponent, final int maxExponent) {
    if (!exponent.isIntegralNumber() || !exponent.canConvertToLong()) {
      return false;
    }
    final long value = exponent.longValue();
    return -maxExponent <= value && value <= maxExponent;
  }

  /**
   * Replaces the three sibling fields with one, keeping it where the first of them was.
   */
  private static void replaceTriple(final ObjectNode node, final String prefix, final JsonNode value) {
    final Set<String> parts = Set.of(prefix + "Length", prefix + "Mantissa", prefix + "Exponent");
    final ObjectNode rebuilt = NODES.objectNode();
    boolean inserted = false;
    for (final Map.Entry<String, JsonNode> field : node.properties()) {
      if (parts.contains(field.getKey())) {
        if (!inserted) {
          rebuilt.set(prefix, value);
          inserted = true;
        }
      } else {
        rebuilt.set(field.getKey(), field.getValue());
      }
    }
    node.removeAll();
    node.setAll(rebuilt);
  }

  private static JsonNode decimal(final BigDecimal value) {
    return NODES.textNode(value.toPlainString());
  }
}
