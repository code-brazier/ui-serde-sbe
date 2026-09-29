package io.github.codebrazier.kafbat.sbe;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HexFormat;
import java.util.List;
import org.agrona.DirectBuffer;
import uk.co.real_logic.sbe.PrimitiveType;
import uk.co.real_logic.sbe.PrimitiveValue;
import uk.co.real_logic.sbe.ir.Encoding;
import uk.co.real_logic.sbe.ir.Token;
import uk.co.real_logic.sbe.otf.TokenListener;
import uk.co.real_logic.sbe.otf.Types;

/**
 * Builds a Jackson tree from the callbacks of {@link uk.co.real_logic.sbe.otf.OtfMessageDecoder}.
 *
 * <p>Field naming follows SBE's own {@code JsonTokenListener}: inside a composite the decoder passes the
 * enclosing field token, so member names have to be taken from the type tokens instead.
 */
final class JsonTreeListener implements TokenListener {

  static final String MESSAGE_TYPE_FIELD = "$message";

  private static final JsonNodeFactory NODES = JsonNodeFactory.instance;

  private final Deque<ObjectNode> stack = new ArrayDeque<>();
  private final GroupListener groupListener;
  private ObjectNode root;
  private int compositeLevel;

  JsonTreeListener(final GroupListener groupListener) {
    this.groupListener = groupListener;
  }

  ObjectNode result() {
    return root;
  }

  @Override
  public void onBeginMessage(final Token token) {
    root = NODES.objectNode();
    root.put(MESSAGE_TYPE_FIELD, token.name());
    stack.clear();
    stack.push(root);
    compositeLevel = 0;
  }

  @Override
  public void onEndMessage(final Token token) {
    stack.pop();
  }

  @Override
  public void onEncoding(final Token fieldToken, final DirectBuffer buffer, final int index, final Token typeToken,
                         final int actingVersion) {
    groupListener.onFieldIndex(index);

    final String name = compositeLevel > 0 ? typeToken.name() : fieldToken.name();
    final Encoding encoding = typeToken.encoding();
    final PrimitiveValue constOrAbsent = constOrNotPresentValue(fieldToken, typeToken, actingVersion);
    final int arrayLength = typeToken.arrayLength();

    if (encoding.primitiveType() == PrimitiveType.CHAR && (arrayLength > 1 || encoding.characterEncoding() != null)) {
      current().put(name, constOrAbsent != null
          ? charConstant(constOrAbsent, encoding)
          : chars(buffer, index, arrayLength, encoding));
    } else if (arrayLength > 1) {
      final ArrayNode array = current().putArray(name);
      final int elementSize = encoding.primitiveType().size();
      for (int i = 0; i < arrayLength; i++) {
        array.add(constOrAbsent != null
            ? primitive(constOrAbsent, encoding)
            : primitive(buffer, index + i * elementSize, encoding, false));
      }
    } else {
      current().set(name, constOrAbsent != null
          ? primitive(constOrAbsent, encoding)
          : primitive(buffer, index, encoding, typeToken.isOptionalEncoding()));
    }  }

  @Override
  public void onEnum(final Token fieldToken, final DirectBuffer buffer, final int index, final List<Token> tokens,
                     final int fromIndex, final int toIndex, final int actingVersion) {
    groupListener.onFieldIndex(index);

    final String name = determineName(0, fieldToken, tokens, fromIndex);
    if (fieldToken.isConstantEncoding()) {
      // constant enum fields reference a value as "EnumType.VALUE"
      final String ref = fieldToken.encoding().constValue().toString();
      current().put(name, ref.substring(ref.indexOf('.') + 1));
      return;
    }

    final Token typeToken = tokens.get(fromIndex + 1);
    final PrimitiveValue absent = constOrNotPresentValue(fieldToken, typeToken, actingVersion);
    final long raw = absent != null ? absent.longValue() : Types.getLong(buffer, index, typeToken.encoding());

    for (int i = fromIndex + 1; i < toIndex; i++) {
      final Token validValue = tokens.get(i);
      if (validValue.encoding().constValue().longValue() == raw) {
        current().put(name, validValue.name());
        return;
      }
    }

    final PrimitiveValue nullValue = typeToken.encoding().applicableNullValue();
    if (nullValue != null && nullValue.longValue() == raw) {
      current().putNull(name);
    } else if (typeToken.encoding().primitiveType() == PrimitiveType.CHAR) {
      // not in the schema (e.g. a newer producer); show the raw character rather than hiding it
      current().put(name, String.valueOf((char) raw));
    } else {
      current().put(name, raw);
    }  }

  @Override
  public void onBitSet(final Token fieldToken, final DirectBuffer buffer, final int index, final List<Token> tokens,
                       final int fromIndex, final int toIndex, final int actingVersion) {
    groupListener.onFieldIndex(index);

    final Token typeToken = tokens.get(fromIndex + 1);
    final PrimitiveValue absent = constOrNotPresentValue(fieldToken, typeToken, actingVersion);
    final long raw = absent != null ? absent.longValue() : Types.getLong(buffer, index, typeToken.encoding());

    final ArrayNode choices = current().putArray(determineName(0, fieldToken, tokens, fromIndex));
    for (int i = fromIndex + 1; i < toIndex; i++) {
      final Token choice = tokens.get(i);
      if ((raw & (1L << choice.encoding().constValue().longValue())) != 0) {
        choices.add(choice.name());
      }
    }  }

  @Override
  public void onBeginComposite(final Token fieldToken, final List<Token> tokens, final int fromIndex,
                               final int toIndex) {
    ++compositeLevel;
    stack.push(current().putObject(determineName(1, fieldToken, tokens, fromIndex)));  }

  @Override
  public void onEndComposite(final Token fieldToken, final List<Token> tokens, final int fromIndex,
                             final int toIndex) {
    --compositeLevel;
    stack.pop();  }

  @Override
  public void onGroupHeader(final Token token, final int numInGroup) {
    current().putArray(token.name());
    groupListener.onGroupHeader(token, numInGroup);
  }

  @Override
  public void onBeginGroup(final Token token, final int groupIndex, final int numInGroup) {
    stack.push(((ArrayNode) current().get(token.name())).addObject());
    groupListener.onBeginGroup();
  }

  @Override
  public void onEndGroup(final Token token, final int groupIndex, final int numInGroup) {
    stack.pop();
    groupListener.onEndGroup(groupIndex, numInGroup);
  }

  @Override
  public void onVarData(final Token fieldToken, final DirectBuffer buffer, final int index, final int length,
                        final Token typeToken) {
    buffer.boundsCheck(index, length);
    final byte[] bytes = new byte[length];
    buffer.getBytes(index, bytes);
    final String characterEncoding = typeToken.encoding().characterEncoding();
    current().put(fieldToken.name(), characterEncoding != null
        ? new String(bytes, Charset.forName(characterEncoding))
        : HexFormat.of().formatHex(bytes));
  }

  private ObjectNode current() {
    return stack.peek();
  }

  private String determineName(final int thresholdLevel, final Token fieldToken, final List<Token> tokens,
                               final int fromIndex) {
    return compositeLevel > thresholdLevel ? tokens.get(fromIndex).name() : fieldToken.name();
  }

  /**
   * The value to show without reading the buffer: a constant, or the null value of an optional field
   * that was added in a later schema version than the one the message was written with.
   */
  private static PrimitiveValue constOrNotPresentValue(final Token fieldToken, final Token typeToken,
                                                       final int actingVersion) {
    if (typeToken.isConstantEncoding()) {
      return typeToken.encoding().constValue();
    }
    if (fieldToken.isOptionalEncoding() && actingVersion < fieldToken.version()) {
      return typeToken.encoding().applicableNullValue();
    }
    return null;
  }

  private static String chars(final DirectBuffer buffer, final int index, final int length, final Encoding encoding) {
    int end = 0;
    while (end < length && buffer.getByte(index + end) != 0) {
      end++;
    }
    if (end == 0 && length == 1) {
      return null;
    }
    final byte[] bytes = new byte[end];
    buffer.getBytes(index, bytes);
    return new String(bytes, charset(encoding));
  }

  private static String charConstant(final PrimitiveValue value, final Encoding encoding) {
    if (value.representation() == PrimitiveValue.Representation.LONG) {
      final long c = value.longValue();
      return c == PrimitiveValue.NULL_VALUE_CHAR ? null : new String(new byte[] {(byte) c}, charset(encoding));
    }
    return value.toString();
  }

  private static Charset charset(final Encoding encoding) {
    return encoding.characterEncoding() != null
        ? Charset.forName(encoding.characterEncoding())
        : StandardCharsets.US_ASCII;
  }

  private static JsonNode primitive(
      final DirectBuffer buffer, final int index, final Encoding encoding, final boolean optional) {
    final PrimitiveType type = encoding.primitiveType();
    return switch (type) {
      case FLOAT -> floating(buffer.getFloat(index, encoding.byteOrder()), optional);
      case DOUBLE -> floating(buffer.getDouble(index, encoding.byteOrder()), optional);
      case CHAR -> {
        final byte c = buffer.getByte(index);
        yield c == 0 ? NODES.nullNode() : NODES.textNode(String.valueOf((char) c));
      }
      default -> {
        final long value = Types.getLong(buffer, index, encoding);
        if (optional && value == encoding.applicableNullValue().longValue()) {
          yield NODES.nullNode();
        }
        yield integer(value, type);
      }
    };
  }

  private static JsonNode primitive(final PrimitiveValue value, final Encoding encoding) {
    if (value.equals(encoding.applicableNullValue()) && encoding.presence() != Encoding.Presence.CONSTANT) {
      return NODES.nullNode();
    }
    return switch (value.representation()) {
      case DOUBLE -> floating(value.doubleValue(), false);
      case LONG -> integer(value.longValue(), encoding.primitiveType());
      case BYTE_ARRAY -> NODES.textNode(value.toString());
    };
  }

  private static JsonNode integer(final long value, final PrimitiveType type) {
    if (type == PrimitiveType.UINT64 && value < 0) {
      return NODES.numberNode(new BigInteger(Long.toUnsignedString(value)));
    }
    return NODES.numberNode(value);
  }

  private static JsonNode floating(final double value, final boolean optional) {
    if (Double.isNaN(value) && optional) {
      return NODES.nullNode();
    }
    // NaN and infinities are not valid JSON numbers
    return Double.isFinite(value) ? NODES.numberNode(value) : NODES.textNode(Double.toString(value));
  }
}
