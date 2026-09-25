package io.kafbat.ui.serde.sbe;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * A read-only {@link DirectBuffer} over a byte array that doesn't use {@code Unsafe}.
 *
 * <p>Agrona's own buffers need {@code --add-exports java.base/jdk.internal.misc=ALL-UNNAMED}, which kafka-ui
 * isn't started with. SBE's on-the-fly decoder only reads primitives and byte ranges, so that is all this
 * implements; every read is bounds-checked, whatever {@code agrona.disable.bounds.checks} says.
 */
final class ByteArrayReadBuffer implements DirectBuffer {

  private static final VarHandle SHORT_LE = view(short[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle SHORT_BE = view(short[].class, ByteOrder.BIG_ENDIAN);
  private static final VarHandle CHAR_LE = view(char[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle CHAR_BE = view(char[].class, ByteOrder.BIG_ENDIAN);
  private static final VarHandle INT_LE = view(int[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle INT_BE = view(int[].class, ByteOrder.BIG_ENDIAN);
  private static final VarHandle LONG_LE = view(long[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle LONG_BE = view(long[].class, ByteOrder.BIG_ENDIAN);
  private static final VarHandle FLOAT_LE = view(float[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle FLOAT_BE = view(float[].class, ByteOrder.BIG_ENDIAN);
  private static final VarHandle DOUBLE_LE = view(double[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle DOUBLE_BE = view(double[].class, ByteOrder.BIG_ENDIAN);

  private final byte[] bytes;

  ByteArrayReadBuffer(final byte[] bytes) {
    this.bytes = bytes;
  }

  private static VarHandle view(final Class<?> arrayType, final ByteOrder order) {
    return MethodHandles.byteArrayViewVarHandle(arrayType, order);
  }

  private static boolean little(final ByteOrder order) {
    return order == ByteOrder.LITTLE_ENDIAN;
  }

  // ---- what the SBE decoder uses ----

  @Override
  public byte getByte(final int index) {
    return bytes[index];
  }

  @Override
  public short getShort(final int index, final ByteOrder order) {
    return (short) (little(order) ? SHORT_LE : SHORT_BE).get(bytes, index);
  }

  @Override
  public char getChar(final int index, final ByteOrder order) {
    return (char) (little(order) ? CHAR_LE : CHAR_BE).get(bytes, index);
  }

  @Override
  public int getInt(final int index, final ByteOrder order) {
    return (int) (little(order) ? INT_LE : INT_BE).get(bytes, index);
  }

  @Override
  public long getLong(final int index, final ByteOrder order) {
    return (long) (little(order) ? LONG_LE : LONG_BE).get(bytes, index);
  }

  @Override
  public float getFloat(final int index, final ByteOrder order) {
    return (float) (little(order) ? FLOAT_LE : FLOAT_BE).get(bytes, index);
  }

  @Override
  public double getDouble(final int index, final ByteOrder order) {
    return (double) (little(order) ? DOUBLE_LE : DOUBLE_BE).get(bytes, index);
  }

  @Override
  public short getShort(final int index) {
    return getShort(index, ByteOrder.nativeOrder());
  }

  @Override
  public char getChar(final int index) {
    return getChar(index, ByteOrder.nativeOrder());
  }

  @Override
  public int getInt(final int index) {
    return getInt(index, ByteOrder.nativeOrder());
  }

  @Override
  public long getLong(final int index) {
    return getLong(index, ByteOrder.nativeOrder());
  }

  @Override
  public float getFloat(final int index) {
    return getFloat(index, ByteOrder.nativeOrder());
  }

  @Override
  public double getDouble(final int index) {
    return getDouble(index, ByteOrder.nativeOrder());
  }

  @Override
  public void getBytes(final int index, final byte[] dst) {
    getBytes(index, dst, 0, dst.length);
  }

  @Override
  public void getBytes(final int index, final byte[] dst, final int offset, final int length) {
    boundsCheck(index, length);
    System.arraycopy(bytes, index, dst, offset, length);
  }

  @Override
  public void getBytes(final int index, final MutableDirectBuffer dstBuffer, final int dstIndex, final int length) {
    boundsCheck(index, length);
    dstBuffer.putBytes(dstIndex, bytes, index, length);
  }

  @Override
  public void getBytes(final int index, final ByteBuffer dstBuffer, final int length) {
    getBytes(index, dstBuffer, dstBuffer.position(), length);
    dstBuffer.position(dstBuffer.position() + length);
  }

  @Override
  public void getBytes(final int index, final ByteBuffer dstBuffer, final int dstOffset, final int length) {
    boundsCheck(index, length);
    dstBuffer.put(dstOffset, bytes, index, length);
  }

  @Override
  public String getStringWithoutLengthAscii(final int index, final int length) {
    boundsCheck(index, length);
    return new String(bytes, index, length, StandardCharsets.US_ASCII);
  }

  @Override
  public String getStringWithoutLengthUtf8(final int index, final int length) {
    boundsCheck(index, length);
    return new String(bytes, index, length, StandardCharsets.UTF_8);
  }

  @Override
  public int capacity() {
    return bytes.length;
  }

  @Override
  public void checkLimit(final int limit) {
    if (limit > bytes.length) {
      throw new IndexOutOfBoundsException("limit=" + limit + " is beyond capacity=" + bytes.length);
    }
  }

  @Override
  public void boundsCheck(final int index, final int length) {
    Objects.checkFromIndexSize(index, length, bytes.length);
  }

  @Override
  public byte[] byteArray() {
    return bytes;
  }

  @Override
  public ByteBuffer byteBuffer() {
    return null;
  }

  @Override
  public long addressOffset() {
    throw unsupported();
  }

  @Override
  public int wrapAdjustment() {
    return 0;
  }

  @Override
  public int compareTo(final DirectBuffer other) {
    final int length = Math.min(capacity(), other.capacity());
    for (int i = 0; i < length; i++) {
      final int cmp = Byte.compare(getByte(i), other.getByte(i));
      if (cmp != 0) {
        return cmp;
      }
    }
    return Integer.compare(capacity(), other.capacity());
  }

  // ---- not needed for decoding: the buffer is fixed to one array, and length-prefixed strings aren't SBE ----

  @Override
  public void wrap(final byte[] buffer) {
    throw unsupported();
  }

  @Override
  public void wrap(final byte[] buffer, final int offset, final int length) {
    throw unsupported();
  }

  @Override
  public void wrap(final ByteBuffer buffer) {
    throw unsupported();
  }

  @Override
  public void wrap(final ByteBuffer buffer, final int offset, final int length) {
    throw unsupported();
  }

  @Override
  public void wrap(final DirectBuffer buffer) {
    throw unsupported();
  }

  @Override
  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    throw unsupported();
  }

  @Override
  public void wrap(final long address, final int length) {
    throw unsupported();
  }

  @Override
  public int parseNaturalIntAscii(final int index, final int length) {
    throw unsupported();
  }

  @Override
  public long parseNaturalLongAscii(final int index, final int length) {
    throw unsupported();
  }

  @Override
  public int parseIntAscii(final int index, final int length) {
    throw unsupported();
  }

  @Override
  public long parseLongAscii(final int index, final int length) {
    throw unsupported();
  }

  @Override
  public String getStringAscii(final int index) {
    throw unsupported();
  }

  @Override
  public int getStringAscii(final int index, final Appendable appendable) {
    throw unsupported();
  }

  @Override
  public String getStringAscii(final int index, final ByteOrder byteOrder) {
    throw unsupported();
  }

  @Override
  public int getStringAscii(final int index, final Appendable appendable, final ByteOrder byteOrder) {
    throw unsupported();
  }

  @Override
  public String getStringAscii(final int index, final int length) {
    throw unsupported();
  }

  @Override
  public int getStringAscii(final int index, final int length, final Appendable appendable) {
    throw unsupported();
  }

  @Override
  public int getStringWithoutLengthAscii(final int index, final int length, final Appendable appendable) {
    throw unsupported();
  }

  @Override
  public String getStringUtf8(final int index) {
    throw unsupported();
  }

  @Override
  public String getStringUtf8(final int index, final ByteOrder byteOrder) {
    throw unsupported();
  }

  @Override
  public String getStringUtf8(final int index, final int length) {
    throw unsupported();
  }

  private static UnsupportedOperationException unsupported() {
    return new UnsupportedOperationException("read-only byte array buffer for SBE decoding");
  }
}
