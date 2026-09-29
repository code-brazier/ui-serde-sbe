package io.github.codebrazier.kafbat.sbe;

import java.math.BigInteger;
import java.util.Arrays;
import org.agrona.ExpandableArrayBuffer;
import test.trading.BigDecimalValueEncoder;
import test.trading.FillEncoder;
import test.trading.HeartbeatEncoder;
import test.trading.MessageHeaderEncoder;
import test.trading.OptionalDecimalEncoder;
import test.trading.OrderEncoder;
import test.trading.Side;
import test.trading.Status;

/**
 * Messages from {@code trading.xml}, encoded with the generated codecs as a real producer would.
 * Needs {@code --add-exports java.base/jdk.internal.misc=ALL-UNNAMED} (Agrona's buffers).
 */
final class TestMessages {

  private TestMessages() {
  }

  /**
   * An Order with every field set; header version 1 predates the {@code tag} field.
   */
  static byte[] order(final int headerVersion) {
    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    final MessageHeaderEncoder header = new MessageHeaderEncoder();
    final OrderEncoder order = new OrderEncoder().wrapAndApplyHeader(buffer, 0, header);
    header.version(headerVersion);

    order.orderId(-2L)
        .symbol("BTC-USD")
        .side(Side.ASK)
        .status(Status.FILLED);
    order.flags().clear().hidden(true).postOnly(true);
    order.price().mantissa(12345L).exponent((byte) -2);
    order.stopPrice().mantissa(OptionalDecimalEncoder.mantissaNullValue()).exponent((byte) 0);
    putScaled(order.quantity(), BigInteger.valueOf(-5), 3);
    order.displayQuantity(OrderEncoder.displayQuantityNullValue());
    order.tag(7);

    final FillEncoder fill = order.fillsCount(1).next().executionId(99L).fill();
    final byte[] fillPrice = BigInteger.valueOf(15).toByteArray();
    fill.priceLength((short) fillPrice.length);
    for (int i = 0; i < fillPrice.length; i++) {
      fill.priceMantissa(i, fillPrice[i]);
    }
    fill.priceExponent((short) 1).venue("VENUE1");

    order.note("hello ✓");
    return Arrays.copyOf(buffer.byteArray(), MessageHeaderEncoder.ENCODED_LENGTH + order.encodedLength());
  }

  private static void putScaled(final BigDecimalValueEncoder encoder, final BigInteger mantissa, final int scale) {
    final byte[] bytes = mantissa.toByteArray();
    encoder.length((short) bytes.length).exponent((short) scale);
    for (int i = 0; i < bytes.length; i++) {
      encoder.mantissa(i, bytes[i]);
    }
  }

  static byte[] heartbeat(final long timestamp) {
    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    final HeartbeatEncoder heartbeat = new HeartbeatEncoder().wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());
    heartbeat.timestamp(timestamp);
    return Arrays.copyOf(buffer.byteArray(), MessageHeaderEncoder.ENCODED_LENGTH + heartbeat.encodedLength());
  }
}
