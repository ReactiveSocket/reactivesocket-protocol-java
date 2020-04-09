package io.rsocket.core;

import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.FrameLengthFlyweight;

final class PayloadValidationUtils {
  public static boolean isValid(int mtu, Payload payload) {
    return payload.hasMetadata()
        ? isValid(mtu, payload.data(), payload.metadata())
        : isValid(mtu, payload.data());
  }

  public static boolean isValid(int mtu, ByteBuf data) {
    return mtu > 0
        || (((FrameHeaderFlyweight.size()
                    + data.readableBytes()
                    + FrameLengthFlyweight.FRAME_LENGTH_SIZE)
                & ~FrameLengthFlyweight.FRAME_LENGTH_MASK)
            == 0);
  }

  public static boolean isValid(int mtu, ByteBuf data, ByteBuf metadata) {
    return mtu > 0
        || (((FrameHeaderFlyweight.size()
                    + FrameLengthFlyweight.FRAME_LENGTH_SIZE
                    + FrameHeaderFlyweight.size()
                    + data.readableBytes()
                    + metadata.readableBytes())
                & ~FrameLengthFlyweight.FRAME_LENGTH_MASK)
            == 0);
  }
}