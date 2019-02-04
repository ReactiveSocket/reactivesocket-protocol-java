package io.rsocket.frame.decoder;

import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;

import java.util.function.Function;

public interface FrameDecoder extends Function<ByteBuf, Payload> {
  FrameDecoder DEFAULT = new DefaultFrameDecoder();
  FrameDecoder ZERO_COPY = new ZeroCopyFrameDecoder();
}
