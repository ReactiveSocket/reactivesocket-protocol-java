package io.rsocket.core;

import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;

import io.rsocket.DuplexConnection;
import io.rsocket.RSocket;
import io.rsocket.frame.decoder.PayloadDecoder;
import reactor.core.publisher.Sinks;
import reactor.util.annotation.Nullable;

public class TestRequesterResponderSupport extends RequesterResponderSupport implements RSocket {

  @Nullable private final RequesterLeaseTracker requesterLeaseTracker;

  public TestRequesterResponderSupport(
      DuplexConnection connection, StreamIdSupplier streamIdSupplier) {
    this(connection, streamIdSupplier, null);
  }

  public TestRequesterResponderSupport(
      DuplexConnection connection,
      StreamIdSupplier streamIdSupplier,
      @Nullable RequesterLeaseTracker requesterLeaseTracker) {
    super(
        0,
        FRAME_LENGTH_MASK,
        Integer.MAX_VALUE,
        PayloadDecoder.ZERO_COPY,
        connection,
        streamIdSupplier,
        __ -> null,
        Sinks.empty());
    this.requesterLeaseTracker = requesterLeaseTracker;
  }

  @Override
  @Nullable
  public RequesterLeaseTracker getRequesterLeaseTracker() {
    return this.requesterLeaseTracker;
  }
}
