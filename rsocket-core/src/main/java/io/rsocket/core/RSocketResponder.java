/*
 * Copyright 2015-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.core;

import io.netty.buffer.ByteBuf;
import io.netty.util.collection.IntObjectMap;
import io.rsocket.DuplexConnection;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.RequestChannelFrameCodec;
import io.rsocket.frame.RequestFireAndForgetFrameCodec;
import io.rsocket.frame.RequestNFrameCodec;
import io.rsocket.frame.RequestResponseFrameCodec;
import io.rsocket.frame.RequestStreamFrameCodec;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.plugins.RequestInterceptor;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.annotation.Nullable;

/** Responder side of RSocket. Receives {@link ByteBuf}s from a peer's {@link RSocketRequester} */
class RSocketResponder extends RequesterResponderSupport implements RSocket {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketResponder.class);

  private static final Exception CLOSED_CHANNEL_EXCEPTION = new ClosedChannelException();

  private final RSocket requestHandler;
  private final Sinks.Empty<Void> onThisSideClosedSink;

  @Nullable private final ResponderLeaseTracker leaseHandler;

  private volatile Throwable terminationError;
  private static final AtomicReferenceFieldUpdater<RSocketResponder, Throwable> TERMINATION_ERROR =
      AtomicReferenceFieldUpdater.newUpdater(
          RSocketResponder.class, Throwable.class, "terminationError");

  RSocketResponder(
      DuplexConnection connection,
      RSocket requestHandler,
      PayloadDecoder payloadDecoder,
      @Nullable ResponderLeaseTracker leaseHandler,
      int mtu,
      int maxFrameLength,
      int maxInboundPayloadSize,
      Function<RSocket, ? extends RequestInterceptor> requestInterceptorFunction,
      Sinks.Empty<Void> onGracefulShutdownSink,
      Sinks.Empty<Void> onThisSideClosedSink,
      Mono<Void> onRequesterGracefulShutdownStarted) {
    super(
        mtu,
        maxFrameLength,
        maxInboundPayloadSize,
        payloadDecoder,
        connection,
        null,
        requestInterceptorFunction,
        onGracefulShutdownSink);

    this.requestHandler = requestHandler;

    this.leaseHandler = leaseHandler;
    this.onThisSideClosedSink = onThisSideClosedSink;

    connection
        .onClose()
        .subscribe(null, this::tryTerminateOnConnectionError, this::tryTerminateOnConnectionClose);

    onRequesterGracefulShutdownStarted.subscribe(null, null, this::onGracefulShutdownStarted);

    connection.receive().subscribe(this::handleFrame, e -> {});
  }

  private void onGracefulShutdownStarted() {
    super.terminate();
    requestHandler.disposeGracefully();
  }

  private void tryTerminateOnConnectionError(Throwable e) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Try terminate connection on responder side");
    }
    tryTerminate(() -> e);
  }

  private void tryTerminateOnConnectionClose() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.info("Try terminate connection on responder side");
    }
    tryTerminate(() -> CLOSED_CHANNEL_EXCEPTION);
  }

  private void tryTerminate(Supplier<Throwable> errorSupplier) {
    if (terminationError == null) {
      Throwable e = errorSupplier.get();
      if (TERMINATION_ERROR.compareAndSet(this, null, e)) {
        doOnDispose();
      }
    }
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    try {
      return requestHandler.fireAndForget(payload);
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    try {
      return requestHandler.requestResponse(payload);
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    try {
      return requestHandler.requestStream(payload);
    } catch (Throwable t) {
      return Flux.error(t);
    }
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    try {
      return requestHandler.requestChannel(payloads);
    } catch (Throwable t) {
      return Flux.error(t);
    }
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    try {
      return requestHandler.metadataPush(payload);
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public void dispose() {
    tryTerminate(() -> new CancellationException("Disposed"));
  }

  @Override
  public boolean isDisposed() {
    return getDuplexConnection().isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return getDuplexConnection().onClose();
  }

  final void doOnDispose() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("closing responder " + getDuplexConnection());
    }
    cleanUpSendingSubscriptions();

    getDuplexConnection().dispose();
    final RequestInterceptor requestInterceptor = getRequestInterceptor();
    if (requestInterceptor != null) {
      requestInterceptor.dispose();
    }

    final ResponderLeaseTracker handler = leaseHandler;
    if (handler != null) {
      handler.dispose();
    }

    requestHandler.dispose();
    onThisSideClosedSink.tryEmitEmpty();
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("responder closed " + getDuplexConnection());
    }
  }

  private void cleanUpSendingSubscriptions() {
    final Collection<FrameHandler> activeStreamsCopy;
    synchronized (this) {
      final IntObjectMap<FrameHandler> activeStreams = this.activeStreams;
      activeStreamsCopy = new ArrayList<>(activeStreams.values());
    }

    for (FrameHandler handler : activeStreamsCopy) {
      if (handler != null) {
        handler.handleCancel();
      }
    }
  }

  final void handleFrame(ByteBuf frame) {
    try {
      int streamId = FrameHeaderCodec.streamId(frame);
      FrameHandler receiver;
      FrameType frameType = FrameHeaderCodec.frameType(frame);
      switch (frameType) {
        case REQUEST_FNF:
          handleFireAndForget(streamId, frame);
          break;
        case REQUEST_RESPONSE:
          handleRequestResponse(streamId, frame);
          break;
        case REQUEST_STREAM:
          long streamInitialRequestN = RequestStreamFrameCodec.initialRequestN(frame);
          handleStream(streamId, frame, streamInitialRequestN);
          break;
        case REQUEST_CHANNEL:
          long channelInitialRequestN = RequestChannelFrameCodec.initialRequestN(frame);
          handleChannel(
              streamId, frame, channelInitialRequestN, FrameHeaderCodec.hasComplete(frame));
          break;
        case METADATA_PUSH:
          handleMetadataPush(metadataPush(super.getPayloadDecoder().apply(frame)));
          break;
        case CANCEL:
          receiver = super.get(streamId);
          if (receiver != null) {
            receiver.handleCancel();
          }
          break;
        case REQUEST_N:
          receiver = super.get(streamId);
          if (receiver != null) {
            long n = RequestNFrameCodec.requestN(frame);
            receiver.handleRequestN(n);
          }
          break;
        case PAYLOAD:
          // TODO: Hook in receiving socket.
          break;
        case NEXT:
          receiver = super.get(streamId);
          if (receiver != null) {
            boolean hasFollows = FrameHeaderCodec.hasFollows(frame);
            receiver.handleNext(frame, hasFollows, false);
          }
          break;
        case COMPLETE:
          receiver = super.get(streamId);
          if (receiver != null) {
            receiver.handleComplete();
          }
          break;
        case ERROR:
          receiver = super.get(streamId);
          if (receiver != null) {
            receiver.handleError(io.rsocket.exceptions.Exceptions.from(streamId, frame));
          }
          break;
        case NEXT_COMPLETE:
          receiver = super.get(streamId);
          if (receiver != null) {
            receiver.handleNext(frame, false, true);
          }
          break;
        case SETUP:
          getDuplexConnection()
              .sendFrame(
                  streamId,
                  ErrorFrameCodec.encode(
                      super.getAllocator(),
                      streamId,
                      new IllegalStateException("Setup frame received post setup.")));
          break;
        case LEASE:
        default:
          getDuplexConnection()
              .sendFrame(
                  streamId,
                  ErrorFrameCodec.encode(
                      super.getAllocator(),
                      streamId,
                      new IllegalStateException(
                          "ServerRSocket: Unexpected frame type: " + frameType)));
          break;
      }
    } catch (Throwable t) {
      LOGGER.error("Unexpected error during frame handling", t);
      getDuplexConnection()
          .sendFrame(
              0,
              ErrorFrameCodec.encode(
                  super.getAllocator(),
                  0,
                  new ConnectionErrorException("Unexpected error during frame handling", t)));
      this.tryTerminateOnConnectionError(t);
    }
  }

  final void handleFireAndForget(int streamId, ByteBuf frame) {
    ResponderLeaseTracker leaseHandler = this.leaseHandler;
    Throwable leaseError;
    if (leaseHandler == null || (leaseError = leaseHandler.use()) == null) {
      if (FrameHeaderCodec.hasFollows(frame)) {
        final RequestInterceptor requestInterceptor = this.getRequestInterceptor();
        if (requestInterceptor != null) {
          requestInterceptor.onStart(
              streamId, FrameType.REQUEST_FNF, RequestFireAndForgetFrameCodec.metadata(frame));
        }

        FireAndForgetResponderSubscriber subscriber =
            new FireAndForgetResponderSubscriber(streamId, frame, this, this);

        this.add(streamId, subscriber);
      } else {
        final RequestInterceptor requestInterceptor = this.getRequestInterceptor();
        if (requestInterceptor != null) {
          requestInterceptor.onStart(
              streamId, FrameType.REQUEST_FNF, RequestFireAndForgetFrameCodec.metadata(frame));

          fireAndForget(super.getPayloadDecoder().apply(frame))
              .subscribe(new FireAndForgetResponderSubscriber(streamId, this));
        } else {
          fireAndForget(super.getPayloadDecoder().apply(frame))
              .subscribe(FireAndForgetResponderSubscriber.INSTANCE);
        }
      }
    } else {
      final RequestInterceptor requestTracker = this.getRequestInterceptor();
      if (requestTracker != null) {
        requestTracker.onReject(
            leaseError, FrameType.REQUEST_FNF, RequestFireAndForgetFrameCodec.metadata(frame));
      }
    }
  }

  final void handleRequestResponse(int streamId, ByteBuf frame) {
    ResponderLeaseTracker leaseHandler = this.leaseHandler;
    Throwable leaseError;
    if (leaseHandler == null || (leaseError = leaseHandler.use()) == null) {
      final RequestInterceptor requestInterceptor = this.getRequestInterceptor();
      if (requestInterceptor != null) {
        requestInterceptor.onStart(
            streamId, FrameType.REQUEST_RESPONSE, RequestResponseFrameCodec.metadata(frame));
      }

      if (FrameHeaderCodec.hasFollows(frame)) {
        RequestResponseResponderSubscriber subscriber =
            new RequestResponseResponderSubscriber(streamId, frame, this, this);

        this.add(streamId, subscriber);
      } else {
        RequestResponseResponderSubscriber subscriber =
            new RequestResponseResponderSubscriber(streamId, this);

        if (this.add(streamId, subscriber)) {
          this.requestResponse(super.getPayloadDecoder().apply(frame)).subscribe(subscriber);
        }
      }
    } else {
      final RequestInterceptor requestInterceptor = this.getRequestInterceptor();
      if (requestInterceptor != null) {
        requestInterceptor.onReject(
            leaseError, FrameType.REQUEST_RESPONSE, RequestResponseFrameCodec.metadata(frame));
      }
      sendLeaseRejection(streamId, leaseError);
    }
  }

  final void handleStream(int streamId, ByteBuf frame, long initialRequestN) {
    ResponderLeaseTracker leaseHandler = this.leaseHandler;
    Throwable leaseError;
    if (leaseHandler == null || (leaseError = leaseHandler.use()) == null) {
      final RequestInterceptor requestInterceptor = this.getRequestInterceptor();
      if (requestInterceptor != null) {
        requestInterceptor.onStart(
            streamId, FrameType.REQUEST_STREAM, RequestStreamFrameCodec.metadata(frame));
      }

      if (FrameHeaderCodec.hasFollows(frame)) {
        RequestStreamResponderSubscriber subscriber =
            new RequestStreamResponderSubscriber(streamId, initialRequestN, frame, this, this);

        this.add(streamId, subscriber);
      } else {
        RequestStreamResponderSubscriber subscriber =
            new RequestStreamResponderSubscriber(streamId, initialRequestN, this);

        if (this.add(streamId, subscriber)) {
          this.requestStream(super.getPayloadDecoder().apply(frame)).subscribe(subscriber);
        }
      }
    } else {
      final RequestInterceptor requestInterceptor = this.getRequestInterceptor();
      if (requestInterceptor != null) {
        requestInterceptor.onReject(
            leaseError, FrameType.REQUEST_STREAM, RequestStreamFrameCodec.metadata(frame));
      }
      sendLeaseRejection(streamId, leaseError);
    }
  }

  final void handleChannel(int streamId, ByteBuf frame, long initialRequestN, boolean complete) {
    ResponderLeaseTracker leaseHandler = this.leaseHandler;
    Throwable leaseError;
    if (leaseHandler == null || (leaseError = leaseHandler.use()) == null) {
      final RequestInterceptor requestInterceptor = this.getRequestInterceptor();
      if (requestInterceptor != null) {
        requestInterceptor.onStart(
            streamId, FrameType.REQUEST_CHANNEL, RequestChannelFrameCodec.metadata(frame));
      }

      if (FrameHeaderCodec.hasFollows(frame)) {
        RequestChannelResponderSubscriber subscriber =
            new RequestChannelResponderSubscriber(streamId, initialRequestN, frame, this, this);

        this.add(streamId, subscriber);
      } else {
        final Payload firstPayload = super.getPayloadDecoder().apply(frame);
        RequestChannelResponderSubscriber subscriber =
            new RequestChannelResponderSubscriber(streamId, initialRequestN, firstPayload, this);

        if (this.add(streamId, subscriber)) {
          this.requestChannel(subscriber).subscribe(subscriber);
          if (complete) {
            subscriber.handleComplete();
          }
        }
      }
    } else {
      final RequestInterceptor requestTracker = this.getRequestInterceptor();
      if (requestTracker != null) {
        requestTracker.onReject(
            leaseError, FrameType.REQUEST_CHANNEL, RequestChannelFrameCodec.metadata(frame));
      }
      sendLeaseRejection(streamId, leaseError);
    }
  }

  private void sendLeaseRejection(int streamId, Throwable leaseError) {
    getDuplexConnection()
        .sendFrame(streamId, ErrorFrameCodec.encode(getAllocator(), streamId, leaseError));
  }

  private void handleMetadataPush(Mono<Void> result) {
    result.subscribe(MetadataPushResponderSubscriber.INSTANCE);
  }

  @Override
  public boolean add(int streamId, FrameHandler frameHandler) {
    if (!super.add(streamId, frameHandler)) {
      frameHandler.handleCancel();
      return false;
    }

    return true;
  }
}
