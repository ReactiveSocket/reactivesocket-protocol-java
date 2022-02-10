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

import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;
import static io.rsocket.frame.FrameType.COMPLETE;
import static io.rsocket.frame.FrameType.ERROR;
import static io.rsocket.frame.FrameType.LEASE;
import static io.rsocket.frame.FrameType.REQUEST_CHANNEL;
import static io.rsocket.frame.FrameType.REQUEST_FNF;
import static io.rsocket.frame.FrameType.SETUP;
import static org.assertj.core.data.Offset.offset;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCounted;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.exceptions.Exceptions;
import io.rsocket.exceptions.RejectedException;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.LeaseFrameCodec;
import io.rsocket.frame.PayloadFrameCodec;
import io.rsocket.frame.RequestChannelFrameCodec;
import io.rsocket.frame.RequestFireAndForgetFrameCodec;
import io.rsocket.frame.RequestResponseFrameCodec;
import io.rsocket.frame.RequestStreamFrameCodec;
import io.rsocket.frame.SetupFrameCodec;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.subscriber.AssertSubscriber;
import io.rsocket.lease.Lease;
import io.rsocket.lease.MissingLeaseException;
import io.rsocket.plugins.InitializingInterceptorRegistry;
import io.rsocket.test.util.TestClientTransport;
import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.test.util.TestServerTransport;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.DefaultPayload;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

class RSocketLeaseTest {
  private static final String TAG = "test";

  private RSocket rSocketRequester;
  private ResponderLeaseTracker responderLeaseTracker;
  private LeaksTrackingByteBufAllocator byteBufAllocator;
  private TestDuplexConnection connection;
  private RSocketResponder rSocketResponder;
  private RSocket mockRSocketHandler;

  private Sinks.Many<Lease> leaseSender = Sinks.many().multicast().onBackpressureBuffer();
  private RequesterLeaseTracker requesterLeaseTracker;
  protected Sinks.Empty<Void> onGracefulShutdownStartedSink;
  protected Sinks.Empty<Void> otherGracefulShutdownSink;
  protected Sinks.Empty<Void> thisGracefulShutdownSink;
  protected Sinks.Empty<Void> thisClosedSink;
  protected Sinks.Empty<Void> otherClosedSink;

  @BeforeEach
  void setUp() {
    PayloadDecoder payloadDecoder = PayloadDecoder.DEFAULT;
    byteBufAllocator = LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);

    connection = new TestDuplexConnection(byteBufAllocator);
    requesterLeaseTracker = new RequesterLeaseTracker(TAG, 0);
    responderLeaseTracker = new ResponderLeaseTracker(TAG, connection, () -> leaseSender.asFlux());
    this.onGracefulShutdownStartedSink = Sinks.empty();
    this.otherGracefulShutdownSink = Sinks.empty();
    this.thisGracefulShutdownSink = Sinks.empty();
    this.thisClosedSink = Sinks.empty();
    this.otherClosedSink = Sinks.empty();

    ClientServerInputMultiplexer multiplexer =
        new ClientServerInputMultiplexer(connection, new InitializingInterceptorRegistry(), true);
    rSocketRequester =
        new RSocketRequester(
            multiplexer.asClientConnection(),
            payloadDecoder,
            StreamIdSupplier.clientSupplier(),
            0,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            0,
            0,
            null,
            __ -> null,
            requesterLeaseTracker,
            onGracefulShutdownStartedSink,
            thisGracefulShutdownSink,
            thisClosedSink,
            otherGracefulShutdownSink.asMono().and(thisGracefulShutdownSink.asMono()),
            otherClosedSink.asMono().and(thisClosedSink.asMono()));

    mockRSocketHandler = mock(RSocket.class);
    when(mockRSocketHandler.metadataPush(any()))
        .then(
            a -> {
              Payload payload = a.getArgument(0);
              payload.release();
              return Mono.empty();
            });
    when(mockRSocketHandler.fireAndForget(any()))
        .then(
            a -> {
              Payload payload = a.getArgument(0);
              payload.release();
              return Mono.empty();
            });
    when(mockRSocketHandler.requestResponse(any()))
        .then(
            a -> {
              Payload payload = a.getArgument(0);
              payload.release();
              return Mono.empty();
            });
    when(mockRSocketHandler.requestStream(any()))
        .then(
            a -> {
              Payload payload = a.getArgument(0);
              payload.release();
              return Flux.empty();
            });
    when(mockRSocketHandler.requestChannel(any()))
        .then(
            a -> {
              Publisher<Payload> payloadPublisher = a.getArgument(0);
              return Flux.from(payloadPublisher)
                  .doOnNext(ReferenceCounted::release)
                  .transform(
                      Operators.lift(
                          (__, actual) ->
                              new BaseSubscriber<Payload>() {
                                @Override
                                protected void hookOnSubscribe(Subscription subscription) {
                                  actual.onSubscribe(this);
                                }

                                @Override
                                protected void hookOnComplete() {
                                  actual.onComplete();
                                }

                                @Override
                                protected void hookOnError(Throwable throwable) {
                                  actual.onError(throwable);
                                }
                              }));
            });

    rSocketResponder =
        new RSocketResponder(
            multiplexer.asServerConnection(),
            mockRSocketHandler,
            payloadDecoder,
            responderLeaseTracker,
            0,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            __ -> null,
            otherGracefulShutdownSink,
            otherClosedSink,
            onGracefulShutdownStartedSink.asMono());
  }

  @AfterEach
  void tearDownAndCheckForLeaks() {
    byteBufAllocator.assertHasNoLeaks();
  }

  @Test
  public void serverRSocketFactoryRejectsUnsupportedLease() {
    Payload payload = DefaultPayload.create(DefaultPayload.EMPTY_BUFFER);
    ByteBuf setupFrame =
        SetupFrameCodec.encode(
            ByteBufAllocator.DEFAULT,
            true,
            1000,
            30_000,
            "application/octet-stream",
            "application/octet-stream",
            payload);

    TestServerTransport transport = new TestServerTransport();
    RSocketServer.create().bind(transport).block();

    TestDuplexConnection connection = transport.connect();
    connection.addToReceivedBuffer(setupFrame);

    Collection<ByteBuf> sent = connection.getSent();
    Assertions.assertThat(sent).hasSize(1);
    ByteBuf error = sent.iterator().next();
    Assertions.assertThat(FrameHeaderCodec.frameType(error)).isEqualTo(ERROR);
    Assertions.assertThat(Exceptions.from(0, error).getMessage())
        .isEqualTo("lease is not supported");
    error.release();
    connection.dispose();
    transport.alloc().assertHasNoLeaks();
  }

  @Test
  public void clientRSocketFactorySetsLeaseFlag() {
    TestClientTransport clientTransport = new TestClientTransport();
    try {
      RSocketConnector.create().lease().connect(clientTransport).block();
      Collection<ByteBuf> sent = clientTransport.testConnection().getSent();
      Assertions.assertThat(sent).hasSize(1);
      ByteBuf setup = sent.iterator().next();
      Assertions.assertThat(FrameHeaderCodec.frameType(setup)).isEqualTo(SETUP);
      Assertions.assertThat(SetupFrameCodec.honorLease(setup)).isTrue();
      setup.release();
    } finally {
      clientTransport.testConnection().dispose();
      clientTransport.alloc().assertHasNoLeaks();
    }
  }

  @ParameterizedTest
  @MethodSource("interactions")
  void requesterMissingLeaseRequestsAreRejected(
      BiFunction<RSocket, Payload, Publisher<?>> interaction) {
    Assertions.assertThat(rSocketRequester.availability()).isCloseTo(0.0, offset(1e-2));
    ByteBuf buffer = byteBufAllocator.buffer();
    buffer.writeCharSequence("test", CharsetUtil.UTF_8);
    Payload payload1 = ByteBufPayload.create(buffer);
    StepVerifier.create(interaction.apply(rSocketRequester, payload1))
        .expectError(MissingLeaseException.class)
        .verify(Duration.ofSeconds(5));

    byteBufAllocator.assertHasNoLeaks();
  }

  @ParameterizedTest
  @MethodSource("interactions")
  void requesterPresentLeaseRequestsAreAccepted(
      BiFunction<RSocket, Payload, Publisher<?>> interaction, FrameType frameType) {
    ByteBuf frame = leaseFrame(5_000, 2, Unpooled.EMPTY_BUFFER);
    requesterLeaseTracker.handleLeaseFrame(frame);

    Assertions.assertThat(rSocketRequester.availability()).isCloseTo(1.0, offset(1e-2));
    ByteBuf buffer = byteBufAllocator.buffer();
    buffer.writeCharSequence("test", CharsetUtil.UTF_8);
    Payload payload1 = ByteBufPayload.create(buffer);
    Flux.from(interaction.apply(rSocketRequester, payload1))
        .as(StepVerifier::create)
        .then(
            () -> {
              if (frameType != REQUEST_FNF) {
                connection.addToReceivedBuffer(
                    PayloadFrameCodec.encodeComplete(byteBufAllocator, 1));
              }
            })
        .expectComplete()
        .verify(Duration.ofSeconds(5));

    if (frameType == REQUEST_CHANNEL) {
      Assertions.assertThat(connection.getSent())
          .hasSize(2)
          .first()
          .matches(bb -> FrameHeaderCodec.frameType(bb) == frameType)
          .matches(ReferenceCounted::release);
      Assertions.assertThat(connection.getSent())
          .element(1)
          .matches(bb -> FrameHeaderCodec.frameType(bb) == COMPLETE)
          .matches(ReferenceCounted::release);
    } else {
      Assertions.assertThat(connection.getSent())
          .hasSize(1)
          .first()
          .matches(bb -> FrameHeaderCodec.frameType(bb) == frameType)
          .matches(ReferenceCounted::release);
    }

    Assertions.assertThat(rSocketRequester.availability()).isCloseTo(0.5, offset(1e-2));

    Assertions.assertThat(frame.release()).isTrue();

    byteBufAllocator.assertHasNoLeaks();
  }

  @ParameterizedTest
  @MethodSource("interactions")
  @SuppressWarnings({"rawtypes", "unchecked"})
  void requesterDepletedAllowedLeaseRequestsAreRejected(
      BiFunction<RSocket, Payload, Publisher<?>> interaction, FrameType interactionType) {
    ByteBuf buffer = byteBufAllocator.buffer();
    buffer.writeCharSequence("test", CharsetUtil.UTF_8);
    Payload payload1 = ByteBufPayload.create(buffer);
    ByteBuf leaseFrame = leaseFrame(5_000, 1, Unpooled.EMPTY_BUFFER);
    requesterLeaseTracker.handleLeaseFrame(leaseFrame);

    double initialAvailability = requesterLeaseTracker.availability();
    Publisher<?> request = interaction.apply(rSocketRequester, payload1);

    // ensures that lease is not used until the frame is sent
    Assertions.assertThat(initialAvailability).isEqualTo(requesterLeaseTracker.availability());
    Assertions.assertThat(connection.getSent()).hasSize(0);

    AssertSubscriber assertSubscriber = AssertSubscriber.create(0);
    request.subscribe(assertSubscriber);

    // if request is FNF, then request frame is sent on subscribe
    // otherwise we need to make request(1)
    if (interactionType != REQUEST_FNF) {
      Assertions.assertThat(initialAvailability).isEqualTo(requesterLeaseTracker.availability());
      Assertions.assertThat(connection.getSent()).hasSize(0);

      assertSubscriber.request(1);
    }

    // ensures availability is changed and lease is used only up on frame sending
    Assertions.assertThat(rSocketRequester.availability()).isCloseTo(0.0, offset(1e-2));

    if (interactionType == REQUEST_CHANNEL) {
      Assertions.assertThat(connection.getSent())
          .hasSize(2)
          .first()
          .matches(bb -> FrameHeaderCodec.frameType(bb) == interactionType)
          .matches(ReferenceCounted::release);
      Assertions.assertThat(connection.getSent())
          .element(1)
          .matches(bb -> FrameHeaderCodec.frameType(bb) == COMPLETE)
          .matches(ReferenceCounted::release);
    } else {
      Assertions.assertThat(connection.getSent())
          .hasSize(1)
          .first()
          .matches(bb -> FrameHeaderCodec.frameType(bb) == interactionType)
          .matches(ReferenceCounted::release);
    }

    ByteBuf buffer2 = byteBufAllocator.buffer();
    buffer2.writeCharSequence("test", CharsetUtil.UTF_8);
    Payload payload2 = ByteBufPayload.create(buffer2);
    Flux.from(interaction.apply(rSocketRequester, payload2))
        .as(StepVerifier::create)
        .expectError(MissingLeaseException.class)
        .verify(Duration.ofSeconds(5));

    Assertions.assertThat(leaseFrame.release()).isTrue();

    byteBufAllocator.assertHasNoLeaks();
  }

  @ParameterizedTest
  @MethodSource("interactions")
  void requesterExpiredLeaseRequestsAreRejected(
      BiFunction<RSocket, Payload, Publisher<?>> interaction) {
    ByteBuf frame = leaseFrame(50, 1, Unpooled.EMPTY_BUFFER);
    requesterLeaseTracker.handleLeaseFrame(frame);

    ByteBuf buffer = byteBufAllocator.buffer();
    buffer.writeCharSequence("test", CharsetUtil.UTF_8);
    Payload payload1 = ByteBufPayload.create(buffer);

    Flux.defer(() -> interaction.apply(rSocketRequester, payload1))
        .delaySubscription(Duration.ofMillis(200))
        .as(StepVerifier::create)
        .expectError(MissingLeaseException.class)
        .verify(Duration.ofSeconds(5));

    Assertions.assertThat(frame.release()).isTrue();

    byteBufAllocator.assertHasNoLeaks();
  }

  @Test
  void requesterAvailabilityRespectsTransport() {
    ByteBuf frame = leaseFrame(5_000, 1, Unpooled.EMPTY_BUFFER);
    try {

      requesterLeaseTracker.handleLeaseFrame(frame);
      double unavailable = 0.0;
      connection.setAvailability(unavailable);
      Assertions.assertThat(rSocketRequester.availability()).isCloseTo(unavailable, offset(1e-2));
    } finally {
      frame.release();
    }
  }

  @ParameterizedTest
  @MethodSource("responderInteractions")
  void responderMissingLeaseRequestsAreRejected(FrameType frameType) {
    ByteBuf buffer = byteBufAllocator.buffer();
    buffer.writeCharSequence("test", CharsetUtil.UTF_8);
    Payload payload1 = ByteBufPayload.create(buffer);

    switch (frameType) {
      case REQUEST_FNF:
        final ByteBuf fnfFrame =
            RequestFireAndForgetFrameCodec.encodeReleasingPayload(byteBufAllocator, 1, payload1);
        rSocketResponder.handleFrame(fnfFrame);
        fnfFrame.release();
        break;
      case REQUEST_RESPONSE:
        final ByteBuf requestResponseFrame =
            RequestResponseFrameCodec.encodeReleasingPayload(byteBufAllocator, 1, payload1);
        rSocketResponder.handleFrame(requestResponseFrame);
        requestResponseFrame.release();
        break;
      case REQUEST_STREAM:
        final ByteBuf requestStreamFrame =
            RequestStreamFrameCodec.encodeReleasingPayload(byteBufAllocator, 1, 1, payload1);
        rSocketResponder.handleFrame(requestStreamFrame);
        requestStreamFrame.release();
        break;
      case REQUEST_CHANNEL:
        final ByteBuf requestChannelFrame =
            RequestChannelFrameCodec.encodeReleasingPayload(byteBufAllocator, 1, true, 1, payload1);
        rSocketResponder.handleFrame(requestChannelFrame);
        requestChannelFrame.release();
        break;
    }

    if (frameType != REQUEST_FNF) {
      Assertions.assertThat(connection.getSent())
          .hasSize(1)
          .first()
          .matches(bb -> FrameHeaderCodec.frameType(bb) == ERROR)
          .matches(bb -> Exceptions.from(1, bb) instanceof RejectedException)
          .matches(ReferenceCounted::release);
    }

    byteBufAllocator.assertHasNoLeaks();
  }

  @ParameterizedTest
  @MethodSource("responderInteractions")
  void responderPresentLeaseRequestsAreAccepted(FrameType frameType) {
    leaseSender.tryEmitNext(Lease.create(Duration.ofMillis(5_000), 2));

    ByteBuf buffer = byteBufAllocator.buffer();
    buffer.writeCharSequence("test", CharsetUtil.UTF_8);
    Payload payload1 = ByteBufPayload.create(buffer);

    switch (frameType) {
      case REQUEST_FNF:
        final ByteBuf fnfFrame =
            RequestFireAndForgetFrameCodec.encodeReleasingPayload(byteBufAllocator, 1, payload1);
        rSocketResponder.handleFireAndForget(1, fnfFrame);
        fnfFrame.release();
        break;
      case REQUEST_RESPONSE:
        final ByteBuf requestResponseFrame =
            RequestResponseFrameCodec.encodeReleasingPayload(byteBufAllocator, 1, payload1);
        rSocketResponder.handleFrame(requestResponseFrame);
        requestResponseFrame.release();
        break;
      case REQUEST_STREAM:
        final ByteBuf requestStreamFrame =
            RequestStreamFrameCodec.encodeReleasingPayload(byteBufAllocator, 1, 1, payload1);
        rSocketResponder.handleFrame(requestStreamFrame);
        requestStreamFrame.release();
        break;
      case REQUEST_CHANNEL:
        final ByteBuf requestChannelFrame =
            RequestChannelFrameCodec.encodeReleasingPayload(byteBufAllocator, 1, true, 1, payload1);
        rSocketResponder.handleFrame(requestChannelFrame);
        requestChannelFrame.release();
        break;
    }

    switch (frameType) {
      case REQUEST_FNF:
        Mockito.verify(mockRSocketHandler).fireAndForget(any());
        break;
      case REQUEST_RESPONSE:
        Mockito.verify(mockRSocketHandler).requestResponse(any());
        break;
      case REQUEST_STREAM:
        Mockito.verify(mockRSocketHandler).requestStream(any());
        break;
      case REQUEST_CHANNEL:
        Mockito.verify(mockRSocketHandler).requestChannel(any());
        break;
    }

    Assertions.assertThat(connection.getSent())
        .first()
        .matches(bb -> FrameHeaderCodec.frameType(bb) == LEASE)
        .matches(ReferenceCounted::release);

    if (frameType != REQUEST_FNF) {
      Assertions.assertThat(connection.getSent())
          .hasSize(2)
          .element(1)
          .matches(bb -> FrameHeaderCodec.frameType(bb) == COMPLETE)
          .matches(ReferenceCounted::release);
    }

    byteBufAllocator.assertHasNoLeaks();
  }

  @ParameterizedTest
  @MethodSource("responderInteractions")
  void responderDepletedAllowedLeaseRequestsAreRejected(FrameType frameType) {
    leaseSender.tryEmitNext(Lease.create(Duration.ofMillis(5_000), 1));

    ByteBuf buffer = byteBufAllocator.buffer();
    buffer.writeCharSequence("test", CharsetUtil.UTF_8);
    Payload payload1 = ByteBufPayload.create(buffer);

    ByteBuf buffer2 = byteBufAllocator.buffer();
    buffer2.writeCharSequence("test2", CharsetUtil.UTF_8);
    Payload payload2 = ByteBufPayload.create(buffer2);

    switch (frameType) {
      case REQUEST_FNF:
        final ByteBuf fnfFrame =
            RequestFireAndForgetFrameCodec.encodeReleasingPayload(byteBufAllocator, 1, payload1);
        final ByteBuf fnfFrame2 =
            RequestFireAndForgetFrameCodec.encodeReleasingPayload(byteBufAllocator, 3, payload2);
        rSocketResponder.handleFrame(fnfFrame);
        rSocketResponder.handleFrame(fnfFrame2);
        fnfFrame.release();
        fnfFrame2.release();
        break;
      case REQUEST_RESPONSE:
        final ByteBuf requestResponseFrame =
            RequestResponseFrameCodec.encodeReleasingPayload(byteBufAllocator, 1, payload1);
        final ByteBuf requestResponseFrame2 =
            RequestResponseFrameCodec.encodeReleasingPayload(byteBufAllocator, 3, payload2);
        rSocketResponder.handleFrame(requestResponseFrame);
        rSocketResponder.handleFrame(requestResponseFrame2);
        requestResponseFrame.release();
        requestResponseFrame2.release();
        break;
      case REQUEST_STREAM:
        final ByteBuf requestStreamFrame =
            RequestStreamFrameCodec.encodeReleasingPayload(byteBufAllocator, 1, 1, payload1);
        final ByteBuf requestStreamFrame2 =
            RequestStreamFrameCodec.encodeReleasingPayload(byteBufAllocator, 3, 1, payload2);
        rSocketResponder.handleFrame(requestStreamFrame);
        rSocketResponder.handleFrame(requestStreamFrame2);
        requestStreamFrame.release();
        requestStreamFrame2.release();
        break;
      case REQUEST_CHANNEL:
        final ByteBuf requestChannelFrame =
            RequestChannelFrameCodec.encodeReleasingPayload(byteBufAllocator, 1, true, 1, payload1);
        final ByteBuf requestChannelFrame2 =
            RequestChannelFrameCodec.encodeReleasingPayload(byteBufAllocator, 3, true, 1, payload2);
        rSocketResponder.handleFrame(requestChannelFrame);
        rSocketResponder.handleFrame(requestChannelFrame2);
        requestChannelFrame.release();
        requestChannelFrame2.release();
        break;
    }

    switch (frameType) {
      case REQUEST_FNF:
        Mockito.verify(mockRSocketHandler).fireAndForget(any());
        break;
      case REQUEST_RESPONSE:
        Mockito.verify(mockRSocketHandler).requestResponse(any());
        break;
      case REQUEST_STREAM:
        Mockito.verify(mockRSocketHandler).requestStream(any());
        break;
      case REQUEST_CHANNEL:
        Mockito.verify(mockRSocketHandler).requestChannel(any());
        break;
    }

    Assertions.assertThat(connection.getSent())
        .first()
        .matches(bb -> FrameHeaderCodec.frameType(bb) == LEASE)
        .matches(ReferenceCounted::release);

    if (frameType != REQUEST_FNF) {
      Assertions.assertThat(connection.getSent())
          .hasSize(3)
          .element(1)
          .matches(bb -> FrameHeaderCodec.frameType(bb) == COMPLETE)
          .matches(ReferenceCounted::release);

      Assertions.assertThat(connection.getSent())
          .hasSize(3)
          .element(2)
          .matches(bb -> FrameHeaderCodec.frameType(bb) == ERROR)
          .matches(bb -> Exceptions.from(1, bb) instanceof RejectedException)
          .matches(ReferenceCounted::release);
    }

    byteBufAllocator.assertHasNoLeaks();
  }

  @ParameterizedTest
  @MethodSource("interactions")
  void expiredLeaseRequestsAreRejected(BiFunction<RSocket, Payload, Publisher<?>> interaction) {
    leaseSender.tryEmitNext(Lease.create(Duration.ofMillis(50), 1));

    ByteBuf buffer = byteBufAllocator.buffer();
    buffer.writeCharSequence("test", CharsetUtil.UTF_8);
    Payload payload1 = ByteBufPayload.create(buffer);

    Flux.from(interaction.apply(rSocketRequester, payload1))
        .delaySubscription(Duration.ofMillis(100))
        .as(StepVerifier::create)
        .expectError(MissingLeaseException.class)
        .verify(Duration.ofSeconds(5));

    Assertions.assertThat(connection.getSent())
        .hasSize(1)
        .first()
        .matches(bb -> FrameHeaderCodec.frameType(bb) == LEASE)
        .matches(ReferenceCounted::release);

    byteBufAllocator.assertHasNoLeaks();
  }

  @Test
  void sendLease() {
    ByteBuf metadata = byteBufAllocator.buffer();
    Charset utf8 = StandardCharsets.UTF_8;
    String metadataContent = "test";
    metadata.writeCharSequence(metadataContent, utf8);
    int ttl = 5_000;
    int numberOfRequests = 2;
    leaseSender.tryEmitNext(Lease.create(Duration.ofMillis(5_000), 2, metadata));

    ByteBuf leaseFrame =
        connection
            .getSent()
            .stream()
            .filter(f -> FrameHeaderCodec.frameType(f) == FrameType.LEASE)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Lease frame not sent"));

    try {
      Assertions.assertThat(LeaseFrameCodec.ttl(leaseFrame)).isEqualTo(ttl);
      Assertions.assertThat(LeaseFrameCodec.numRequests(leaseFrame)).isEqualTo(numberOfRequests);
      Assertions.assertThat(LeaseFrameCodec.metadata(leaseFrame).toString(utf8))
          .isEqualTo(metadataContent);
    } finally {
      leaseFrame.release();
    }
  }

  //  @Test
  //  void receiveLease() {
  //    Collection<Lease> receivedLeases = new ArrayList<>();
  //    leaseReceiver.subscribe(lease -> receivedLeases.add(lease));
  //
  //    ByteBuf metadata = byteBufAllocator.buffer();
  //    Charset utf8 = StandardCharsets.UTF_8;
  //    String metadataContent = "test";
  //    metadata.writeCharSequence(metadataContent, utf8);
  //    int ttl = 5_000;
  //    int numberOfRequests = 2;
  //
  //    ByteBuf leaseFrame = leaseFrame(ttl, numberOfRequests, metadata).retain(1);
  //
  //    connection.addToReceivedBuffer(leaseFrame);
  //
  //    Assertions.assertThat(receivedLeases.isEmpty()).isFalse();
  //    Lease receivedLease = receivedLeases.iterator().next();
  //    Assertions.assertThat(receivedLease.getTimeToLiveMillis()).isEqualTo(ttl);
  //
  // Assertions.assertThat(receivedLease.getStartingAllowedRequests()).isEqualTo(numberOfRequests);
  //    Assertions.assertThat(receivedLease.metadata().toString(utf8)).isEqualTo(metadataContent);
  //
  //    ReferenceCountUtil.safeRelease(leaseFrame);
  //  }

  ByteBuf leaseFrame(int ttl, int requests, ByteBuf metadata) {
    return LeaseFrameCodec.encode(byteBufAllocator, ttl, requests, metadata);
  }

  static Stream<Arguments> interactions() {
    return Stream.of(
        Arguments.of(
            (BiFunction<RSocket, Payload, Publisher<?>>) RSocket::fireAndForget,
            FrameType.REQUEST_FNF),
        Arguments.of(
            (BiFunction<RSocket, Payload, Publisher<?>>) RSocket::requestResponse,
            FrameType.REQUEST_RESPONSE),
        Arguments.of(
            (BiFunction<RSocket, Payload, Publisher<?>>) RSocket::requestStream,
            FrameType.REQUEST_STREAM),
        Arguments.of(
            (BiFunction<RSocket, Payload, Publisher<?>>)
                (rSocket, payload) -> rSocket.requestChannel(Mono.just(payload)),
            FrameType.REQUEST_CHANNEL));
  }

  static Stream<FrameType> responderInteractions() {
    return Stream.of(
        FrameType.REQUEST_FNF,
        FrameType.REQUEST_RESPONSE,
        FrameType.REQUEST_STREAM,
        FrameType.REQUEST_CHANNEL);
  }
}
