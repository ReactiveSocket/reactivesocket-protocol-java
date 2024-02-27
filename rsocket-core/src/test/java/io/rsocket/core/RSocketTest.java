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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.exceptions.ApplicationErrorException;
import io.rsocket.exceptions.CustomRSocketException;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.subscriber.AssertSubscriber;
import io.rsocket.test.util.LocalDuplexConnection;
import io.rsocket.util.DefaultPayload;
import io.rsocket.util.EmptyPayload;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReference;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

public class RSocketTest {

  public final SocketRule rule = new SocketRule();

  @BeforeEach
  public void setup() {
    rule.init();
  }

  @AfterEach
  public void tearDownAndCheckOnLeaks() {
    rule.alloc().assertHasNoLeaks();
  }

  @Test
  public void rsocketDisposalShouldEndupWithNoErrorsOnClose() {
    RSocket requestHandlingRSocket =
        new RSocket() {
          final Disposable disposable = Disposables.single();

          @Override
          public void dispose() {
            disposable.dispose();
          }

          @Override
          public boolean isDisposed() {
            return disposable.isDisposed();
          }
        };
    rule.setRequestAcceptor(requestHandlingRSocket);
    rule.crs
        .onClose()
        .as(StepVerifier::create)
        .expectSubscription()
        .then(rule.crs::dispose)
        .expectComplete()
        .verify(Duration.ofMillis(100));

    Assertions.assertThat(requestHandlingRSocket.isDisposed()).isTrue();
  }

  @Test
  @Timeout(2_000)
  public void testRequestReplyNoError() {
    StepVerifier.create(rule.crs.requestResponse(DefaultPayload.create("hello")))
        .expectNextCount(1)
        .expectComplete()
        .verify();
  }

  @Test
  @Timeout(2000)
  public void testHandlerEmitsError() {
    rule.setRequestAcceptor(
        new RSocket() {
          @Override
          public Mono<Payload> requestResponse(Payload payload) {
            return Mono.error(new NullPointerException("Deliberate exception."));
          }
        });
    rule.crs
        .requestResponse(EmptyPayload.INSTANCE)
        .as(StepVerifier::create)
        .expectErrorSatisfies(
            t ->
                Assertions.assertThat(t)
                    .isInstanceOf(ApplicationErrorException.class)
                    .hasMessage("Deliberate exception."))
        .verify(Duration.ofMillis(100));
  }

  @Test
  @Timeout(2000)
  public void testHandlerEmitsCustomError() {
    rule.setRequestAcceptor(
        new RSocket() {
          @Override
          public Mono<Payload> requestResponse(Payload payload) {
            return Mono.error(
                new CustomRSocketException(0x00000501, "Deliberate Custom exception."));
          }
        });
    rule.crs
        .requestResponse(EmptyPayload.INSTANCE)
        .as(StepVerifier::create)
        .expectErrorSatisfies(
            t ->
                Assertions.assertThat(t)
                    .isInstanceOf(CustomRSocketException.class)
                    .hasMessage("Deliberate Custom exception.")
                    .hasFieldOrPropertyWithValue("errorCode", 0x00000501))
        .verify();
  }

  @Test
  @Timeout(2000)
  public void testRequestPropagatesCorrectlyForRequestChannel() {
    rule.setRequestAcceptor(
        new RSocket() {
          @Override
          public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            return Flux.from(payloads)
                // specifically limits request to 3 in order to prevent 256 request from limitRate
                // hidden on the responder side
                .take(3, true);
          }
        });

    Flux.range(0, 3)
        .map(i -> DefaultPayload.create("" + i))
        .as(rule.crs::requestChannel)
        .as(publisher -> StepVerifier.create(publisher, 3))
        .expectSubscription()
        .expectNextCount(3)
        .expectComplete()
        .verify(Duration.ofMillis(5000));
  }

  @Test
  @Timeout(2000)
  public void testStream() {
    Flux<Payload> responses = rule.crs.requestStream(DefaultPayload.create("Payload In"));
    StepVerifier.create(responses).expectNextCount(10).expectComplete().verify();
  }

  @Test
  @Timeout(200000)
  public void testChannel() {
    Flux<Payload> requests =
        Flux.range(0, 10).map(i -> DefaultPayload.create("streaming in -> " + i));
    Flux<Payload> responses = rule.crs.requestChannel(requests);
    StepVerifier.create(responses).expectNextCount(10).expectComplete().verify();
  }

  @Test
  @Timeout(2000)
  public void testErrorPropagatesCorrectly() {
    AtomicReference<Throwable> error = new AtomicReference<>();
    rule.setRequestAcceptor(
        new RSocket() {
          @Override
          public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            return Flux.from(payloads).doOnError(error::set);
          }
        });
    Flux<Payload> requests = Flux.error(new RuntimeException("test"));
    Flux<Payload> responses = rule.crs.requestChannel(requests);
    StepVerifier.create(responses).expectErrorMessage("test").verify();
    Assertions.assertThat(error.get()).isNull();
  }

  @Test
  public void requestChannelCase_StreamIsTerminatedAfterBothSidesSentCompletion1() {
    TestPublisher<Payload> requesterPublisher = TestPublisher.create();
    AssertSubscriber<Payload> requesterSubscriber = new AssertSubscriber<>(0);

    AssertSubscriber<Payload> responderSubscriber = new AssertSubscriber<>(0);
    TestPublisher<Payload> responderPublisher = TestPublisher.create();

    initRequestChannelCase(
        requesterPublisher, requesterSubscriber, responderPublisher, responderSubscriber);

    nextFromRequesterPublisher(requesterPublisher, responderSubscriber);

    completeFromRequesterPublisher(requesterPublisher, responderSubscriber);

    nextFromResponderPublisher(responderPublisher, requesterSubscriber);

    completeFromResponderPublisher(responderPublisher, requesterSubscriber);
  }

  @Test
  public void requestChannelCase_StreamIsTerminatedAfterBothSidesSentCompletion2() {
    TestPublisher<Payload> requesterPublisher = TestPublisher.create();
    AssertSubscriber<Payload> requesterSubscriber = new AssertSubscriber<>(0);

    AssertSubscriber<Payload> responderSubscriber = new AssertSubscriber<>(0);
    TestPublisher<Payload> responderPublisher = TestPublisher.create();

    initRequestChannelCase(
        requesterPublisher, requesterSubscriber, responderPublisher, responderSubscriber);

    nextFromResponderPublisher(responderPublisher, requesterSubscriber);

    completeFromResponderPublisher(responderPublisher, requesterSubscriber);

    nextFromRequesterPublisher(requesterPublisher, responderSubscriber);

    completeFromRequesterPublisher(requesterPublisher, responderSubscriber);
  }

  @Test
  public void
      requestChannelCase_CancellationFromResponderShouldLeaveStreamInHalfClosedStateWithNextCompletionPossibleFromRequester() {
    TestPublisher<Payload> requesterPublisher = TestPublisher.create();
    AssertSubscriber<Payload> requesterSubscriber = new AssertSubscriber<>(0);

    AssertSubscriber<Payload> responderSubscriber = new AssertSubscriber<>(0);
    TestPublisher<Payload> responderPublisher = TestPublisher.create();

    initRequestChannelCase(
        requesterPublisher, requesterSubscriber, responderPublisher, responderSubscriber);

    nextFromRequesterPublisher(requesterPublisher, responderSubscriber);

    cancelFromResponderSubscriber(requesterPublisher, responderSubscriber);

    nextFromResponderPublisher(responderPublisher, requesterSubscriber);

    completeFromResponderPublisher(responderPublisher, requesterSubscriber);
  }

  @Test
  public void
      requestChannelCase_CompletionFromRequesterShouldLeaveStreamInHalfClosedStateWithNextCancellationPossibleFromResponder() {
    TestPublisher<Payload> requesterPublisher = TestPublisher.create();
    AssertSubscriber<Payload> requesterSubscriber = new AssertSubscriber<>(0);

    AssertSubscriber<Payload> responderSubscriber = new AssertSubscriber<>(0);
    TestPublisher<Payload> responderPublisher = TestPublisher.create();

    initRequestChannelCase(
        requesterPublisher, requesterSubscriber, responderPublisher, responderSubscriber);

    nextFromResponderPublisher(responderPublisher, requesterSubscriber);

    completeFromResponderPublisher(responderPublisher, requesterSubscriber);

    nextFromRequesterPublisher(requesterPublisher, responderSubscriber);

    cancelFromResponderSubscriber(requesterPublisher, responderSubscriber);
  }

  @Test
  public void
      requestChannelCase_ensureThatRequesterSubscriberCancellationTerminatesStreamsOnBothSides() {
    TestPublisher<Payload> requesterPublisher = TestPublisher.create();
    AssertSubscriber<Payload> requesterSubscriber = new AssertSubscriber<>(0);

    AssertSubscriber<Payload> responderSubscriber = new AssertSubscriber<>(0);
    TestPublisher<Payload> responderPublisher = TestPublisher.create();

    initRequestChannelCase(
        requesterPublisher, requesterSubscriber, responderPublisher, responderSubscriber);

    nextFromResponderPublisher(responderPublisher, requesterSubscriber);

    nextFromRequesterPublisher(requesterPublisher, responderSubscriber);

    // ensures both sides are terminated
    cancelFromRequesterSubscriber(
        requesterPublisher, requesterSubscriber, responderPublisher, responderSubscriber);
  }

  @Test
  public void requestChannelCase_ErrorFromResponderShouldTerminatesStreamsOnBothSides() {
    TestPublisher<Payload> requesterPublisher = TestPublisher.create();
    AssertSubscriber<Payload> requesterSubscriber = new AssertSubscriber<>(0);

    AssertSubscriber<Payload> responderSubscriber = new AssertSubscriber<>(0);
    TestPublisher<Payload> responderPublisher = TestPublisher.create();

    initRequestChannelCase(
        requesterPublisher, requesterSubscriber, responderPublisher, responderSubscriber);

    nextFromResponderPublisher(responderPublisher, requesterSubscriber);

    nextFromRequesterPublisher(requesterPublisher, responderSubscriber);

    // ensures both sides are terminated
    errorFromResponderPublisher(
        requesterPublisher, requesterSubscriber, responderPublisher, responderSubscriber);
  }

  @Test
  public void requestChannelCase_ErrorFromRequesterShouldTerminatesStreamsOnBothSides() {
    TestPublisher<Payload> requesterPublisher = TestPublisher.create();
    AssertSubscriber<Payload> requesterSubscriber = new AssertSubscriber<>(0);

    AssertSubscriber<Payload> responderSubscriber = new AssertSubscriber<>(0);
    TestPublisher<Payload> responderPublisher = TestPublisher.create();

    initRequestChannelCase(
        requesterPublisher, requesterSubscriber, responderPublisher, responderSubscriber);

    nextFromResponderPublisher(responderPublisher, requesterSubscriber);

    nextFromRequesterPublisher(requesterPublisher, responderSubscriber);

    // ensures both sides are terminated
    errorFromRequesterPublisher(
        requesterPublisher, requesterSubscriber, responderPublisher, responderSubscriber);
  }

  void initRequestChannelCase(
      TestPublisher<Payload> requesterPublisher,
      AssertSubscriber<Payload> requesterSubscriber,
      TestPublisher<Payload> responderPublisher,
      AssertSubscriber<Payload> responderSubscriber) {
    rule.setRequestAcceptor(
        new RSocket() {
          @Override
          public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            payloads.subscribe(responderSubscriber);
            return responderPublisher.flux();
          }
        });

    rule.crs.requestChannel(requesterPublisher).subscribe(requesterSubscriber);

    requesterPublisher.assertWasSubscribed();
    requesterSubscriber.assertSubscribed();

    responderSubscriber.assertNotSubscribed();
    responderPublisher.assertWasNotSubscribed();

    // firstRequest
    requesterSubscriber.request(1);
    requesterPublisher.assertMaxRequested(1);
    requesterPublisher.next(DefaultPayload.create("initialData", "initialMetadata"));

    responderSubscriber.assertSubscribed();
    responderPublisher.assertWasSubscribed();
  }

  void nextFromRequesterPublisher(
      TestPublisher<Payload> requesterPublisher, AssertSubscriber<Payload> responderSubscriber) {
    // ensures that outerUpstream and innerSubscriber is not terminated so the requestChannel
    requesterPublisher.assertSubscribers(1);
    responderSubscriber.assertNotTerminated();

    responderSubscriber.request(6);
    requesterPublisher.next(
        DefaultPayload.create("d1", "m1"),
        DefaultPayload.create("d2"),
        DefaultPayload.create("d3", "m3"),
        DefaultPayload.create("d4"),
        DefaultPayload.create("d5", "m5"));

    List<Payload> innerPayloads = responderSubscriber.awaitAndAssertNextValueCount(6).values();
    Assertions.assertThat(innerPayloads.stream().map(Payload::getDataUtf8))
        .containsExactly("initialData", "d1", "d2", "d3", "d4", "d5");
    Assertions.assertThat(innerPayloads.stream().map(Payload::hasMetadata))
        .containsExactly(true, true, false, true, false, true);
    Assertions.assertThat(innerPayloads.stream().map(Payload::getMetadataUtf8))
        .containsExactly("initialMetadata", "m1", "", "m3", "", "m5");
  }

  void completeFromRequesterPublisher(
      TestPublisher<Payload> requesterPublisher, AssertSubscriber<Payload> responderSubscriber) {
    // ensures that after sending complete upstream part is closed
    requesterPublisher.complete();
    responderSubscriber.assertTerminated();
    requesterPublisher.assertNoSubscribers();
  }

  void cancelFromResponderSubscriber(
      TestPublisher<Payload> requesterPublisher, AssertSubscriber<Payload> responderSubscriber) {
    // ensures that after sending complete upstream part is closed
    responderSubscriber.cancel();
    requesterPublisher.assertWasCancelled();
    requesterPublisher.assertNoSubscribers();
  }

  void nextFromResponderPublisher(
      TestPublisher<Payload> responderPublisher, AssertSubscriber<Payload> requesterSubscriber) {
    // ensures that downstream is not terminated so the requestChannel state is half-closed
    responderPublisher.assertSubscribers(1);
    requesterSubscriber.assertNotTerminated();

    // ensures responderPublisher can send messages and outerSubscriber can receive them
    requesterSubscriber.request(5);
    responderPublisher.next(
        DefaultPayload.create("rd1", "rm1"),
        DefaultPayload.create("rd2"),
        DefaultPayload.create("rd3", "rm3"),
        DefaultPayload.create("rd4"),
        DefaultPayload.create("rd5", "rm5"));

    List<Payload> outerPayloads = requesterSubscriber.awaitAndAssertNextValueCount(5).values();
    Assertions.assertThat(outerPayloads.stream().map(Payload::getDataUtf8))
        .containsExactly("rd1", "rd2", "rd3", "rd4", "rd5");
    Assertions.assertThat(outerPayloads.stream().map(Payload::hasMetadata))
        .containsExactly(true, false, true, false, true);
    Assertions.assertThat(outerPayloads.stream().map(Payload::getMetadataUtf8))
        .containsExactly("rm1", "", "rm3", "", "rm5");
  }

  void completeFromResponderPublisher(
      TestPublisher<Payload> responderPublisher, AssertSubscriber<Payload> requesterSubscriber) {
    // ensures that after sending complete inner upstream is closed
    responderPublisher.complete();
    requesterSubscriber.assertTerminated();
    responderPublisher.assertNoSubscribers();
  }

  void cancelFromRequesterSubscriber(
      TestPublisher<Payload> requesterPublisher,
      AssertSubscriber<Payload> requesterSubscriber,
      TestPublisher<Payload> responderPublisher,
      AssertSubscriber<Payload> responderSubscriber) {
    // ensures that after sending cancel the whole requestChannel is terminated
    requesterSubscriber.cancel();
    // error should be propagated
    responderSubscriber.assertTerminated();
    responderPublisher.assertWasCancelled();
    responderPublisher.assertNoSubscribers();
    // ensures that cancellation is propagated to the actual upstream
    requesterPublisher.assertWasCancelled();
    requesterPublisher.assertNoSubscribers();
  }

  static final CustomRSocketException EXCEPTION = new CustomRSocketException(123456, "test");

  void errorFromResponderPublisher(
      TestPublisher<Payload> requesterPublisher,
      AssertSubscriber<Payload> requesterSubscriber,
      TestPublisher<Payload> responderPublisher,
      AssertSubscriber<Payload> responderSubscriber) {
    // ensures that after sending cancel the whole requestChannel is terminated
    responderPublisher.error(EXCEPTION);
    // error should be propagated
    responderSubscriber.assertTerminated().assertError(CancellationException.class);
    requesterSubscriber
        .assertTerminated()
        .assertError(CustomRSocketException.class)
        .assertErrorMessage("test");
    // ensures that cancellation is propagated to the actual upstream
    requesterPublisher.assertWasCancelled();
    requesterPublisher.assertNoSubscribers();
  }

  void errorFromRequesterPublisher(
      TestPublisher<Payload> requesterPublisher,
      AssertSubscriber<Payload> requesterSubscriber,
      TestPublisher<Payload> responderPublisher,
      AssertSubscriber<Payload> responderSubscriber) {
    // ensures that after sending cancel the whole requestChannel is terminated
    requesterPublisher.error(EXCEPTION);
    // error should be propagated
    responderSubscriber
        .assertTerminated()
        .assertError(CustomRSocketException.class)
        .assertErrorMessage("test");
    requesterSubscriber
        .assertTerminated()
        .assertError(CustomRSocketException.class)
        .assertErrorMessage("test");

    // ensures that cancellation is propagated to the actual upstream
    responderPublisher.assertWasCancelled();
    responderPublisher.assertNoSubscribers();
  }

  public static class SocketRule {

    Sinks.Many<ByteBuf> serverProcessor;
    Sinks.Many<ByteBuf> clientProcessor;
    private RSocketRequester crs;

    @SuppressWarnings("unused")
    private RSocketResponder srs;

    private RSocket requestAcceptor;

    private LeaksTrackingByteBufAllocator allocator;
    protected Sinks.Empty<Void> onGracefulShutdownStartedSink;
    protected Sinks.Empty<Void> otherGracefulShutdownSink;
    protected Sinks.Empty<Void> thisGracefulShutdownSink;
    protected Sinks.Empty<Void> thisClosedSink;
    protected Sinks.Empty<Void> otherClosedSink;

    public LeaksTrackingByteBufAllocator alloc() {
      return allocator;
    }

    public void init() {
      allocator = LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
      serverProcessor = Sinks.many().multicast().directBestEffort();
      clientProcessor = Sinks.many().multicast().directBestEffort();

      this.onGracefulShutdownStartedSink = Sinks.empty();
      this.otherGracefulShutdownSink = Sinks.empty();
      this.thisGracefulShutdownSink = Sinks.empty();
      this.thisClosedSink = Sinks.empty();
      this.otherClosedSink = Sinks.empty();

      LocalDuplexConnection serverConnection =
          new LocalDuplexConnection("server", allocator, clientProcessor, serverProcessor);
      LocalDuplexConnection clientConnection =
          new LocalDuplexConnection("client", allocator, serverProcessor, clientProcessor);

      clientConnection.onClose().doFinally(__ -> serverConnection.dispose()).subscribe();
      serverConnection.onClose().doFinally(__ -> clientConnection.dispose()).subscribe();

      requestAcceptor =
          null != requestAcceptor
              ? requestAcceptor
              : new RSocket() {
                @Override
                public Mono<Payload> requestResponse(Payload payload) {
                  return Mono.just(payload);
                }

                @Override
                public Flux<Payload> requestStream(Payload payload) {
                  return Flux.range(1, 10)
                      .map(i -> DefaultPayload.create("server got -> [" + payload + "]"));
                }

                @Override
                public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                  Flux.from(payloads)
                      .map(
                          payload ->
                              DefaultPayload.create("server got -> [" + payload.toString() + "]"))
                      .subscribe();

                  return Flux.range(1, 10)
                      .map(
                          payload ->
                              DefaultPayload.create("server got -> [" + payload.toString() + "]"));
                }
              };

      srs =
          new RSocketResponder(
              serverConnection,
              requestAcceptor,
              PayloadDecoder.DEFAULT,
              null,
              0,
              FRAME_LENGTH_MASK,
              Integer.MAX_VALUE,
              __ -> null,
              otherGracefulShutdownSink,
              otherClosedSink,
              onGracefulShutdownStartedSink.asMono());

      crs =
          new RSocketRequester(
              clientConnection,
              PayloadDecoder.DEFAULT,
              StreamIdSupplier.clientSupplier(),
              0,
              FRAME_LENGTH_MASK,
              Integer.MAX_VALUE,
              0,
              0,
              null,
              __ -> null,
              null,
              onGracefulShutdownStartedSink,
              thisGracefulShutdownSink,
              thisClosedSink,
              otherGracefulShutdownSink.asMono().and(thisGracefulShutdownSink.asMono()),
              otherClosedSink.asMono().and(thisClosedSink.asMono()));
    }

    public void setRequestAcceptor(RSocket requestAcceptor) {
      this.requestAcceptor = requestAcceptor;
      init();
    }
  }
}
