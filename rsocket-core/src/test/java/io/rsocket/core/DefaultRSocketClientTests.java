package io.rsocket.core;
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

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.rsocket.FrameAssert;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RaceTestConstants;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.PayloadFrameCodec;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.subscriber.AssertSubscriber;
import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.RSocketProxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.util.RaceTestUtils;
import reactor.util.context.Context;
import reactor.util.context.ContextView;
import reactor.util.retry.Retry;

public class DefaultRSocketClientTests {

  ClientSocketRule rule;

  @BeforeEach
  public void setUp() throws Throwable {
    Hooks.onNextDropped(ReferenceCountUtil::safeRelease);
    Hooks.onErrorDropped((t) -> {});
    rule = new ClientSocketRule();
    rule.init();
  }

  @AfterEach
  public void tearDown() {
    Hooks.resetOnErrorDropped();
    Hooks.resetOnNextDropped();
    rule.allocator.assertHasNoLeaks();
  }

  @Test
  @SuppressWarnings("unchecked")
  void discardElementsConsumerShouldAcceptOtherTypesThanReferenceCounted() {
    Consumer discardElementsConsumer = DefaultRSocketClient.DISCARD_ELEMENTS_CONSUMER;
    discardElementsConsumer.accept(new Object());
  }

  @Test
  void droppedElementsConsumerReleaseReference() {
    ReferenceCounted referenceCounted = Mockito.mock(ReferenceCounted.class);
    Mockito.when(referenceCounted.release()).thenReturn(true);
    Mockito.when(referenceCounted.refCnt()).thenReturn(1);

    Consumer discardElementsConsumer = DefaultRSocketClient.DISCARD_ELEMENTS_CONSUMER;
    discardElementsConsumer.accept(referenceCounted);

    Mockito.verify(referenceCounted).release();
  }

  static Stream<Arguments> interactions() {
    return Stream.of(
        Arguments.of(
            (BiFunction<RSocketClient, Publisher<Payload>, Publisher<?>>)
                (client, payload) -> client.fireAndForget(Mono.fromDirect(payload)),
            FrameType.REQUEST_FNF),
        Arguments.of(
            (BiFunction<RSocketClient, Publisher<Payload>, Publisher<?>>)
                (client, payload) -> client.requestResponse(Mono.fromDirect(payload)),
            FrameType.REQUEST_RESPONSE),
        Arguments.of(
            (BiFunction<RSocketClient, Publisher<Payload>, Publisher<?>>)
                (client, payload) -> client.requestStream(Mono.fromDirect(payload)),
            FrameType.REQUEST_STREAM),
        Arguments.of(
            (BiFunction<RSocketClient, Publisher<Payload>, Publisher<?>>)
                RSocketClient::requestChannel,
            FrameType.REQUEST_CHANNEL),
        Arguments.of(
            (BiFunction<RSocketClient, Publisher<Payload>, Publisher<?>>)
                (client, payload) -> client.metadataPush(Mono.fromDirect(payload)),
            FrameType.METADATA_PUSH));
  }

  @ParameterizedTest
  @MethodSource("interactions")
  public void shouldSentFrameOnResolution(
      BiFunction<RSocketClient, Publisher<Payload>, Publisher<?>> request, FrameType requestType) {
    Payload payload = ByteBufPayload.create("test", "testMetadata");
    TestPublisher<Payload> testPublisher =
        TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION);

    Publisher<?> publisher = request.apply(rule.client, testPublisher);

    StepVerifier.create(publisher)
        .expectSubscription()
        .then(() -> Assertions.assertThat(rule.connection.getSent()).isEmpty())
        .then(
            () -> {
              if (requestType != FrameType.REQUEST_CHANNEL) {
                testPublisher.next(payload);
              }
            })
        .then(() -> rule.delayer.run())
        .then(
            () -> {
              if (requestType == FrameType.REQUEST_CHANNEL) {
                testPublisher.next(payload);
              }
            })
        .then(testPublisher::complete)
        .then(
            () -> {
              if (requestType == FrameType.REQUEST_CHANNEL) {
                Assertions.assertThat(rule.connection.getSent())
                    .hasSize(2)
                    .first()
                    .matches(bb -> FrameHeaderCodec.frameType(bb).equals(requestType))
                    .matches(ReferenceCounted::release);

                Assertions.assertThat(rule.connection.getSent())
                    .element(1)
                    .matches(bb -> FrameHeaderCodec.frameType(bb).equals(FrameType.COMPLETE))
                    .matches(ReferenceCounted::release);
              } else {
                Assertions.assertThat(rule.connection.getSent())
                    .hasSize(1)
                    .first()
                    .matches(bb -> FrameHeaderCodec.frameType(bb).equals(requestType))
                    .matches(ReferenceCounted::release);
              }
            })
        .then(
            () -> {
              if (requestType != FrameType.REQUEST_FNF && requestType != FrameType.METADATA_PUSH) {
                rule.connection.addToReceivedBuffer(
                    PayloadFrameCodec.encodeComplete(rule.allocator, 1));
              }
            })
        .expectComplete()
        .verify(Duration.ofMillis(1000));

    rule.allocator.assertHasNoLeaks();
  }

  @ParameterizedTest
  @MethodSource("interactions")
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void shouldHaveNoLeaksOnPayloadInCaseOfRacingOfOnNextAndCancel(
      BiFunction<RSocketClient, Publisher<Payload>, Publisher<?>> request, FrameType requestType) {
    Assumptions.assumeThat(requestType).isNotEqualTo(FrameType.REQUEST_CHANNEL);

    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
      ClientSocketRule rule = new ClientSocketRule();
      rule.init();
      Payload payload = ByteBufPayload.create("test", "testMetadata");
      TestPublisher<Payload> testPublisher =
          TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION);
      AssertSubscriber assertSubscriber = AssertSubscriber.create(0);

      Publisher<?> publisher = request.apply(rule.client, testPublisher);
      publisher.subscribe(assertSubscriber);

      testPublisher.assertWasNotRequested();

      assertSubscriber.request(1);

      testPublisher.assertWasRequested();
      testPublisher.assertMaxRequested(1);
      testPublisher.assertMinRequested(1);

      RaceTestUtils.race(
          () -> {
            testPublisher.next(payload);
            rule.delayer.run();
          },
          assertSubscriber::cancel);

      Collection<ByteBuf> sent = rule.connection.getSent();
      if (sent.size() == 1) {
        Assertions.assertThat(sent)
            .allMatch(bb -> FrameHeaderCodec.frameType(bb).equals(requestType))
            .allMatch(ReferenceCounted::release);
      } else if (sent.size() == 2) {
        Assertions.assertThat(sent)
            .first()
            .matches(bb -> FrameHeaderCodec.frameType(bb).equals(requestType))
            .matches(ReferenceCounted::release);
        Assertions.assertThat(sent)
            .element(1)
            .matches(bb -> FrameHeaderCodec.frameType(bb).equals(FrameType.CANCEL))
            .matches(ReferenceCounted::release);
      } else {
        Assertions.assertThat(sent).isEmpty();
      }

      rule.allocator.assertHasNoLeaks();
    }
  }

  @ParameterizedTest
  @MethodSource("interactions")
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void shouldHaveNoLeaksOnPayloadInCaseOfRacingOfRequestAndCancel(
      BiFunction<RSocketClient, Publisher<Payload>, Publisher<?>> request, FrameType requestType) {
    Assumptions.assumeThat(requestType).isNotEqualTo(FrameType.REQUEST_CHANNEL);

    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
      ClientSocketRule rule = new ClientSocketRule();
      rule.init();
      ByteBuf dataBuffer = rule.allocator.buffer();
      dataBuffer.writeCharSequence("test", CharsetUtil.UTF_8);

      ByteBuf metadataBuffer = rule.allocator.buffer();
      metadataBuffer.writeCharSequence("testMetadata", CharsetUtil.UTF_8);

      Payload payload = ByteBufPayload.create(dataBuffer, metadataBuffer);
      AssertSubscriber assertSubscriber = AssertSubscriber.create(0);

      Publisher<?> publisher = request.apply(rule.client, Mono.just(payload));
      publisher.subscribe(assertSubscriber);

      RaceTestUtils.race(
          () -> {
            assertSubscriber.request(1);
            rule.delayer.run();
          },
          assertSubscriber::cancel);

      Collection<ByteBuf> sent = rule.connection.getSent();
      if (sent.size() == 1) {
        Assertions.assertThat(sent)
            .allMatch(bb -> FrameHeaderCodec.frameType(bb).equals(requestType))
            .allMatch(ReferenceCounted::release);
      } else if (sent.size() == 2) {
        Assertions.assertThat(sent)
            .first()
            .matches(bb -> FrameHeaderCodec.frameType(bb).equals(requestType))
            .matches(ReferenceCounted::release);
        Assertions.assertThat(sent)
            .element(1)
            .matches(bb -> FrameHeaderCodec.frameType(bb).equals(FrameType.CANCEL))
            .matches(ReferenceCounted::release);
      } else {
        Assertions.assertThat(sent).isEmpty();
      }

      rule.allocator.assertHasNoLeaks();
    }
  }

  @ParameterizedTest
  @MethodSource("interactions")
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void shouldPropagateDownstreamContext(
      BiFunction<RSocketClient, Publisher<Payload>, Publisher<?>> request, FrameType requestType) {
    Assumptions.assumeThat(requestType).isNotEqualTo(FrameType.REQUEST_CHANNEL);

    ByteBuf dataBuffer = rule.allocator.buffer();
    dataBuffer.writeCharSequence("test", CharsetUtil.UTF_8);

    ByteBuf metadataBuffer = rule.allocator.buffer();
    metadataBuffer.writeCharSequence("testMetadata", CharsetUtil.UTF_8);

    Payload payload = ByteBufPayload.create(dataBuffer, metadataBuffer);
    AssertSubscriber assertSubscriber = new AssertSubscriber(Context.of("test", "test"));

    ContextView[] receivedContext = new Context[1];
    Publisher<?> publisher =
        request.apply(
            rule.client,
            Mono.just(payload)
                .mergeWith(
                    Mono.deferContextual(
                            c -> {
                              receivedContext[0] = c;
                              return Mono.empty();
                            })
                        .then(Mono.empty())));
    publisher.subscribe(assertSubscriber);

    rule.delayer.run();

    Collection<ByteBuf> sent = rule.connection.getSent();
    if (sent.size() == 1) {
      Assertions.assertThat(sent)
          .allMatch(bb -> FrameHeaderCodec.frameType(bb).equals(requestType))
          .allMatch(ReferenceCounted::release);
    } else if (sent.size() == 2) {
      Assertions.assertThat(sent)
          .first()
          .matches(bb -> FrameHeaderCodec.frameType(bb).equals(requestType))
          .matches(ReferenceCounted::release);
      Assertions.assertThat(sent)
          .element(1)
          .matches(bb -> FrameHeaderCodec.frameType(bb).equals(FrameType.CANCEL))
          .matches(ReferenceCounted::release);
    } else {
      Assertions.assertThat(sent).isEmpty();
    }

    Assertions.assertThat(receivedContext)
        .hasSize(1)
        .allSatisfy(
            c ->
                Assertions.assertThat(
                        c.stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
                    .containsKeys("test", DefaultRSocketClient.ON_DISCARD_KEY));

    rule.allocator.assertHasNoLeaks();
  }

  @ParameterizedTest
  @MethodSource("interactions")
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void shouldSupportMultiSubscriptionOnTheSameInteractionPublisher(
      BiFunction<RSocketClient, Publisher<Payload>, Publisher<?>> request, FrameType requestType) {
    AtomicBoolean once1 = new AtomicBoolean();
    AtomicBoolean once2 = new AtomicBoolean();
    Mono<Payload> source =
        Mono.fromCallable(
                () -> {
                  if (!once1.getAndSet(true)) {
                    throw new IllegalStateException("test");
                  }
                  return ByteBufPayload.create("test", "testMetadata");
                })
            .doFinally(
                st -> {
                  rule.delayer.run();
                  if (requestType != FrameType.METADATA_PUSH
                      && requestType != FrameType.REQUEST_FNF) {
                    if (st != SignalType.ON_ERROR) {
                      if (!once2.getAndSet(true)) {
                        rule.connection.addToReceivedBuffer(
                            ErrorFrameCodec.encode(
                                rule.allocator, 1, new IllegalStateException("test")));
                      } else {
                        rule.connection.addToReceivedBuffer(
                            PayloadFrameCodec.encodeComplete(rule.allocator, 3));
                      }
                    }
                  }
                });
    AssertSubscriber assertSubscriber = AssertSubscriber.create(0);

    Publisher<?> publisher = request.apply(rule.client, source);
    if (publisher instanceof Mono) {
      ((Mono) publisher)
          .retryWhen(Retry.backoff(3, Duration.ofMillis(100)))
          .subscribe(assertSubscriber);
    } else {
      ((Flux) publisher)
          .retryWhen(Retry.backoff(3, Duration.ofMillis(100)))
          .subscribe(assertSubscriber);
    }

    assertSubscriber.request(1);

    if (requestType == FrameType.REQUEST_CHANNEL) {
      rule.delayer.run();
    }

    assertSubscriber.await(Duration.ofSeconds(10)).assertComplete();

    if (requestType == FrameType.REQUEST_CHANNEL) {
      ArrayList<ByteBuf> sent = new ArrayList<>(rule.connection.getSent());
      Assertions.assertThat(sent).hasSize(4);
      for (int i = 0; i < sent.size(); i++) {
        if (i % 2 == 0) {
          Assertions.assertThat(sent.get(i))
              .matches(bb -> FrameHeaderCodec.frameType(bb).equals(requestType))
              .matches(ReferenceCounted::release);
        } else {
          Assertions.assertThat(sent.get(i))
              .matches(bb -> FrameHeaderCodec.frameType(bb).equals(FrameType.COMPLETE))
              .matches(ReferenceCounted::release);
        }
      }
    } else {
      Collection<ByteBuf> sent = rule.connection.getSent();
      Assertions.assertThat(sent)
          .hasSize(
              requestType == FrameType.REQUEST_FNF || requestType == FrameType.METADATA_PUSH
                  ? 1
                  : 2)
          .allMatch(bb -> FrameHeaderCodec.frameType(bb).equals(requestType))
          .allMatch(ReferenceCounted::release);
    }

    rule.allocator.assertHasNoLeaks();
  }

  @Test
  public void shouldBeAbleToResolveOriginalSource() {
    AssertSubscriber<RSocket> assertSubscriber = AssertSubscriber.create(0);
    rule.client.source().subscribe(assertSubscriber);

    assertSubscriber.assertNotTerminated();

    rule.delayer.run();

    assertSubscriber.request(1);

    assertSubscriber.assertTerminated().assertValueCount(1);

    AssertSubscriber<RSocket> assertSubscriber1 = AssertSubscriber.create();

    rule.client.source().subscribe(assertSubscriber1);

    assertSubscriber1.assertTerminated().assertValueCount(1);

    Assertions.assertThat(assertSubscriber1.values()).isEqualTo(assertSubscriber.values());

    rule.allocator.assertHasNoLeaks();
  }

  @Test
  public void shouldDisposeOriginalSource() {
    AssertSubscriber<RSocket> assertSubscriber = AssertSubscriber.create();
    rule.client.source().subscribe(assertSubscriber);
    rule.delayer.run();
    assertSubscriber.assertTerminated().assertValueCount(1);

    rule.client.dispose();

    Assertions.assertThat(rule.client.isDisposed()).isTrue();

    AssertSubscriber<RSocket> assertSubscriber1 = AssertSubscriber.create();

    rule.client.source().subscribe(assertSubscriber1);

    assertSubscriber1
        .assertTerminated()
        .assertError(CancellationException.class)
        .assertErrorMessage("Disposed");

    Assertions.assertThat(rule.socket.isDisposed()).isTrue();

    FrameAssert.assertThat(rule.connection.awaitFrame())
        .hasStreamIdZero()
        .hasData("Disposed")
        .hasNoLeaks();

    rule.allocator.assertHasNoLeaks();
  }

  @Test
  public void shouldReceiveOnCloseNotificationOnDisposeOriginalSource() {
    Sinks.Empty<Void> onCloseDelayer = Sinks.empty();
    ClientSocketRule rule =
        new ClientSocketRule() {
          @Override
          protected RSocket newRSocket() {
            return new RSocketProxy(super.newRSocket()) {
              @Override
              public Mono<Void> onClose() {
                return super.onClose().and(onCloseDelayer.asMono());
              }
            };
          }
        };
    rule.init();
    AssertSubscriber<RSocket> assertSubscriber = AssertSubscriber.create();
    rule.client.source().subscribe(assertSubscriber);
    rule.delayer.run();
    assertSubscriber.assertTerminated().assertValueCount(1);

    rule.client.dispose();

    Assertions.assertThat(rule.client.isDisposed()).isTrue();

    AssertSubscriber<Void> onCloseSubscriber = AssertSubscriber.create();

    rule.client.onClose().subscribe(onCloseSubscriber);
    onCloseSubscriber.assertNotTerminated();

    onCloseDelayer.tryEmitEmpty();

    onCloseSubscriber.assertTerminated().assertComplete();

    Assertions.assertThat(rule.socket.isDisposed()).isTrue();

    FrameAssert.assertThat(rule.connection.awaitFrame())
        .hasStreamIdZero()
        .hasData("Disposed")
        .hasNoLeaks();

    rule.allocator.assertHasNoLeaks();
  }

  @Test
  public void shouldResolveOnStartSource() {
    AssertSubscriber<RSocket> assertSubscriber = AssertSubscriber.create();
    Assertions.assertThat(rule.client.connect()).isTrue();
    rule.client.source().subscribe(assertSubscriber);
    rule.delayer.run();
    assertSubscriber.assertTerminated().assertValueCount(1);

    rule.client.dispose();

    Assertions.assertThat(rule.client.isDisposed()).isTrue();

    AssertSubscriber<Void> assertSubscriber1 = AssertSubscriber.create();

    rule.client.onClose().subscribe(assertSubscriber1);

    assertSubscriber1.assertTerminated().assertComplete();

    Assertions.assertThat(rule.socket.isDisposed()).isTrue();

    FrameAssert.assertThat(rule.connection.awaitFrame())
        .hasStreamIdZero()
        .hasData("Disposed")
        .hasNoLeaks();

    rule.allocator.assertHasNoLeaks();
  }

  @Test
  public void shouldNotStartIfAlreadyDisposed() {
    Assertions.assertThat(rule.client.connect()).isTrue();
    Assertions.assertThat(rule.client.connect()).isTrue();
    rule.delayer.run();

    rule.client.dispose();

    Assertions.assertThat(rule.client.connect()).isFalse();

    Assertions.assertThat(rule.client.isDisposed()).isTrue();

    AssertSubscriber<Void> assertSubscriber1 = AssertSubscriber.create();

    rule.client.onClose().subscribe(assertSubscriber1);

    assertSubscriber1.assertTerminated().assertComplete();

    Assertions.assertThat(rule.socket.isDisposed()).isTrue();

    FrameAssert.assertThat(rule.connection.awaitFrame())
        .hasStreamIdZero()
        .hasData("Disposed")
        .hasNoLeaks();

    rule.allocator.assertHasNoLeaks();
  }

  @Test
  public void shouldBeRestartedIfSourceWasClosed() {
    AssertSubscriber<RSocket> assertSubscriber = AssertSubscriber.create();
    AssertSubscriber<Void> terminateSubscriber = AssertSubscriber.create();

    Assertions.assertThat(rule.client.connect()).isTrue();
    rule.client.source().subscribe(assertSubscriber);
    rule.client.onClose().subscribe(terminateSubscriber);

    rule.delayer.run();

    assertSubscriber.assertTerminated().assertValueCount(1);

    rule.socket.dispose();

    FrameAssert.assertThat(rule.connection.awaitFrame())
        .hasStreamIdZero()
        .hasData("Disposed")
        .hasNoLeaks();

    terminateSubscriber.assertNotTerminated();
    Assertions.assertThat(rule.client.isDisposed()).isFalse();

    rule.connection = new TestDuplexConnection(rule.allocator);
    rule.socket = rule.newRSocket();
    rule.producer = Sinks.one();

    AssertSubscriber<RSocket> assertSubscriber2 = AssertSubscriber.create();

    Assertions.assertThat(rule.client.connect()).isTrue();
    rule.client.source().subscribe(assertSubscriber2);

    rule.delayer.run();

    assertSubscriber2.assertTerminated().assertValueCount(1);

    rule.client.dispose();

    terminateSubscriber.assertTerminated().assertComplete();

    Assertions.assertThat(rule.client.connect()).isFalse();

    Assertions.assertThat(rule.socket.isDisposed()).isTrue();

    FrameAssert.assertThat(rule.connection.awaitFrame())
        .hasStreamIdZero()
        .hasData("Disposed")
        .hasNoLeaks();

    rule.allocator.assertHasNoLeaks();
  }

  @Test
  public void shouldDisposeOriginalSourceIfRacing() {
    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
      ClientSocketRule rule = new ClientSocketRule();

      rule.init();

      AssertSubscriber<RSocket> assertSubscriber = AssertSubscriber.create();
      rule.client.source().subscribe(assertSubscriber);

      RaceTestUtils.race(rule.delayer, () -> rule.client.dispose());

      assertSubscriber.assertTerminated();

      Assertions.assertThat(rule.client.isDisposed()).isTrue();
      Assertions.assertThat(rule.socket.isDisposed()).isTrue();

      AssertSubscriber<RSocket> assertSubscriber1 = AssertSubscriber.create();

      rule.client.source().subscribe(assertSubscriber1);

      assertSubscriber1
          .assertTerminated()
          .assertError(CancellationException.class)
          .assertErrorMessage("Disposed");

      ByteBuf buf;
      while ((buf = rule.connection.pollFrame()) != null) {
        FrameAssert.assertThat(buf).hasStreamIdZero().hasData("Disposed").hasNoLeaks();
      }

      rule.allocator.assertHasNoLeaks();
    }
  }

  @Test
  public void shouldStartOriginalSourceOnceIfRacing() {
    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
      ClientSocketRule rule = new ClientSocketRule();

      rule.init();

      AssertSubscriber<RSocket> assertSubscriber = AssertSubscriber.create();

      RaceTestUtils.race(
          () -> rule.client.source().subscribe(assertSubscriber), () -> rule.client.connect());

      Assertions.assertThat(rule.producer.currentSubscriberCount()).isOne();

      rule.delayer.run();

      assertSubscriber.assertTerminated();

      rule.client.dispose();

      Assertions.assertThat(rule.client.isDisposed()).isTrue();
      Assertions.assertThat(rule.socket.isDisposed()).isTrue();

      AssertSubscriber<Void> assertSubscriber1 = AssertSubscriber.create();

      rule.client.onClose().subscribe(assertSubscriber1);
      FrameAssert.assertThat(rule.connection.awaitFrame())
          .hasStreamIdZero()
          .hasData("Disposed")
          .hasNoLeaks();

      assertSubscriber1.assertTerminated().assertComplete();

      rule.allocator.assertHasNoLeaks();
    }
  }

  public static class ClientSocketRule extends AbstractSocketRule<RSocket> {

    protected RSocketClient client;
    protected Runnable delayer;
    protected Sinks.One<RSocket> producer;

    protected Sinks.Empty<Void> onGracefulShutdownStartedSink;
    protected Sinks.Empty<Void> thisGracefulShutdownSink;
    protected Sinks.Empty<Void> thisClosedSink;

    @Override
    protected void doInit() {
      super.doInit();
      delayer = () -> producer.tryEmitValue(socket);
      producer = Sinks.one();
      client =
          new DefaultRSocketClient(
              Mono.defer(
                  () ->
                      producer
                          .asMono()
                          .doOnCancel(() -> socket.dispose())
                          .doOnDiscard(Disposable.class, Disposable::dispose)));
    }

    @Override
    protected RSocket newRSocket() {
      this.onGracefulShutdownStartedSink = Sinks.empty();
      this.thisGracefulShutdownSink = Sinks.empty();
      this.thisClosedSink = Sinks.empty();
      return new RSocketRequester(
          connection,
          PayloadDecoder.ZERO_COPY,
          StreamIdSupplier.clientSupplier(),
          0,
          maxFrameLength,
          maxInboundPayloadSize,
          Integer.MAX_VALUE,
          Integer.MAX_VALUE,
          null,
          __ -> null,
          null,
          onGracefulShutdownStartedSink,
          thisGracefulShutdownSink,
          thisClosedSink,
          thisGracefulShutdownSink.asMono(),
          thisClosedSink.asMono());
    }
  }
}
