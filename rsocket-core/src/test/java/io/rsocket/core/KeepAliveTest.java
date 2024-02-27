/*
 * Copyright 2015-2019 the original author or authors.
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
import static io.rsocket.keepalive.KeepAliveHandler.DefaultKeepAliveHandler;
import static io.rsocket.keepalive.KeepAliveHandler.ResumableKeepAliveHandler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.FrameAssert;
import io.rsocket.RSocket;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.KeepAliveFrameCodec;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.resume.InMemoryResumableFramesStore;
import io.rsocket.resume.RSocketSession;
import io.rsocket.resume.ResumableDuplexConnection;
import io.rsocket.resume.ResumeStateHolder;
import io.rsocket.test.util.TestDuplexConnection;
import java.time.Duration;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

public class KeepAliveTest {
  private static final int KEEP_ALIVE_INTERVAL = 100;
  private static final int KEEP_ALIVE_TIMEOUT = 1000;
  private static final int RESUMABLE_KEEP_ALIVE_TIMEOUT = 200;

  VirtualTimeScheduler virtualTimeScheduler;

  @BeforeEach
  public void setUp() {
    virtualTimeScheduler = VirtualTimeScheduler.getOrSet();
  }

  @AfterEach
  public void tearDown() {
    VirtualTimeScheduler.reset();
  }

  static RSocketState requester(int tickPeriod, int timeout) {
    LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    TestDuplexConnection connection = new TestDuplexConnection(allocator);
    Sinks.Empty<Void> empty = Sinks.empty();
    RSocketRequester rSocket =
        new RSocketRequester(
            connection,
            PayloadDecoder.ZERO_COPY,
            StreamIdSupplier.clientSupplier(),
            0,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            tickPeriod,
            timeout,
            new DefaultKeepAliveHandler(),
            r -> null,
            null,
            Sinks.empty(),
            Sinks.empty(),
            empty,
            Sinks.<Void>empty().asMono(),
            empty.asMono());
    return new RSocketState(rSocket, allocator, connection, empty);
  }

  static ResumableRSocketState resumableRequester(int tickPeriod, int timeout) {
    LeaksTrackingByteBufAllocator allocator =
        LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);
    TestDuplexConnection connection = new TestDuplexConnection(allocator);
    ResumableDuplexConnection resumableConnection =
        new ResumableDuplexConnection(
            "test",
            Unpooled.EMPTY_BUFFER,
            connection,
            new InMemoryResumableFramesStore("test", Unpooled.EMPTY_BUFFER, 10_000));
    Sinks.Empty<Void> onClose = Sinks.empty();

    RSocketRequester rSocket =
        new RSocketRequester(
            resumableConnection,
            PayloadDecoder.ZERO_COPY,
            StreamIdSupplier.clientSupplier(),
            0,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            tickPeriod,
            timeout,
            new ResumableKeepAliveHandler(
                resumableConnection,
                Mockito.mock(RSocketSession.class),
                Mockito.mock(ResumeStateHolder.class)),
            __ -> null,
            null,
            Sinks.empty(),
            Sinks.empty(),
            onClose,
            Sinks.<Void>empty().asMono(),
            onClose.asMono());
    return new ResumableRSocketState(rSocket, connection, resumableConnection, onClose, allocator);
  }

  @Test
  void rSocketNotDisposedOnPresentKeepAlives() {
    RSocketState requesterState = requester(KEEP_ALIVE_INTERVAL, KEEP_ALIVE_TIMEOUT);

    TestDuplexConnection connection = requesterState.connection();

    Disposable disposable =
        Flux.interval(Duration.ofMillis(KEEP_ALIVE_INTERVAL))
            .subscribe(
                n ->
                    connection.addToReceivedBuffer(
                        KeepAliveFrameCodec.encode(
                            requesterState.allocator, true, 0, Unpooled.EMPTY_BUFFER)));

    virtualTimeScheduler.advanceTimeBy(Duration.ofMillis(KEEP_ALIVE_TIMEOUT * 2));

    RSocket rSocket = requesterState.rSocket();

    Assertions.assertThat(rSocket.isDisposed()).isFalse();

    disposable.dispose();

    requesterState.connection.dispose();
    requesterState.rSocket.dispose();

    Assertions.assertThat(requesterState.connection.getSent()).allMatch(ByteBuf::release);

    requesterState.allocator.assertHasNoLeaks();
  }

  @Test
  void noKeepAlivesSentAfterRSocketDispose() {
    RSocketState requesterState = requester(KEEP_ALIVE_INTERVAL, KEEP_ALIVE_TIMEOUT);

    requesterState.rSocket().dispose();

    Duration duration = Duration.ofMillis(500);

    virtualTimeScheduler.advanceTimeBy(duration);

    FrameAssert.assertThat(requesterState.connection.pollFrame())
        .typeOf(FrameType.ERROR)
        .hasData("Disposed")
        .hasNoLeaks();
    FrameAssert.assertThat(requesterState.connection.pollFrame()).isNull();
    requesterState.allocator.assertHasNoLeaks();
  }

  @Test
  void rSocketDisposedOnMissingKeepAlives() {
    RSocketState requesterState = requester(KEEP_ALIVE_INTERVAL, KEEP_ALIVE_TIMEOUT);

    RSocket rSocket = requesterState.rSocket();

    virtualTimeScheduler.advanceTimeBy(Duration.ofMillis(KEEP_ALIVE_TIMEOUT * 2));

    Assertions.assertThat(rSocket.isDisposed()).isTrue();
    rSocket
        .onClose()
        .as(StepVerifier::create)
        .expectError(ConnectionErrorException.class)
        .verify(Duration.ofMillis(100));

    Assertions.assertThat(requesterState.connection.getSent()).allMatch(ByteBuf::release);

    requesterState.allocator.assertHasNoLeaks();
  }

  @Test
  void clientRequesterSendsKeepAlives() {
    RSocketState RSocketState = requester(KEEP_ALIVE_INTERVAL, KEEP_ALIVE_TIMEOUT);
    TestDuplexConnection connection = RSocketState.connection();

    virtualTimeScheduler.advanceTimeBy(Duration.ofMillis(KEEP_ALIVE_INTERVAL));
    this.keepAliveFrameWithRespondFlag(connection.pollFrame());
    virtualTimeScheduler.advanceTimeBy(Duration.ofMillis(KEEP_ALIVE_INTERVAL));
    this.keepAliveFrameWithRespondFlag(connection.pollFrame());
    virtualTimeScheduler.advanceTimeBy(Duration.ofMillis(KEEP_ALIVE_INTERVAL));
    this.keepAliveFrameWithRespondFlag(connection.pollFrame());

    RSocketState.rSocket.dispose();
    FrameAssert.assertThat(connection.pollFrame())
        .typeOf(FrameType.ERROR)
        .hasData("Disposed")
        .hasNoLeaks();
    RSocketState.connection.dispose();

    RSocketState.allocator.assertHasNoLeaks();
  }

  @Test
  void requesterRespondsToKeepAlives() {
    RSocketState rSocketState = requester(100_000, 100_000);
    TestDuplexConnection connection = rSocketState.connection();
    Duration duration = Duration.ofMillis(100);
    Mono.delay(duration)
        .subscribe(
            l ->
                connection.addToReceivedBuffer(
                    KeepAliveFrameCodec.encode(
                        rSocketState.allocator, true, 0, Unpooled.EMPTY_BUFFER)));

    virtualTimeScheduler.advanceTimeBy(duration);
    FrameAssert.assertThat(connection.awaitFrame())
        .typeOf(FrameType.KEEPALIVE)
        .matches(this::keepAliveFrameWithoutRespondFlag);

    rSocketState.rSocket.dispose();
    FrameAssert.assertThat(rSocketState.connection.pollFrame())
        .typeOf(FrameType.ERROR)
        .hasStreamIdZero()
        .hasData("Disposed")
        .hasNoLeaks();
    rSocketState.connection.dispose();

    rSocketState.allocator.assertHasNoLeaks();
  }

  @Test
  void resumableRequesterNoKeepAlivesAfterDisconnect() {
    ResumableRSocketState rSocketState =
        resumableRequester(KEEP_ALIVE_INTERVAL, KEEP_ALIVE_TIMEOUT);
    TestDuplexConnection testConnection = rSocketState.connection();
    ResumableDuplexConnection resumableDuplexConnection = rSocketState.resumableDuplexConnection();

    resumableDuplexConnection.disconnect();

    Duration duration = Duration.ofMillis(KEEP_ALIVE_INTERVAL * 5);
    virtualTimeScheduler.advanceTimeBy(duration);
    Assertions.assertThat(testConnection.pollFrame()).isNull();

    rSocketState.rSocket.dispose();
    rSocketState.connection.dispose();

    rSocketState.allocator.assertHasNoLeaks();
  }

  @Test
  void resumableRequesterKeepAlivesAfterReconnect() {
    ResumableRSocketState rSocketState =
        resumableRequester(KEEP_ALIVE_INTERVAL, KEEP_ALIVE_TIMEOUT);
    ResumableDuplexConnection resumableDuplexConnection = rSocketState.resumableDuplexConnection();
    resumableDuplexConnection.disconnect();
    TestDuplexConnection newTestConnection = new TestDuplexConnection(rSocketState.alloc());
    resumableDuplexConnection.connect(newTestConnection);
    //    resumableDuplexConnection.(0, 0, ignored -> Mono.empty());

    virtualTimeScheduler.advanceTimeBy(Duration.ofMillis(KEEP_ALIVE_INTERVAL));

    FrameAssert.assertThat(newTestConnection.awaitFrame())
        .typeOf(FrameType.KEEPALIVE)
        .hasStreamIdZero()
        .hasNoLeaks();

    rSocketState.rSocket.dispose();
    FrameAssert.assertThat(newTestConnection.pollFrame())
        .typeOf(FrameType.ERROR)
        .hasStreamIdZero()
        .hasData("Disposed")
        .hasNoLeaks();
    FrameAssert.assertThat(newTestConnection.pollFrame())
        .typeOf(FrameType.ERROR)
        .hasStreamIdZero()
        .hasData("Connection Closed Unexpectedly") // API limitations
        .hasNoLeaks();
    newTestConnection.dispose();

    rSocketState.allocator.assertHasNoLeaks();
  }

  @Test
  void resumableRequesterNoKeepAlivesAfterDispose() {
    ResumableRSocketState rSocketState =
        resumableRequester(KEEP_ALIVE_INTERVAL, KEEP_ALIVE_TIMEOUT);
    rSocketState.rSocket().dispose();
    Duration duration = Duration.ofMillis(500);
    StepVerifier.create(Flux.from(rSocketState.connection().getSentAsPublisher()).take(duration))
        .then(() -> virtualTimeScheduler.advanceTimeBy(duration))
        .expectComplete()
        .verify(Duration.ofSeconds(5));

    rSocketState.rSocket.dispose();
    FrameAssert.assertThat(rSocketState.connection.pollFrame())
        .typeOf(FrameType.ERROR)
        .hasStreamIdZero()
        .hasData("Disposed")
        .hasNoLeaks();
    rSocketState.connection.dispose();
    FrameAssert.assertThat(rSocketState.connection.pollFrame())
        .typeOf(FrameType.ERROR)
        .hasStreamIdZero()
        .hasData("Connection Closed Unexpectedly")
        .hasNoLeaks();

    rSocketState.allocator.assertHasNoLeaks();
  }

  @Test
  void resumableRSocketsNotDisposedOnMissingKeepAlives() throws InterruptedException {
    ResumableRSocketState resumableRequesterState =
        resumableRequester(KEEP_ALIVE_INTERVAL, RESUMABLE_KEEP_ALIVE_TIMEOUT);
    RSocket rSocket = resumableRequesterState.rSocket();
    TestDuplexConnection connection = resumableRequesterState.connection();

    virtualTimeScheduler.advanceTimeBy(Duration.ofMillis(500));

    Assertions.assertThat(rSocket.isDisposed()).isFalse();
    Assertions.assertThat(connection.isDisposed()).isTrue();

    Assertions.assertThat(resumableRequesterState.connection.getSent()).allMatch(ByteBuf::release);

    resumableRequesterState.connection.dispose();
    resumableRequesterState.rSocket.dispose();

    resumableRequesterState.allocator.assertHasNoLeaks();
  }

  private boolean keepAliveFrame(ByteBuf frame) {
    return FrameHeaderCodec.frameType(frame) == FrameType.KEEPALIVE;
  }

  private boolean keepAliveFrameWithRespondFlag(ByteBuf frame) {
    return keepAliveFrame(frame) && KeepAliveFrameCodec.respondFlag(frame) && frame.release();
  }

  private boolean keepAliveFrameWithoutRespondFlag(ByteBuf frame) {
    return keepAliveFrame(frame) && !KeepAliveFrameCodec.respondFlag(frame) && frame.release();
  }

  static class RSocketState {
    private final RSocket rSocket;
    private final TestDuplexConnection connection;
    private final LeaksTrackingByteBufAllocator allocator;
    private final Sinks.Empty<Void> onClose;

    public RSocketState(
        RSocket rSocket,
        LeaksTrackingByteBufAllocator allocator,
        TestDuplexConnection connection,
        Sinks.Empty<Void> onClose) {
      this.rSocket = rSocket;
      this.connection = connection;
      this.allocator = allocator;
      this.onClose = onClose;
    }

    public TestDuplexConnection connection() {
      return connection;
    }

    public RSocket rSocket() {
      return rSocket;
    }

    public LeaksTrackingByteBufAllocator alloc() {
      return allocator;
    }
  }

  static class ResumableRSocketState {
    private final RSocket rSocket;
    private final TestDuplexConnection connection;
    private final ResumableDuplexConnection resumableDuplexConnection;
    private final LeaksTrackingByteBufAllocator allocator;
    private final Sinks.Empty<Void> onClose;

    public ResumableRSocketState(
        RSocket rSocket,
        TestDuplexConnection connection,
        ResumableDuplexConnection resumableDuplexConnection,
        Sinks.Empty<Void> onClose,
        LeaksTrackingByteBufAllocator allocator) {
      this.rSocket = rSocket;
      this.connection = connection;
      this.resumableDuplexConnection = resumableDuplexConnection;
      this.onClose = onClose;
      this.allocator = allocator;
    }

    public TestDuplexConnection connection() {
      return connection;
    }

    public ResumableDuplexConnection resumableDuplexConnection() {
      return resumableDuplexConnection;
    }

    public RSocket rSocket() {
      return rSocket;
    }

    public LeaksTrackingByteBufAllocator alloc() {
      return allocator;
    }
  }
}
