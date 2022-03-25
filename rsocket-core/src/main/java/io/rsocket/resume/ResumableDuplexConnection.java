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

package io.rsocket.resume;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.CharsetUtil;
import io.rsocket.DuplexConnection;
import io.rsocket.RSocketErrorException;
import io.rsocket.exceptions.ConnectionCloseException;
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.internal.UnboundedProcessor;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Sinks;
import reactor.util.annotation.Nullable;

public class ResumableDuplexConnection extends Flux<ByteBuf>
    implements DuplexConnection, Subscription {

  static final Logger logger = LoggerFactory.getLogger(ResumableDuplexConnection.class);

  final String side;
  final String session;
  final ResumableFramesStore resumableFramesStore;

  final UnboundedProcessor savableFramesSender;
  final Disposable framesSaverDisposable;
  final Sinks.Empty<Void> onClose;
  final SocketAddress remoteAddress;
  final Sinks.Many<Integer> onConnectionClosedSink;

  CoreSubscriber<? super ByteBuf> receiveSubscriber;
  FrameReceivingSubscriber activeReceivingSubscriber;

  volatile int state;
  static final AtomicIntegerFieldUpdater<ResumableDuplexConnection> STATE =
      AtomicIntegerFieldUpdater.newUpdater(ResumableDuplexConnection.class, "state");

  volatile DuplexConnection activeConnection;
  static final AtomicReferenceFieldUpdater<ResumableDuplexConnection, DuplexConnection>
      ACTIVE_CONNECTION =
          AtomicReferenceFieldUpdater.newUpdater(
              ResumableDuplexConnection.class, DuplexConnection.class, "activeConnection");

  int connectionIndex = 0;

  public ResumableDuplexConnection(
      String side,
      ByteBuf session,
      DuplexConnection initialConnection,
      ResumableFramesStore resumableFramesStore) {
    this.side = side;
    this.session = session.toString(CharsetUtil.UTF_8);
    this.onConnectionClosedSink = Sinks.unsafe().many().unicast().onBackpressureBuffer();
    this.resumableFramesStore = resumableFramesStore;
    this.savableFramesSender = new UnboundedProcessor();
    this.framesSaverDisposable = resumableFramesStore.saveFrames(savableFramesSender).subscribe();
    this.onClose = Sinks.empty();
    this.remoteAddress = initialConnection.remoteAddress();

    ACTIVE_CONNECTION.lazySet(this, initialConnection);
  }

  public boolean connect(DuplexConnection nextConnection) {
    final DuplexConnection activeConnection = this.activeConnection;
    if (activeConnection != DisposedConnection.INSTANCE
        && ACTIVE_CONNECTION.compareAndSet(this, activeConnection, nextConnection)) {

      activeConnection.dispose();

      initConnection(nextConnection);

      return true;
    } else {
      return false;
    }
  }

  void initConnection(DuplexConnection nextConnection) {
    final int nextConnectionIndex = this.connectionIndex + 1;
    final FrameReceivingSubscriber frameReceivingSubscriber =
        new FrameReceivingSubscriber(side, resumableFramesStore, receiveSubscriber);

    this.connectionIndex = nextConnectionIndex;
    this.activeReceivingSubscriber = frameReceivingSubscriber;

    if (logger.isDebugEnabled()) {
      logger.debug(
          "Side[{}]|Session[{}]|DuplexConnection[{}]. Connecting", side, session, connectionIndex);
    }

    final Disposable resumeStreamSubscription =
        resumableFramesStore
            .resumeStream()
            .subscribe(
                f -> nextConnection.sendFrame(FrameHeaderCodec.streamId(f), f),
                t -> sendErrorAndClose(new ConnectionErrorException(t.getMessage())),
                () ->
                    sendErrorAndClose(
                        new ConnectionCloseException("Connection Closed Unexpectedly")));
    nextConnection.receive().subscribe(frameReceivingSubscriber);
    nextConnection
        .onClose()
        .doFinally(
            __ -> {
              frameReceivingSubscriber.dispose();
              resumeStreamSubscription.dispose();
              if (logger.isDebugEnabled()) {
                logger.debug(
                    "Side[{}]|Session[{}]|DuplexConnection[{}]. Disconnected",
                    side,
                    session,
                    connectionIndex);
              }
              Sinks.EmitResult result = onConnectionClosedSink.tryEmitNext(nextConnectionIndex);
              if (!result.equals(Sinks.EmitResult.OK)) {
                logger.error(
                    "Side[{}]|Session[{}]|DuplexConnection[{}]. Failed to notify session of closed connection: {}",
                    side,
                    session,
                    connectionIndex,
                    result);
              }
            })
        .subscribe();
  }

  public void disconnect() {
    final DuplexConnection activeConnection = this.activeConnection;
    if (activeConnection != DisposedConnection.INSTANCE) {
      activeConnection.dispose();
    }
  }

  @Override
  public void sendFrame(int streamId, ByteBuf frame) {
    if (streamId == 0) {
      savableFramesSender.onNextPrioritized(frame);
    } else {
      savableFramesSender.onNext(frame);
    }
  }

  /**
   * Publisher for a sequence of integers starting at 1, with each next number emitted when the
   * currently active connection is closed and should be resumed. The Publisher never emits an error
   * and completes when the connection is disposed and not resumed.
   */
  Flux<Integer> onActiveConnectionClosed() {
    return onConnectionClosedSink.asFlux();
  }

  @Override
  public void sendErrorAndClose(RSocketErrorException rSocketErrorException) {
    final DuplexConnection activeConnection =
        ACTIVE_CONNECTION.getAndSet(this, DisposedConnection.INSTANCE);
    if (activeConnection == DisposedConnection.INSTANCE) {
      return;
    }

    activeConnection.sendErrorAndClose(rSocketErrorException);
    activeConnection
        .onClose()
        .subscribe(
            null,
            t -> {
              framesSaverDisposable.dispose();
              activeReceivingSubscriber.dispose();
              savableFramesSender.dispose();
              onConnectionClosedSink.tryEmitComplete();

              onClose.tryEmitError(t);
            },
            () -> {
              framesSaverDisposable.dispose();
              activeReceivingSubscriber.dispose();
              savableFramesSender.dispose();
              onConnectionClosedSink.tryEmitComplete();

              final Throwable cause = rSocketErrorException.getCause();
              if (cause == null) {
                onClose.tryEmitEmpty();
              } else {
                onClose.tryEmitError(cause);
              }
            });
  }

  @Override
  public Flux<ByteBuf> receive() {
    return this;
  }

  @Override
  public ByteBufAllocator alloc() {
    return activeConnection.alloc();
  }

  @Override
  public Mono<Void> onClose() {
    return onClose.asMono();
  }

  @Override
  public void dispose() {
    dispose(null);
  }

  void dispose(@Nullable Throwable e) {
    final DuplexConnection activeConnection =
        ACTIVE_CONNECTION.getAndSet(this, DisposedConnection.INSTANCE);
    if (activeConnection == DisposedConnection.INSTANCE) {
      return;
    }

    if (activeConnection != null) {
      activeConnection.dispose();
    }

    if (logger.isDebugEnabled()) {
      logger.debug(
          "Side[{}]|Session[{}]|DuplexConnection[{}]. Disposing...",
          side,
          session,
          connectionIndex);
    }

    framesSaverDisposable.dispose();
    activeReceivingSubscriber.dispose();
    savableFramesSender.dispose();
    onConnectionClosedSink.tryEmitComplete();

    if (e != null) {
      onClose.tryEmitError(e);
    } else {
      onClose.tryEmitEmpty();
    }
  }

  @Override
  @SuppressWarnings("ConstantConditions")
  public boolean isDisposed() {
    return onClose.scan(Scannable.Attr.TERMINATED) || onClose.scan(Scannable.Attr.CANCELLED);
  }

  @Override
  public SocketAddress remoteAddress() {
    return remoteAddress;
  }

  @Override
  public void request(long n) {
    if (state == 1 && STATE.compareAndSet(this, 1, 2)) {
      initConnection(this.activeConnection);
    }
  }

  @Override
  public void cancel() {
    dispose();
  }

  @Override
  public void subscribe(CoreSubscriber<? super ByteBuf> receiverSubscriber) {
    if (state == 0 && STATE.compareAndSet(this, 0, 1)) {
      receiveSubscriber = receiverSubscriber;
      receiverSubscriber.onSubscribe(this);
    }
  }

  static boolean isResumableFrame(ByteBuf frame) {
    return FrameHeaderCodec.streamId(frame) != 0;
  }

  private static final class DisposedConnection implements DuplexConnection {

    static final DisposedConnection INSTANCE = new DisposedConnection();

    private DisposedConnection() {}

    @Override
    public void dispose() {}

    @Override
    public Mono<Void> onClose() {
      return Mono.never();
    }

    @Override
    public void sendFrame(int streamId, ByteBuf frame) {}

    @Override
    public Flux<ByteBuf> receive() {
      return Flux.never();
    }

    @Override
    public void sendErrorAndClose(RSocketErrorException e) {}

    @Override
    public ByteBufAllocator alloc() {
      return ByteBufAllocator.DEFAULT;
    }

    @Override
    @SuppressWarnings("ConstantConditions")
    public SocketAddress remoteAddress() {
      return null;
    }
  }

  private static final class FrameReceivingSubscriber
      implements CoreSubscriber<ByteBuf>, Disposable {

    final ResumableFramesStore resumableFramesStore;
    final CoreSubscriber<? super ByteBuf> actual;
    final String tag;

    volatile Subscription s;
    static final AtomicReferenceFieldUpdater<FrameReceivingSubscriber, Subscription> S =
        AtomicReferenceFieldUpdater.newUpdater(
            FrameReceivingSubscriber.class, Subscription.class, "s");

    boolean cancelled;

    private FrameReceivingSubscriber(
        String tag, ResumableFramesStore store, CoreSubscriber<? super ByteBuf> actual) {
      this.tag = tag;
      this.resumableFramesStore = store;
      this.actual = actual;
    }

    @Override
    public void onSubscribe(Subscription s) {
      if (Operators.setOnce(S, this, s)) {
        s.request(Long.MAX_VALUE);
      }
    }

    @Override
    public void onNext(ByteBuf frame) {
      if (cancelled || s == Operators.cancelledSubscription()) {
        return;
      }

      if (isResumableFrame(frame)) {
        if (resumableFramesStore.resumableFrameReceived(frame)) {
          actual.onNext(frame);
        }
        return;
      }

      actual.onNext(frame);
    }

    @Override
    public void onError(Throwable t) {
      Operators.set(S, this, Operators.cancelledSubscription());
    }

    @Override
    public void onComplete() {
      Operators.set(S, this, Operators.cancelledSubscription());
    }

    @Override
    public void dispose() {
      cancelled = true;
      Operators.terminate(S, this);
    }

    @Override
    public boolean isDisposed() {
      return cancelled || s == Operators.cancelledSubscription();
    }
  }
}
