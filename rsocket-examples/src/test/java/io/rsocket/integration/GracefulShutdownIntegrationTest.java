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

package io.rsocket.integration;

import static org.assertj.core.api.Assertions.assertThat;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.ByteBufPayload;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

public class GracefulShutdownIntegrationTest {
  private RSocket handler;
  private Sinks.Empty<Void> clientGracefulShutdownSink;
  private Sinks.Empty<Void> serverGracefulShutdownSink;
  private Disposable stream;

  private CloseableChannel server;

  @BeforeEach
  public void startup() {
    server =
        RSocketServer.create(
                (setup, sendingSocket) -> {
                  stream =
                      sendingSocket
                          .requestStream(ByteBufPayload.create("REQUEST", "META"))
                          .subscribe();
                  return Mono.just(handler);
                })
            .bind(TcpServerTransport.create("localhost", 0))
            .block();
  }

  @AfterEach
  public void cleanup() {
    server.dispose();
  }

  @Test
  @Timeout(15_000L)
  public void testCompleteWithoutNext() {
    clientGracefulShutdownSink = Sinks.unsafe().empty();
    serverGracefulShutdownSink = Sinks.unsafe().empty();
    final AtomicBoolean gracefullyDisposedServer = new AtomicBoolean();
    final AtomicBoolean gracefullyDisposedClient = new AtomicBoolean();
    final AtomicBoolean disposedServer = new AtomicBoolean();
    final AtomicBoolean disposedClient = new AtomicBoolean();
    final Sinks.Empty<Void> requestHandled = Sinks.unsafe().empty();
    handler =
        new RSocket() {
          @Override
          public Flux<Payload> requestStream(Payload payload) {
            payload.release();

            requestHandled.tryEmitEmpty();
            return Flux.<Payload>never().takeUntilOther(serverGracefulShutdownSink.asMono());
          }

          @Override
          public void dispose() {
            disposedServer.set(true);
          }

          @Override
          public void disposeGracefully() {
            gracefullyDisposedServer.set(true);
          }
        };
    RSocket client =
        RSocketConnector.create()
            .acceptor(
                SocketAcceptor.with(
                    new RSocket() {
                      @Override
                      public Flux<Payload> requestStream(Payload payload) {
                        payload.release();
                        return Flux.<Payload>never()
                            .takeUntilOther(clientGracefulShutdownSink.asMono());
                      }

                      @Override
                      public void dispose() {
                        disposedClient.set(true);
                      }

                      @Override
                      public void disposeGracefully() {
                        gracefullyDisposedClient.set(true);
                      }
                    }))
            .connect(TcpClientTransport.create(server.address()))
            .block();

    AtomicReference<SignalType> terminalSignal = new AtomicReference<>();

    StepVerifier clientRequestVerifier =
        client
            .requestStream(ByteBufPayload.create("REQUEST", "META"))
            .onErrorResume(
                t -> Mono.empty()) // FIXME: onComplete frame may not be delivered on time
            .doFinally(terminalSignal::set)
            .as(StepVerifier::create)
            .expectSubscription()
            .expectComplete()
            .verifyLater();

    requestHandled.asMono().block(Duration.ofSeconds(5));

    assertThat(gracefullyDisposedServer).isFalse();
    assertThat(gracefullyDisposedClient).isFalse();

    client.disposeGracefully();

    assertThat(client.isDisposed()).isFalse();
    assertThat(gracefullyDisposedServer)
        .as("gracefullyDisposedServer after disposeGracefully")
        .isTrue();
    assertThat(gracefullyDisposedClient)
        .as("gracefullyDisposedClient after disposeGracefully")
        .isTrue();

    assertThat(disposedServer).as("disposedServer after disposeGracefully").isFalse();
    assertThat(disposedClient).as("disposedClient after disposeGracefully").isFalse();
    assertThat(terminalSignal).as("terminalSignal after disposeGracefully").hasValue(null);
    assertThat(stream.isDisposed()).isFalse();

    clientGracefulShutdownSink.tryEmitEmpty();

    assertThat(client.isDisposed()).isFalse();
    assertThat(terminalSignal)
        .as("terminalSignal after clientGracefulShutdownSink.tryEmitEmpty")
        .hasValue(null);
    Awaitility.waitAtMost(Duration.ofSeconds(5)).until(() -> stream.isDisposed());

    serverGracefulShutdownSink.tryEmitEmpty();

    clientRequestVerifier.verify(Duration.ofSeconds(5));
    assertThat(client.isDisposed()).isTrue();
    assertThat(stream.isDisposed()).isTrue();
    Awaitility.waitAtMost(Duration.ofSeconds(5)).untilTrue(disposedServer);
    Awaitility.waitAtMost(Duration.ofSeconds(5)).untilTrue(disposedClient);
  }
}
