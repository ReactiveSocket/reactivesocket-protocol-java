/*
 * Copyright 2015-2020 the original author or authors.
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

package io.rsocket.transport.netty;

import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.rsocket.test.TransportTest;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.time.Duration;
import reactor.core.Exceptions;
import reactor.netty.http.Http11SslContextSpec;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.server.HttpServer;

final class WebsocketSecureTransportTest
    extends TransportTest<InetSocketAddress, CloseableChannel> {

  @Override
  protected TransportPair<InetSocketAddress, CloseableChannel> createTransportPair() {
    return new TransportPair<>(
        () -> new InetSocketAddress("localhost", 0),
        (address, server, allocator) ->
            WebsocketClientTransport.create(
                HttpClient.create()
                    .option(ChannelOption.ALLOCATOR, allocator)
                    .remoteAddress(server::address)
                    .secure(
                        ssl ->
                            ssl.sslContext(
                                Http11SslContextSpec.forClient()
                                    .configure(
                                        scb ->
                                            scb.trustManager(
                                                InsecureTrustManagerFactory.INSTANCE)))),
                String.format(
                    "https://%s:%d/", server.address().getHostName(), server.address().getPort())),
        (address, allocator) -> {
          try {
            SelfSignedCertificate ssc = new SelfSignedCertificate();
            HttpServer server =
                HttpServer.create()
                    .option(ChannelOption.ALLOCATOR, allocator)
                    .bindAddress(() -> address)
                    .secure(
                        ssl ->
                            ssl.sslContext(
                                Http11SslContextSpec.forServer(
                                    ssc.certificate(), ssc.privateKey())));
            return WebsocketServerTransport.create(server);
          } catch (CertificateException e) {
            throw Exceptions.propagate(e);
          }
        },
        Duration.ofMinutes(2));
  }
}
