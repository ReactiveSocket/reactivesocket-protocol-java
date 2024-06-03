/*
 * Copyright 2015-Present the original author or authors.
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

package io.rsocket.router;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.frame.FrameType;
import io.rsocket.router.ImmutableRoutingRSocket.ImmutableRouterBuilder;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

public abstract class RoutingRSocket implements RSocket {

  final RouteCodec routeCodec;

  RoutingRSocket(RouteCodec routeCodec) {
    this.routeCodec = routeCodec;
  }

  @Nullable
  protected abstract HandlerFunction handlerFor(Route route);

  @Override
  @SuppressWarnings({"unchecked"})
  public Mono<Void> fireAndForget(Payload payload) {
    try {
      final Route route = this.routeCodec.decode(payload.sliceMetadata(), FrameType.REQUEST_FNF);

      if (route != null) {
        final HandlerFunction handler = handlerFor(route);

        if (handler != null) {
          return (Mono<Void>) handler.handle(payload);
        }
      }

      return RSocket.super.fireAndForget(payload);
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public Mono<Payload> requestResponse(Payload payload) {
    try {
      final Route route =
          this.routeCodec.decode(payload.sliceMetadata(), FrameType.REQUEST_RESPONSE);

      if (route != null) {
        final HandlerFunction handler = handlerFor(route);

        if (handler != null) {
          return (Mono<Payload>) handler.handle(payload);
        }
      }

      return RSocket.super.requestResponse(payload);
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Flux<Payload> requestStream(Payload payload) {
    try {
      final Route route = this.routeCodec.decode(payload.sliceMetadata(), FrameType.REQUEST_STREAM);

      if (route != null) {
        final HandlerFunction handler = handlerFor(route);

        if (handler != null) {
          return (Flux<Payload>) handler.handle(payload);
        }
      }

      return RSocket.super.requestStream(payload);
    } catch (Throwable t) {
      return Flux.error(t);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Flux<Payload> requestChannel(Publisher<Payload> source) {
    return Flux.from(source)
        .switchOnFirst(
            (firstSignal, payloads) -> {
              final Payload firstPayload = firstSignal.get();

              if (firstPayload != null) {
                try {
                  final Route route =
                      this.routeCodec.decode(
                          firstPayload.sliceMetadata(), FrameType.REQUEST_CHANNEL);
                  if (route != null) {
                    final HandlerFunction handler = handlerFor(route);

                    if (handler != null) {
                      return (Flux<Payload>) handler.handle(firstPayload, payloads);
                    }
                  }
                } catch (Throwable t) {
                  firstPayload.release();
                  return Flux.error(t);
                }
              }

              return RSocket.super.requestChannel(payloads);
            },
            false);
  }

  public static Builder<?> immutable(RouteCodec routeCodec) {
    return new ImmutableRouterBuilder(routeCodec);
  }

  public interface Builder<T extends Builder<T>> {

    default T addFireAndForget(String route, Function<Payload, Mono<Void>> handler) {
      return addHandler(Router.route(route).fireAndForget(handler));
    }

    default T addFireAndForget(String route, Consumer<Payload> handler) {
      return addHandler(Router.route(route).fireAndForget(handler));
    }

    default T addRequestResponse(String route, Function<Payload, Mono<Payload>> handler) {
      return addHandler(Router.route(route).requestResponse(handler));
    }

    default T addRequestStream(String route, Function<Payload, Flux<Payload>> handler) {
      return addHandler(Router.route(route).requestStream(handler));
    }

    default T addRequestChannel(String route, Function<Flux<Payload>, Flux<Payload>> handler) {
      return addHandler(Router.route(route).requestChannel(handler));
    }

    default T addRequestChannel(
        String route, BiFunction<Payload, Flux<Payload>, Flux<Payload>> handler) {
      return addHandler(Router.route(route).requestChannel(handler));
    }

    T addHandler(HandlerFunction handler);

    RoutingRSocket build();
  }
}
