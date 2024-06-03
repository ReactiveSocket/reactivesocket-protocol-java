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
import io.rsocket.frame.FrameType;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

public final class Router {

  public static RequestSpec route(String route) {
    return route(route, null);
  }

  public static RequestSpec route(String route, @Nullable String mimeType) {
    return new RequestSpec(route, mimeType);
  }

  @SuppressWarnings("rawtypes")
  static final class RequestSpec {
    final String route;
    final String mimeType;

    RequestSpec(String route, @Nullable String mimeType) {
      this.route = route;
      this.mimeType = mimeType;
    }

    public HandlerFunction fireAndForget(Function<Payload, Mono<Void>> handler) {
      final Route route = new Route(FrameType.REQUEST_FNF, this.route, mimeType);
      return new HandlerFunction() {
        @Override
        public Route route() {
          return route;
        }

        @Override
        public Publisher handle(Payload firstPayload, Flux<Payload> payloads) {
          return handler.apply(firstPayload);
        }
      };
    }

    public HandlerFunction fireAndForget(Consumer<Payload> handler) {
      final Route route = new Route(FrameType.REQUEST_FNF, this.route, this.mimeType);
      return new HandlerFunction() {
        @Override
        public Route route() {
          return route;
        }

        @Override
        public Publisher handle(Payload firstPayload, Flux<Payload> payloads) {
          handler.accept(firstPayload);
          return Mono.empty();
        }
      };
    }

    public HandlerFunction requestResponse(Function<Payload, Mono<Payload>> handler) {
      final Route route = new Route(FrameType.REQUEST_RESPONSE, this.route, this.mimeType);
      return new HandlerFunction() {
        @Override
        public Route route() {
          return route;
        }

        @Override
        public Publisher handle(Payload firstPayload, Flux<Payload> payloads) {
          return handler.apply(firstPayload);
        }
      };
    }

    public HandlerFunction requestStream(Function<Payload, Flux<Payload>> handler) {
      final Route route = new Route(FrameType.REQUEST_STREAM, this.route, this.mimeType);
      return new HandlerFunction() {
        @Override
        public Route route() {
          return route;
        }

        @Override
        public Publisher handle(Payload firstPayload, Flux<Payload> payloads) {
          return handler.apply(firstPayload);
        }
      };
    }

    public HandlerFunction requestChannel(Function<Flux<Payload>, Flux<Payload>> handler) {
      final Route route = new Route(FrameType.REQUEST_CHANNEL, this.route, this.mimeType);
      return new HandlerFunction() {
        @Override
        public Route route() {
          return route;
        }

        @Override
        public Publisher handle(Payload firstPayload, Flux<Payload> payloads) {
          return handler.apply(payloads);
        }
      };
    }

    public HandlerFunction requestChannel(
        BiFunction<Payload, Flux<Payload>, Flux<Payload>> handler) {
      final Route route = new Route(FrameType.REQUEST_CHANNEL, this.route, this.mimeType);
      return new HandlerFunction() {
        @Override
        public Route route() {
          return route;
        }

        @Override
        public Publisher handle(Payload firstPayload, Flux<Payload> payloads) {
          return handler.apply(firstPayload, payloads);
        }
      };
    }
  }
}
