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
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

public interface HandlerFunction {

  Route route();

  @SuppressWarnings("rawtypes")
  default Publisher handle(Payload payload) {
    return handle(payload, null);
  }

  @SuppressWarnings("rawtypes")
  Publisher handle(Payload firstPayload, @Nullable Flux<Payload> payloads);
}
