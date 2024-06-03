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

import io.rsocket.frame.FrameType;
import reactor.util.annotation.Nullable;

public final class Route {

  final String route;
  final String mimeType;
  final FrameType requestType;

  public Route(FrameType requestType, String route) {
    this(requestType, route, null);
  }

  public Route(FrameType requestType, String route, @Nullable String mimeType) {
    this.route = route;
    this.mimeType = mimeType;
    this.requestType = requestType;
  }

  public String route() {
    return this.route;
  }

  @Nullable
  public String mimeType() {
    return this.mimeType;
  }

  public FrameType requestType() {
    return requestType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Route route1 = (Route) o;

    if (!route.equals(route1.route)) {
      return false;
    }
    if (mimeType != null ? !mimeType.equals(route1.mimeType) : route1.mimeType != null) {
      return false;
    }
    return requestType == route1.requestType;
  }

  @Override
  public int hashCode() {
    int result = route.hashCode();
    result = 31 * result + (mimeType != null ? mimeType.hashCode() : 0);
    result = 31 * result + requestType.hashCode();
    return result;
  }
}
