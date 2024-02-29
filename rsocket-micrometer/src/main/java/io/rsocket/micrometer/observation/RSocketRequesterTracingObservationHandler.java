/*
 * Copyright 2013-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.micrometer.observation;

import io.micrometer.observation.Observation;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.TracingObservationHandler;
import io.micrometer.tracing.internal.EncodingUtils;
import io.micrometer.tracing.propagation.Propagator;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.rsocket.Payload;
import io.rsocket.metadata.CompositeMetadataCodec;
import io.rsocket.metadata.TracingMetadataCodec;
import io.rsocket.metadata.WellKnownMimeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;

public class RSocketRequesterTracingObservationHandler
    implements TracingObservationHandler<RSocketContext> {
  private static final Logger log =
      LoggerFactory.getLogger(RSocketRequesterTracingObservationHandler.class);

  private final Propagator propagator;

  private final Propagator.Setter<CompositeByteBuf> setter;

  private final Tracer tracer;

  private final boolean isZipkinPropagationEnabled;

  public RSocketRequesterTracingObservationHandler(
      Tracer tracer,
      Propagator propagator,
      Propagator.Setter<CompositeByteBuf> setter,
      boolean isZipkinPropagationEnabled) {
    this.tracer = tracer;
    this.propagator = propagator;
    this.setter = setter;
    this.isZipkinPropagationEnabled = isZipkinPropagationEnabled;
  }

  @Override
  public boolean supportsContext(Observation.Context context) {
    return context instanceof RSocketContext
        && ((RSocketContext) context).side == RSocketContext.Side.REQUESTER;
  }

  @Override
  public Tracer getTracer() {
    return this.tracer;
  }

  @Override
  public void onStart(RSocketContext context) {
    Payload payload = context.payload;
    Span.Builder spanBuilder = this.tracer.spanBuilder();
    Span parentSpan = getParentSpan(context);
    if (parentSpan != null) {
      spanBuilder.setParent(parentSpan.context());
    }
    Span span = spanBuilder.kind(Span.Kind.PRODUCER).start();
    log.debug("Extracted result from context or thread local {}", span);
    // TODO: newmetadata returns an empty composite byte buf
    final CompositeByteBuf newMetadata =
        PayloadUtils.cleanTracingMetadata(payload, new HashSet<>(propagator.fields()));
    TraceContext traceContext = span.context();
    if (this.isZipkinPropagationEnabled) {
      injectDefaultZipkinRSocketHeaders(newMetadata, traceContext);
    }
    this.propagator.inject(traceContext, newMetadata, this.setter);
    context.modifiedPayload = PayloadUtils.payload(payload, newMetadata);
    getTracingContext(context).setSpan(span);
  }

  @Override
  public void onError(RSocketContext context) {
    Throwable error = context.getError();
    if (error != null) {
      getRequiredSpan(context).error(error);
    }
  }

  @Override
  public void onStop(RSocketContext context) {
    Span span = getRequiredSpan(context);
    tagSpan(context, span);
    span.name(context.getContextualName()).end();
  }

  private void injectDefaultZipkinRSocketHeaders(
      CompositeByteBuf newMetadata, TraceContext traceContext) {
    TracingMetadataCodec.Flags flags =
        traceContext.sampled() == null
            ? TracingMetadataCodec.Flags.UNDECIDED
            : traceContext.sampled()
                ? TracingMetadataCodec.Flags.SAMPLE
                : TracingMetadataCodec.Flags.NOT_SAMPLE;
    String traceId = traceContext.traceId();
    long[] traceIds = EncodingUtils.fromString(traceId);
    long[] spanId = EncodingUtils.fromString(traceContext.spanId());
    long[] parentSpanId = EncodingUtils.fromString(traceContext.parentId());
    boolean isTraceId128Bit = traceIds.length == 2;
    final ByteBufAllocator allocator = newMetadata.alloc();
    if (isTraceId128Bit) {
      CompositeMetadataCodec.encodeAndAddMetadata(newMetadata,
        allocator, WellKnownMimeType.MESSAGE_RSOCKET_TRACING_ZIPKIN,
        TracingMetadataCodec.encode128(
          allocator,
          traceIds[0],
          traceIds[1],
          spanId[0],
          EncodingUtils.fromString(traceContext.parentId())[0],
          flags));
    } else {
      CompositeMetadataCodec.encodeAndAddMetadata(newMetadata,
        allocator, WellKnownMimeType.MESSAGE_RSOCKET_TRACING_ZIPKIN,
        TracingMetadataCodec.encode64(
          allocator, traceIds[0], spanId[0], parentSpanId[0], flags));
    }
  }
}
