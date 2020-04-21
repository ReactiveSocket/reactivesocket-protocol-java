package io.rsocket.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.rsocket.reflection.FieldHelper;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.assertj.core.api.Assertions;

/**
 * Additional Utils which allows to decorate a ByteBufAllocator and track/assertOnLeaks all created
 * ByteBuffs
 */
public class LeaksTrackingByteBufAllocator implements ByteBufAllocator {

  /**
   * Allows to Instruments the ByteBufAllocator.DEFAULT allocator and replace an existing one using
   * reflection
   *
   * @return
   */
  public static synchronized LeaksTrackingByteBufAllocator instrumentDefault() {
    if (ByteBufAllocator.DEFAULT instanceof LeaksTrackingByteBufAllocator) {
      return (LeaksTrackingByteBufAllocator) ByteBufAllocator.DEFAULT;
    }

    final LeaksTrackingByteBufAllocator instrumented =
        new LeaksTrackingByteBufAllocator(ByteBufAllocator.DEFAULT);
    try {
      Field field = ByteBufAllocator.class.getField("DEFAULT");
      FieldHelper.setFinalStatic(field, instrumented);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return instrumented;
  }

  public static synchronized void deinstrumentDefault() {
    if (ByteBufAllocator.DEFAULT instanceof LeaksTrackingByteBufAllocator) {
      LeaksTrackingByteBufAllocator instrumented =
          (LeaksTrackingByteBufAllocator) ByteBufAllocator.DEFAULT;
      try {
        Field field = ByteBufAllocator.class.getField("DEFAULT");
        FieldHelper.setFinalStatic(field, instrumented.delegate);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Allows to instrument any given the instance of ByteBufAllocator
   *
   * @param allocator
   * @return
   */
  public static LeaksTrackingByteBufAllocator instrument(ByteBufAllocator allocator) {
    return new LeaksTrackingByteBufAllocator(allocator);
  }

  final ConcurrentLinkedQueue<ByteBuf> tracker = new ConcurrentLinkedQueue<>();

  final ByteBufAllocator delegate;

  private LeaksTrackingByteBufAllocator(ByteBufAllocator delegate) {
    this.delegate = delegate;
  }

  public LeaksTrackingByteBufAllocator assertHasNoLeaks() {
    try {
      Assertions.assertThat(tracker)
          .allSatisfy(
              buf -> {
                if (buf instanceof CompositeByteBuf) {
                  if (buf.refCnt() > 0) {
                    List<ByteBuf> decomposed =
                        ((CompositeByteBuf) buf).decompose(0, buf.readableBytes());
                    for (int i = 0; i < decomposed.size(); i++) {
                      Assertions.assertThat(decomposed.get(i))
                          .matches(bb -> bb.refCnt() == 0, "Got unreleased CompositeByteBuf");
                    }
                  }

                } else {
                  Assertions.assertThat(buf)
                      .matches(bb -> bb.refCnt() == 0, "buffer should be released");
                }
              });
    } finally {
      tracker.clear();
    }
    return this;
  }

  // Delegating logic with tracking of buffers

  @Override
  public ByteBuf buffer() {
    return track(delegate.buffer());
  }

  @Override
  public ByteBuf buffer(int initialCapacity) {
    return track(delegate.buffer(initialCapacity));
  }

  @Override
  public ByteBuf buffer(int initialCapacity, int maxCapacity) {
    return track(delegate.buffer(initialCapacity, maxCapacity));
  }

  @Override
  public ByteBuf ioBuffer() {
    return track(delegate.ioBuffer());
  }

  @Override
  public ByteBuf ioBuffer(int initialCapacity) {
    return track(delegate.ioBuffer(initialCapacity));
  }

  @Override
  public ByteBuf ioBuffer(int initialCapacity, int maxCapacity) {
    return track(delegate.ioBuffer(initialCapacity, maxCapacity));
  }

  @Override
  public ByteBuf heapBuffer() {
    return track(delegate.heapBuffer());
  }

  @Override
  public ByteBuf heapBuffer(int initialCapacity) {
    return track(delegate.heapBuffer(initialCapacity));
  }

  @Override
  public ByteBuf heapBuffer(int initialCapacity, int maxCapacity) {
    return track(delegate.heapBuffer(initialCapacity, maxCapacity));
  }

  @Override
  public ByteBuf directBuffer() {
    return track(delegate.directBuffer());
  }

  @Override
  public ByteBuf directBuffer(int initialCapacity) {
    return track(delegate.directBuffer(initialCapacity));
  }

  @Override
  public ByteBuf directBuffer(int initialCapacity, int maxCapacity) {
    return track(delegate.directBuffer(initialCapacity, maxCapacity));
  }

  @Override
  public CompositeByteBuf compositeBuffer() {
    return track(delegate.compositeBuffer());
  }

  @Override
  public CompositeByteBuf compositeBuffer(int maxNumComponents) {
    return track(delegate.compositeBuffer(maxNumComponents));
  }

  @Override
  public CompositeByteBuf compositeHeapBuffer() {
    return track(delegate.compositeHeapBuffer());
  }

  @Override
  public CompositeByteBuf compositeHeapBuffer(int maxNumComponents) {
    return track(delegate.compositeHeapBuffer(maxNumComponents));
  }

  @Override
  public CompositeByteBuf compositeDirectBuffer() {
    return track(delegate.compositeDirectBuffer());
  }

  @Override
  public CompositeByteBuf compositeDirectBuffer(int maxNumComponents) {
    return track(delegate.compositeDirectBuffer(maxNumComponents));
  }

  @Override
  public boolean isDirectBufferPooled() {
    return delegate.isDirectBufferPooled();
  }

  @Override
  public int calculateNewCapacity(int minNewCapacity, int maxCapacity) {
    return delegate.calculateNewCapacity(minNewCapacity, maxCapacity);
  }

  <T extends ByteBuf> T track(T buffer) {
    tracker.offer(buffer);

    return buffer;
  }
}
