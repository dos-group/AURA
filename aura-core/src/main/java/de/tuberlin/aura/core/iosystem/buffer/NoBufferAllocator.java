package de.tuberlin.aura.core.iosystem.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;

public class NoBufferAllocator implements ByteBufAllocator {
    @Override
    public ByteBuf buffer() {
        return null;
    }

    @Override
    public ByteBuf buffer(int initialCapacity) {
        return null;
    }

    @Override
    public ByteBuf buffer(int initialCapacity, int maxCapacity) {
        return null;
    }

    @Override
    public ByteBuf ioBuffer() {
        return null;
    }

    @Override
    public ByteBuf ioBuffer(int initialCapacity) {
        return null;
    }

    @Override
    public ByteBuf ioBuffer(int initialCapacity, int maxCapacity) {
        return null;
    }

    @Override
    public ByteBuf heapBuffer() {
        return null;
    }

    @Override
    public ByteBuf heapBuffer(int initialCapacity) {
        return null;
    }

    @Override
    public ByteBuf heapBuffer(int initialCapacity, int maxCapacity) {
        return null;
    }

    @Override
    public ByteBuf directBuffer() {
        return null;
    }

    @Override
    public ByteBuf directBuffer(int initialCapacity) {
        return null;
    }

    @Override
    public ByteBuf directBuffer(int initialCapacity, int maxCapacity) {
        return null;
    }

    @Override
    public CompositeByteBuf compositeBuffer() {
        return null;
    }

    @Override
    public CompositeByteBuf compositeBuffer(int maxNumComponents) {
        return null;
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer() {
        return null;
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer(int maxNumComponents) {
        return null;
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer() {
        return null;
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer(int maxNumComponents) {
        return null;
    }

    @Override
    public boolean isDirectBufferPooled() {
        return false;
    }
}
