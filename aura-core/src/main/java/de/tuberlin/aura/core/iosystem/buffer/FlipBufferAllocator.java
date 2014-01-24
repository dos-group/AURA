package de.tuberlin.aura.core.iosystem.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Wrapper around the {@see PooledByteBufAllocator} which provides buffers with the specified size only.
 * <p/>
 * Important: The parameters initialCapacity and maxCapacity are ignored in this implementation.
 * The Allocator does always allocate buffers with the size specified in the constructor.
 * Furthermore, composite buffers are not supported.
 * <p/>
 * As we have no chance to restrict the underlying PooledByteBufAllocator limit the buffers it allocates,
 * the user itself is responsible to release buffers first if the specified numberOfBuffers is reached in
 * order to keep size of used memory.
 */
public class FlipBufferAllocator implements ByteBufAllocator {

    private final static Logger LOG = LoggerFactory.getLogger(FlipBufferAllocator.class);

    private final PooledByteBufAllocator allocator;

    private final int bufferSize;

    public FlipBufferAllocator(int bufferSize, int numberOfBuffers, boolean direct) {
        // sanity
        if ((bufferSize & (bufferSize - 1)) != 0) {
            // not a power of two
            LOG.warn("The given buffer size is not a power of two.");

            this.bufferSize = Util.nextPowerOf2(bufferSize);
        } else {
            this.bufferSize = bufferSize;
        }

        PooledByteBufAllocator allocator = new PooledByteBufAllocator(
                direct,
                (direct ? 0 : 1),
                (direct ? 1 : 0),
                this.bufferSize * numberOfBuffers,
                0);

        this.allocator = allocator;
    }

    @Override
    public ByteBuf buffer() {
        return allocator.buffer(bufferSize, bufferSize);
    }

    @Override
    public ByteBuf buffer(int initialCapacity) {
        return allocator.buffer(bufferSize, bufferSize);
    }

    @Override
    public ByteBuf buffer(int initialCapacity, int maxCapacity) {
        return allocator.buffer(bufferSize, bufferSize);
    }

    @Override
    public ByteBuf ioBuffer() {
        return allocator.ioBuffer(bufferSize, bufferSize);
    }

    @Override
    public ByteBuf ioBuffer(int initialCapacity) {
        return allocator.ioBuffer(bufferSize, bufferSize);
    }

    @Override
    public ByteBuf ioBuffer(int initialCapacity, int maxCapacity) {
        return allocator.ioBuffer(bufferSize, bufferSize);
    }

    @Override
    public ByteBuf heapBuffer() {
        return allocator.heapBuffer(bufferSize, bufferSize);
    }

    @Override
    public ByteBuf heapBuffer(int initialCapacity) {
        return allocator.heapBuffer(bufferSize, bufferSize);
    }

    @Override
    public ByteBuf heapBuffer(int initialCapacity, int maxCapacity) {
        return allocator.heapBuffer(bufferSize, bufferSize);
    }

    @Override
    public ByteBuf directBuffer() {
        return allocator.directBuffer(bufferSize, bufferSize);
    }

    @Override
    public ByteBuf directBuffer(int initialCapacity) {
        return allocator.directBuffer(bufferSize, bufferSize);
    }

    @Override
    public ByteBuf directBuffer(int initialCapacity, int maxCapacity) {
        return allocator.directBuffer(bufferSize, bufferSize);
    }

    @Override
    public CompositeByteBuf compositeBuffer() {
        LOG.error("Tried to allocate compositeBuffer.");
        throw new UnsupportedOperationException();
    }

    @Override
    public CompositeByteBuf compositeBuffer(int maxNumComponents) {
        LOG.error("Tried to allocate compositeBuffer.");
        throw new UnsupportedOperationException();
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer() {
        LOG.error("Tried to allocate compositeHeapBuffer.");
        throw new UnsupportedOperationException();
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer(int maxNumComponents) {
        LOG.error("Tried to allocate compositeHeapBuffer.");
        throw new UnsupportedOperationException();
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer() {
        LOG.error("Tried to allocate compositeDirectBuffer.");
        throw new UnsupportedOperationException();
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer(int maxNumComponents) {
        LOG.error("Tried to allocate compositeDirectBuffer.");
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDirectBufferPooled() {
        return allocator.isDirectBufferPooled();
    }
}
