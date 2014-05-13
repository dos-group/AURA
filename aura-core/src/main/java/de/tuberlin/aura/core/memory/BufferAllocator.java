package de.tuberlin.aura.core.memory;

import de.tuberlin.aura.core.memory.spi.IAllocator;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 */
public final class BufferAllocator implements IAllocator {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final int _8K = 1024 * 8;

    public static final int _16K = 1024 * 16;

    public static final int _32K = 1024 * 32;

    public static final int _64K = 1024 * 64;

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final int bufferSize;

    private final int bufferCount;

    private final byte[] memoryArena;

    private final BlockingQueue<MemoryView> freeList;

    // private final Set<MemoryView> usedSet;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public BufferAllocator(final int bufferSize, final int bufferCount) {

        // sanity check.
        if ((bufferSize & (bufferSize - 1)) != 0 && bufferSize < _8K && bufferSize > _64K)
            throw new IllegalArgumentException("illegal buffer size");
        if (bufferCount <= 0)
            throw new IllegalArgumentException("bufferCount <= 0");
        if ((bufferCount * bufferSize) % _64K != 0)
            throw new IllegalArgumentException("allocated memory must be a multiple of 64K");

        this.bufferSize = bufferSize;

        this.bufferCount = bufferCount;

        this.memoryArena = new byte[bufferSize * bufferCount];

        this.freeList = new LinkedBlockingQueue<>();

        // this.usedSet = new HashSet<>();

        for (int i = 0; i < bufferCount; ++i) {
            this.freeList.add(new MemoryView(this, memoryArena, i * bufferSize, bufferSize));
        }
    }

    // ---------------------------------------------------
    // Public Methods: IAllocator Interface.
    // ---------------------------------------------------

    @Override
    public MemoryView alloc() {
        try {
            final MemoryView memory = freeList.take();

            // memory.acquire();
            // usedSet.add(memory);
            return memory;
        } catch (InterruptedException ie) {
            return null;
        }
    }

    @Override
    public void free(final MemoryView memory) {
        // sanity check.
        if (memory == null)
            throw new IllegalArgumentException("memory == null");

        // memory.release();
        // if(memory.getRefCount() == 0) {
        freeList.add(memory);
        // usedSet.remove(memory);
        // }
    }

    @Override
    public boolean hasFree() {
        return !freeList.isEmpty();
    }

    @Override
    public int getBufferSize() {
        return bufferSize;
    }

    @Override
    public boolean isNotUsed() {
        return false;// usedSet.isEmpty();
    }
}
