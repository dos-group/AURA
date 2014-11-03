package de.tuberlin.aura.core.memory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import de.tuberlin.aura.core.memory.spi.IAllocator;
import de.tuberlin.aura.core.memory.spi.IBufferCallback;


public final class BufferAllocatorGroup implements IAllocator {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final int bufferSize;

    private final List<IAllocator> assignedAllocators;

    private AtomicInteger idxAlloc = new AtomicInteger(0);

    private AtomicInteger idxBlockingAlloc = new AtomicInteger(0);

    private AtomicInteger idxAllocWithCallBack = new AtomicInteger(0);

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public BufferAllocatorGroup(final int bufferSize, final List<IAllocator> initialAssignedAllocators) {
        // sanity check.
        if ((bufferSize & (bufferSize - 1)) != 0 && bufferSize < BufferAllocator.BufferSize._8K.bytes && bufferSize > BufferAllocator.BufferSize._64K.bytes)
            throw new IllegalArgumentException("illegal buffer size");
        if (initialAssignedAllocators == null)
            throw new IllegalArgumentException("initialAssignedAllocators == null");

        // check correct buffer sizes.
        for (final IAllocator allocator : initialAssignedAllocators) {
            if (allocator.getBufferSize() != bufferSize)
                throw new IllegalStateException("buffer size of allocator does not fit");
        }

        this.bufferSize = bufferSize;

        this.assignedAllocators = initialAssignedAllocators;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void addAllocator(final IAllocator allocator) {
        // sanity check.
        if (allocator == null)
            throw new IllegalArgumentException("allocator == null");
        if (allocator.getBufferSize() != bufferSize)
            throw new IllegalArgumentException("buffer size does not fit");

        assignedAllocators.add(allocator);
    }

    public IAllocator removeAllocator() {
        IAllocator notUsedAllocator = null;
        for (final IAllocator allocator : assignedAllocators) {
            if (allocator.isNotUsed()) {
                notUsedAllocator = allocator;
                break;
            }
        }
        return notUsedAllocator;
    }

    public IAllocator getAllocator(final int index) {
        // sanity check.
        if (index < 0)
            throw new IndexOutOfBoundsException("index < 0");
        if (index >= assignedAllocators.size())
            throw new IndexOutOfBoundsException("index < 0");

        return assignedAllocators.get(index);
    }

    @Override
    public MemoryView alloc() {

        //idxAlloc = ++idxAlloc % assignedAllocators.size();

        idxAlloc.set(idxAlloc.incrementAndGet() % assignedAllocators.size());

        return assignedAllocators.get(0).alloc();
    }

    @Override
    public MemoryView allocBlocking() throws InterruptedException {

        //idxBlockingAlloc = ++idxBlockingAlloc % assignedAllocators.size();

        idxBlockingAlloc.set(idxBlockingAlloc.incrementAndGet() % assignedAllocators.size());

        return assignedAllocators.get(0).allocBlocking();
    }

    @Override
    public MemoryView alloc(final IBufferCallback bufferCallback) {
        // sanity check.
        if (bufferCallback == null)
            throw new IllegalArgumentException("bufferCallback == null");

        //idxAllocWithCallBack = ++idxAllocWithCallBack % assignedAllocators.size();

        idxAllocWithCallBack.set(idxAllocWithCallBack.incrementAndGet() % assignedAllocators.size());

        return assignedAllocators.get(0).alloc(bufferCallback);
    }

    @Override
   public void free(MemoryView memory) {
        memory.free();
    }

    @Override
    public int getBufferSize() {
        return bufferSize;
    }

    @Override
    public boolean hasFree() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNotUsed() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void checkForMemoryLeaks() {
        for (final IAllocator allocator : assignedAllocators) {
            allocator.checkForMemoryLeaks();
        }
    }

    @Override
    public int getBufferCount() {
        int bufferCount = 0;
        for (final IAllocator allocator : assignedAllocators)
            bufferCount += allocator.getBufferCount();
        return bufferCount;
    }
}
