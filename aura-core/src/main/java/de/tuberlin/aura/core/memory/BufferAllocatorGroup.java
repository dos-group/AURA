package de.tuberlin.aura.core.memory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import de.tuberlin.aura.core.memory.spi.IAllocator;

/**
 *
 */
public final class BufferAllocatorGroup implements IAllocator {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final int bufferSize;

    private final List<IAllocator> assignedAllocators;

    private final AtomicInteger allocCounter;

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public BufferAllocatorGroup(final int bufferSize, final List<IAllocator> initialAssignedAllocators) {
        // sanity check.
        if ((bufferSize & (bufferSize - 1)) != 0 && bufferSize < BufferAllocator._8K && bufferSize > BufferAllocator._64K)
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

        this.allocCounter = new AtomicInteger(0);
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

    // ---------------------------------------------------
    // Public Methods: IAllocator Interface.
    // ---------------------------------------------------

    @Override
    public MemoryView alloc() {
        // for (final IAllocator allocator : assignedAllocators) {
        // if (allocator.hasFree()) {
        // return allocator.alloc();
        // }
        // }
        return assignedAllocators.get(0).alloc();
    }

    @Override
    public MemoryView allocBlocking() throws InterruptedException {
        // for (final IAllocator allocator : assignedAllocators) {
        // if (allocator.hasFree()) {
        // return allocator.allocBlocking();
        // }
        // }
        return assignedAllocators.get(0).allocBlocking();
    }

    @Override
    public MemoryView alloc(final BufferCallback bufferCallback) {
        // sanity check.
        if (bufferCallback == null)
            throw new IllegalArgumentException("bufferCallback == null");

        // if (allocCounter.get() == Integer.MAX_VALUE)
        // allocCounter.set(0);
        //
        // final int count = allocCounter.getAndIncrement();
        //
        // int allocatorIndex = count % (assignedAllocators.size());
        // int tmpAllocatorIndex = allocatorIndex;
        // int index = 0;
        //
        // while (index++ < assignedAllocators.size()) {
        // final IAllocator allocator = assignedAllocators.get((tmpAllocatorIndex++) %
        // (assignedAllocators.size()));
        // if (allocator.hasFree()) {
        // return allocator.alloc();
        // }
        // }
        //
        // return assignedAllocators.get(allocatorIndex).alloc(bufferCallback);
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
}
