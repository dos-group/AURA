package de.tuberlin.aura.core.memory;

import de.tuberlin.aura.core.memory.spi.IAllocator;

import java.util.List;

/**
 *
 */
public final class BufferAllocatorGroup implements IAllocator {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final int bufferSize;

    final List<IAllocator> assignedAllocators;

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
        for (final IAllocator allocator : assignedAllocators) {
            if (allocator.hasFree()) {
                return allocator.alloc();
            }
        }
        MemoryView memory = assignedAllocators.get(0).alloc();
        return memory;
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






}
