package de.tuberlin.aura.core.memory;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import de.tuberlin.aura.core.memory.spi.IAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public final class MemoryView {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final static Logger LOG = LoggerFactory.getLogger(MemoryView.class);

    public final IAllocator allocator;

    public final byte[] memory;

    public final int baseOffset;

    public final int size;

    private final AtomicInteger refCount;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    /**
     *
     * @param allocator
     * @param memory
     */
    public MemoryView(final IAllocator allocator, final byte[] memory) {
        this(allocator, memory, 0, memory.length);
    }

    /**
     *
     * @param allocator
     * @param memory
     * @param baseOffset
     * @param size
     */
    public MemoryView(final IAllocator allocator, final byte[] memory, int baseOffset, int size) {
        // sanity check.
        if (allocator == null)
            throw new IllegalArgumentException("allocator == null");
        if (memory == null)
            throw new IllegalArgumentException("memory == null");
        if (baseOffset < 0 && baseOffset >= memory.length)
            throw new IllegalArgumentException("bad baseOffset");
        if (size < 0 && size >= memory.length && baseOffset + size >= memory.length)
            throw new IllegalArgumentException("bad size");

        this.allocator = allocator;

        this.memory = memory;

        this.baseOffset = baseOffset;

        this.size = size;

        this.refCount = new AtomicInteger(0);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    /**
     *
     * @return
     */
    public int size() {
        return size;
    }

    /**
     *
     * @return
     */
    public byte[] copy() {
        return Arrays.copyOfRange(memory, baseOffset, baseOffset + size);
    }

    /**
     *
     * @param dst
     */
    public void copy(byte[] dst) {
        // sanity check.
        if (dst == null)
            throw new IllegalArgumentException("dst == null");
        System.arraycopy(memory, baseOffset, dst, 0, baseOffset + size);
    }

    /**
     *
     */
    public void free() {
        if (refCount.decrementAndGet() == 0) {
            if(allocator != null)
                allocator.free(this);
            //refCount.set(0);
        }

        if (refCount.get() < 0) {
            LOG.error("--> FAILURE: refCount(" + refCount.get() + ")" + " < 0");
        }
    }

    /**
     *
     * @return
     */
    public MemoryView weakCopy() {
        retain();
        return this;
    }

    /**
     *
     */
    public void retain() {
        refCount.getAndIncrement();
    }

    /**
     *
     */
    public void release() {
        refCount.getAndDecrement();
    }

    /**
     *
     * @return
     */
    public int getRefCount() {
        return refCount.get();
    }

    /**
     *
     * @param refCount
     */
    public void setRefCount(final int refCount) {
        // sanity check.
        if (refCount < 0)
            throw new IllegalArgumentException("refCount < 0");

        this.refCount.set(refCount);
    }
}
