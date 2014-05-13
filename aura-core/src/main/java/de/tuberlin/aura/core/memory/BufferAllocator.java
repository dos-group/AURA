package de.tuberlin.aura.core.memory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public final class BufferAllocator implements IAllocator {

    private final static Logger LOG = LoggerFactory.getLogger(BufferAllocator.class);

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

    private final ConcurrentLinkedQueue<BufferCallback> callbackList;

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

        this.callbackList = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < bufferCount; ++i) {
            this.freeList.add(new MemoryView(this, memoryArena, i * bufferSize, bufferSize));
        }
    }

    // ---------------------------------------------------
    // Public Methods: IAllocator Interface.
    // ---------------------------------------------------

    @Override
    public synchronized MemoryView alloc() {
        final MemoryView memory = freeList.poll();
        return memory;
    }

    @Override
    public synchronized MemoryView allocBlocking() throws InterruptedException {
        MemoryView view = freeList.poll(10, TimeUnit.SECONDS);
        if (view == null) {
            logStatus();
            view = freeList.take();
        }
        return view;
    }

    private void logStatus() {
        LOG.info("buffer count :" + bufferCount);
        LOG.info("freelist size: " + freeList.size());
        LOG.info("callbacks    : " + callbackList.size());
    }

    @Override
    public synchronized MemoryView alloc(final BufferCallback callback) {
        // sanity check.
        if (callback == null)
            throw new IllegalArgumentException("callback == null");

        final MemoryView memory = freeList.poll();

        if (memory == null) {
            callbackList.add(callback);
        }
        return memory;
    }

    @Override
    public synchronized void free(final MemoryView memory) {
        // sanity check.
        if (memory == null)
            throw new IllegalArgumentException("memory == null");

        if (!callbackList.isEmpty()) {
            BufferCallback bufferCallback = callbackList.poll();
            bufferCallback.bufferReader(memory);
        } else {
            freeList.add(memory);
        }
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
        return false;
    }
}
