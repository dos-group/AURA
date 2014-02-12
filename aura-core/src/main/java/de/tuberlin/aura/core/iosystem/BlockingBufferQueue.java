package de.tuberlin.aura.core.iosystem;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class BlockingBufferQueue<T> implements BufferQueue<T> {

    private final BlockingQueue<T> backingQueue;

    BlockingBufferQueue() {
        this.backingQueue = new ArrayBlockingQueue<>(10);
    }

    @Override
    public T take() throws InterruptedException {
        return backingQueue.take();
    }

    @Override
    public void offer(T value) {
        backingQueue.offer(value);
    }

    @Override
    public int drainTo(BufferQueue<? super T> dump) {

        // TODO: hack to check if the mechanism is working. FIX!!!
        if (dump instanceof BlockingBufferQueue) {
            return this.backingQueue.drainTo(((BlockingBufferQueue) dump).backingQueue);
        }

        throw new IllegalArgumentException("not proper implemented yet.");
    }

    @Override
    public boolean isEmpty() {
        return backingQueue.isEmpty();
    }

    @Override
    public Iterator<T> iterator() {
        return backingQueue.iterator();
    }

    public static class Factory<F> implements BufferQueueFactory<F> {

        @Override
        public BufferQueue<F> newInstance() {
            return new BlockingBufferQueue<F>();
        }
    }
}
