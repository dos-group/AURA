package de.tuberlin.aura.core.iosystem;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockingBufferQueue<T> implements BufferQueue<T> {

    public final static Logger LOG = LoggerFactory.getLogger(BlockingBufferQueue.class);

    private final BlockingQueue<T> backingQueue;

    BlockingBufferQueue() {
        this.backingQueue = new ArrayBlockingQueue<>(3);
    }

    @Override
    public T take() throws InterruptedException {
        return backingQueue.take();
    }

    @Override
    public boolean offer(T value) {
        return backingQueue.offer(value);
    }

    @Override
    public void put(T value) throws InterruptedException {
        backingQueue.put(value);
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

    @Override
    public String toString() {
        return backingQueue.toString();
    }
}
