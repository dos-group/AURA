package de.tuberlin.aura.core.iosystem;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple wrapper for a {@link java.util.concurrent.LinkedBlockingQueue}.
 * 
 * @param <T>
 */
public class BlockingBufferQueue<T> implements BufferQueue<T> {

    public final static Logger LOG = LoggerFactory.getLogger(BlockingBufferQueue.class);

    private final BlockingQueue<T> backingQueue;

    BlockingBufferQueue() {
        this.backingQueue = new LinkedBlockingQueue<>();
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
    public boolean isEmpty() {
        return backingQueue.isEmpty();
    }

    @Override
    public Iterator<T> iterator() {
        return backingQueue.iterator();
    }

    @Override
    public String toString() {
        return backingQueue.toString();
    }

    public static class Factory<F> implements FACTORY<F> {

        @Override
        public BufferQueue<F> newInstance() {
            return new BlockingBufferQueue<>();
        }
    }
}
