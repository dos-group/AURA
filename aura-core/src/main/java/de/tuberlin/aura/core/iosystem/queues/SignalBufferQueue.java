package de.tuberlin.aura.core.iosystem.queues;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple wrapper for a {@link java.util.concurrent.LinkedBlockingQueue}.
 * 
 * @param <T>
 */
public class SignalBufferQueue<T> implements BufferQueue<T> {

    public final static Logger LOG = LoggerFactory.getLogger(BlockingBufferQueue.class);

    private final BlockingQueue<T> backingQueue;

    private QueueObserver observer;

    SignalBufferQueue() {
        this.backingQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public T take() throws InterruptedException {

        return backingQueue.take();
    }

    @Override
    public boolean offer(T value) {
        backingQueue.offer(value);
        if (backingQueue.size() == 1) {
            observer.signalNotEmpty();
        }
        return true;
    }

    @Override
    public void put(T value) throws InterruptedException {
        backingQueue.put(value);
        if (backingQueue.size() == 1) {
            observer.signalNotEmpty();
        }
    }

    @Override
    public boolean isEmpty() {
        return backingQueue.isEmpty();
    }

    @Override
    public String toString() {
        return backingQueue.toString();
    }

    @Override
    public T poll(final long timeout, final TimeUnit timeUnit) throws InterruptedException {
        return backingQueue.poll(timeout, timeUnit);
    }

    @Override
    public T poll() {
        return backingQueue.poll();
    }

    @Override
    public int size() {
        return backingQueue.size();
    }

    @Override
    public void registerObserver(QueueObserver observer) {
        this.observer = observer;
    }

    @Override
    public void removeObserver(QueueObserver observer) {
        this.observer = null;
    }

    public static class Factory<F> implements FACTORY<F> {

        @Override
        public BufferQueue<F> newInstance() {
            return new BlockingBufferQueue<>();
        }
    }
}
