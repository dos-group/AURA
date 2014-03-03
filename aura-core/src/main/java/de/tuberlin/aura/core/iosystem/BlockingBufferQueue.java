package de.tuberlin.aura.core.iosystem;

import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class BlockingBufferQueue<T> implements BufferQueue<T> {

    private final Logger LOG = Logger.getRootLogger();

    private final BlockingQueue<T> backingQueue;

    private HashMap<T, Long> queuingTimes;

    BlockingBufferQueue() {
        this.backingQueue = new ArrayBlockingQueue<>(1000, true);
        this.queuingTimes = new HashMap<>();
    }

    @Override
    public T take() throws InterruptedException {
        T val = backingQueue.take();
        LOG.debug("TIME_IN_QUEUE:" + Long.toString(System.currentTimeMillis() - this.queuingTimes.get(val)) + ":" +
                Integer.toString(this.backingQueue.size()));

        return val;
    }

    @Override
    public void offer(T value) {

        LOG.debug("PUT_QUEUE:" + Integer.toString(this.backingQueue.size()));

        this.queuingTimes.put(value, System.currentTimeMillis());
        backingQueue.offer(value);
    }

    @Override
    public int drainTo(Collection<? super T> dump) {
        return backingQueue.drainTo(dump);
    }

    public static class Factory<F> implements BufferQueueFactory<F> {

        @Override
        public BufferQueue<F> newInstance() {
            return new BlockingBufferQueue<F>();
        }
    }
}
