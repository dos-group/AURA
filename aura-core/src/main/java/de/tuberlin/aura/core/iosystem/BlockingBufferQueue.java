package de.tuberlin.aura.core.iosystem;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class BlockingBufferQueue<T> implements BufferQueue<T> {

    private final BlockingQueue<T> backingQueue;

    BlockingBufferQueue() {
        this.backingQueue = new LinkedBlockingQueue<T>();
    }

    @Override
    public T take() throws InterruptedException {
        return backingQueue.take();
    }

    @Override
    public void offer(T value) {
        backingQueue.offer(value);
    }


    public static class Factory<F> implements BufferQueueFactory<F> {

        @Override
        public BufferQueue newInstance() {
            return new BlockingBufferQueue<F>();
        }
    }
}
