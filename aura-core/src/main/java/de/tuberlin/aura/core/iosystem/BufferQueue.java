package de.tuberlin.aura.core.iosystem;

public interface BufferQueue<T> {

    T take() throws InterruptedException;

    void offer(T value);
}
