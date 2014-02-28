package de.tuberlin.aura.core.iosystem;

public interface BufferQueue<T> extends Iterable<T> {

    T take() throws InterruptedException;

    boolean offer(T value);

    void put(T value) throws InterruptedException;

    int drainTo(BufferQueue<? super T> dump);

    boolean isEmpty();
}
