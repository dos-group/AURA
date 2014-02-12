package de.tuberlin.aura.core.iosystem;

public interface BufferQueue<T> extends Iterable<T> {

    T take() throws InterruptedException;

    void offer(T value);

    int drainTo(BufferQueue<? super T> dump);

    boolean isEmpty();
}
