package de.tuberlin.aura.core.iosystem;

public interface BufferQueue<T> extends Iterable<T> {

    T take() throws InterruptedException;

    boolean offer(T value);

    void put(T value) throws InterruptedException;

    boolean isEmpty();

    public interface FACTORY<T> {

        BufferQueue<T> newInstance();
    }
}
