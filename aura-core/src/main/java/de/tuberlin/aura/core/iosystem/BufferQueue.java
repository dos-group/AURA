package de.tuberlin.aura.core.iosystem;

import de.tuberlin.aura.core.statistic.MeasurementManager;

public interface BufferQueue<T> extends Iterable<T> {

    T take() throws InterruptedException;

    boolean offer(T value);

    void put(T value) throws InterruptedException;

    boolean isEmpty();

    public interface FACTORY<T> {

        BufferQueue<T> newInstance(String name, MeasurementManager measurementManager);
    }
}
