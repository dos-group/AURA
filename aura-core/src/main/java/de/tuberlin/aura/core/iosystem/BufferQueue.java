package de.tuberlin.aura.core.iosystem;

import de.tuberlin.aura.core.statistic.MeasurementManager;

import java.util.concurrent.TimeUnit;

public interface BufferQueue<T> extends Iterable<T> {

    T take() throws InterruptedException;

    T poll(final long timeout, final TimeUnit timeUnit) throws InterruptedException;

    boolean offer(T value);

    void put(T value) throws InterruptedException;

    boolean isEmpty();

    public int size();

    public interface FACTORY<T> {

        BufferQueue<T> newInstance(String name, MeasurementManager measurementManager);
    }
}
