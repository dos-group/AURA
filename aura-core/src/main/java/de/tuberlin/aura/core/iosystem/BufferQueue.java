package de.tuberlin.aura.core.iosystem;

import java.util.concurrent.TimeUnit;

import de.tuberlin.aura.core.measurement.MeasurementManager;

public interface BufferQueue<T> extends Iterable<T> {

    T take() throws InterruptedException;

    T poll(final long timeout, final TimeUnit timeUnit) throws InterruptedException;

    boolean offer(T value);

    void put(T value) throws InterruptedException;

    boolean isEmpty();

    public int size();

    public MeasurementManager getMeasurementManager();

    public String getName();

    public interface FACTORY<T> {

        BufferQueue<T> newInstance(String name, MeasurementManager measurementManager);
    }
}
