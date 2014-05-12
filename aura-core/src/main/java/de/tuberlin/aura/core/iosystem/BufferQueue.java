package de.tuberlin.aura.core.iosystem;

import java.util.concurrent.TimeUnit;

import de.tuberlin.aura.core.statistic.MeasurementManager;

public interface BufferQueue<T> extends Iterable<T> {

    T take() throws InterruptedException;

    T poll(final long timeout, final TimeUnit timeUnit) throws InterruptedException;

    T poll();

    boolean offer(T value);

    void put(T value) throws InterruptedException;

    boolean isEmpty();

    public int size();

    public interface FACTORY<T> {

        BufferQueue<T> newInstance(String name, MeasurementManager measurementManager);
    }

    // measurement

    public String getName();

    public MeasurementManager getMeasurementManager();

    // observer

    public void registerObserver(QueueObserver observer);

    public void removeObserver(QueueObserver observer);

    public interface QueueObserver {

        public void signalNotFull();

        public void signalNotEmpty();

        public void signalNewElement();
    }
}
