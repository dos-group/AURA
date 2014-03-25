package de.tuberlin.aura.core.iosystem;

import de.tuberlin.aura.core.statistic.MeasurementManager;

public interface BufferQueueFactory<T> {

    BufferQueue<T> newInstance(String name, MeasurementManager measurementManager);
}
