package de.tuberlin.aura.core.iosystem;

public interface BufferQueueFactory<T> {

    BufferQueue<T> newInstance(String name);
}
