package de.tuberlin.aura.core.iosystem;

import java.util.Collection;

public interface BufferQueue<T> {

    T take() throws InterruptedException;

    void offer(T value);

    int drainTo(Collection<? super T> dump);
}
