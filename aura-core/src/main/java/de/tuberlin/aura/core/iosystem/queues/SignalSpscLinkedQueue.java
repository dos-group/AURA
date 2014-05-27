package de.tuberlin.aura.core.iosystem.queues;

import static de.tuberlin.aura.core.common.utils.UnsafeAccess.UNSAFE;

import java.util.concurrent.TimeUnit;

abstract class SpscLinkedQueuePad0<E> {

    long p00, p01, p02, p03, p04, p05, p06, p07;

    long p30, p31, p32, p33, p34, p35, p36, p37;
}


abstract class SpscLinkedQueueHead<E> extends SpscLinkedQueuePad0<E> {

    protected LinkedQueueNode<E> head = new LinkedQueueNode<>();
}


abstract class SpscLinkedQueuePad1<E> extends SpscLinkedQueueHead<E> {

    long p00, p01, p02, p03, p04, p05, p06, p07;

    long p30, p31, p32, p33, p34, p35, p36, p37;
}


abstract class SpscLinkedQueueTail<E> extends SpscLinkedQueuePad1<E> {

    protected LinkedQueueNode<E> tail = head;
}


public final class SignalSpscLinkedQueue<E> extends SpscLinkedQueueTail<E> implements BufferQueue<E> {

    long p00, p01, p02, p03, p04, p05, p06, p07;

    long p30, p31, p32, p33, p34, p35, p36, p37;

    private final static long EMPTY_OFFSET;

    static {
        try {
            EMPTY_OFFSET = UNSAFE.objectFieldOffset(SignalSpscLinkedQueue.class.getDeclaredField("empty"));
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private volatile int empty = 1;

    @Override
    public boolean offer(E e) {
        if (e == null) {
            throw new IllegalArgumentException("null elements not allowed");
        }
        LinkedQueueNode<E> n = new LinkedQueueNode<>();
        n.spValue(e);
        head.soNext(n);
        head = n;
        if (UNSAFE.compareAndSwapInt(this, EMPTY_OFFSET, 1, 0)) {
            observer.signalNotEmpty();
        }
        return true;
    }

    @Override
    public E poll() {
        LinkedQueueNode<E> n = tail.lvNext();
        if (n != null) {
            tail = n;
            return n.lpValue();
        }
        // UNSAFE.putIntVolatile(this, EMPTY_OFFSET, 1);
        empty = 1;
        return null;
    }

    public E peek() {
        return tail.lvValue();
    }

    @Override
    public int size() {
        LinkedQueueNode<E> temp = tail;
        int size = 0;
        while ((temp = temp.lvNext()) != null) {
            size++;
        }
        return size;
    }


    // --------------------------------------------------------------
    // BUFFER QUEUE IMPLEMENTATIONS
    // --------------------------------------------------------------

    private QueueObserver observer;


    @Override
    public E take() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public E poll(long timeout, TimeUnit timeUnit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(E value) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        return empty == 1;
    }

    @Override
    public void registerObserver(QueueObserver observer) {
        this.observer = observer;
    }

    @Override
    public void removeObserver(QueueObserver observer) {
        this.observer = null;
    }

    public static class Factory<F> implements FACTORY<F> {

        @Override
        public BufferQueue<F> newInstance() {
            return new SignalSpscLinkedQueue<>();
        }
    }
}
