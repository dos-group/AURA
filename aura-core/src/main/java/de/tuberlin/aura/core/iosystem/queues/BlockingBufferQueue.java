package de.tuberlin.aura.core.iosystem.queues;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.statistic.AccumulatedLatencyMeasurement;
import de.tuberlin.aura.core.statistic.MeasurementManager;
import de.tuberlin.aura.core.statistic.MeasurementType;

/**
 * Simple wrapper for a {@link java.util.concurrent.LinkedBlockingQueue}.
 * 
 * @param <T>
 */
public class BlockingBufferQueue<T> implements BufferQueue<T> {

    public final static Logger LOG = LoggerFactory.getLogger(BlockingBufferQueue.class);

    private final BlockingQueue<DataHolder<T>> backingQueue;

    private String name;

    private long sumLatency;

    private long sumQueueSize;

    private int counter;

    private MeasurementManager measurementManager;

    BlockingBufferQueue(String name, MeasurementManager measurementManager) {
        this.name = name;
        this.measurementManager = measurementManager;
        this.backingQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public T take() throws InterruptedException {
        DataHolder<T> val = backingQueue.take();

        this.sumLatency += (System.currentTimeMillis() - val.time);
        this.sumQueueSize += this.backingQueue.size();
        ++this.counter;

        if (this.counter == 1000) {
            this.measurementManager.add(new AccumulatedLatencyMeasurement(MeasurementType.LATENCY,
                                                                          "Queue Latency -> " + this.name,
                                                                          -1,
                                                                          -1,
                                                                          (double) this.sumLatency / (double) this.counter,
                                                                          -1));
            this.measurementManager.add(new AccumulatedLatencyMeasurement(MeasurementType.LATENCY,
                                                                          "Queue Size -> " + this.name,
                                                                          -1,
                                                                          -1,
                                                                          (double) this.sumQueueSize / (double) this.counter,
                                                                          -1));

            // LOG.info(this.name + ": TIME_IN_QUEUE|" + Double.toString((double) this.sumLatency /
            // (double) this.counter) + "|" +
            // Double.toString((double) this.sumQueueSize / (double) this.counter));

            this.counter = 0;
            this.sumLatency = 0;
            this.sumQueueSize = 0;
        }

        return val.data;
    }

    @Override
    public boolean offer(T value) {

        // LOG.debug("PUT_QUEUE:" + Integer.toString(this.backingQueue.size()));
        DataHolder<T> holder = new DataHolder<>();
        holder.time = System.currentTimeMillis();
        holder.data = value;

        return backingQueue.offer(holder);
    }

    @Override
    public void put(T value) throws InterruptedException {
        DataHolder<T> holder = new DataHolder<>();
        holder.time = System.currentTimeMillis();
        holder.data = value;

        backingQueue.put(holder);
    }

    @Override
    public boolean isEmpty() {
        return backingQueue.isEmpty();
    }

    @Override
    public Iterator<T> iterator() {
        return new QueueIterator(this.backingQueue.iterator());
    }

    @Override
    public String toString() {
        return backingQueue.toString();
    }

    @Override
    public T poll(final long timeout, final TimeUnit timeUnit) throws InterruptedException {
        DataHolder<T> elem = backingQueue.poll(timeout, timeUnit);
        return (elem == null) ? null : elem.data;
    }

    @Override
    public T poll() {
        DataHolder<T> elem = backingQueue.poll();
        return (elem == null) ? null : elem.data;
    }

    @Override
    public int size() {
        return backingQueue.size();
    }

    @Override
    public MeasurementManager getMeasurementManager() {
        return this.measurementManager;
    }

    @Override
    public void registerObserver(QueueObserver observer) {}

    @Override
    public void removeObserver(QueueObserver observer) {}

    @Override
    public String getName() {
        return this.name;
    }

    public static class Factory<F> implements FACTORY<F> {

        @Override
        public BufferQueue<F> newInstance(String name, MeasurementManager measurementManager) {
            return new BlockingBufferQueue<>(name, measurementManager);
        }
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public class DataHolder<D> {

        public long time;

        public D data;
    }

    public class QueueIterator implements Iterator<T> {

        private Iterator<DataHolder<T>> backingIterator;

        public QueueIterator(Iterator<DataHolder<T>> backingIterator) {
            this.backingIterator = backingIterator;
        }

        @Override
        public boolean hasNext() {
            return backingIterator.hasNext();
        }

        @Override
        public T next() {
            return backingIterator.next().data;
        }

        @Override
        public void remove() {
            backingIterator.remove();
        }
    }
}
