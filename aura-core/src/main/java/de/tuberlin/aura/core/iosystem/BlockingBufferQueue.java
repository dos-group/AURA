package de.tuberlin.aura.core.iosystem;

import de.tuberlin.aura.core.statistic.AccumulatedLatencyMeasurement;
import de.tuberlin.aura.core.statistic.MeasurementManager;
import de.tuberlin.aura.core.statistic.MeasurementType;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class BlockingBufferQueue<T> implements BufferQueue<T> {

    private final Logger LOG = Logger.getRootLogger();

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
        //this.backingQueue = new ArrayBlockingQueue<>(1024, true);
    }

    @Override
    public T take() throws InterruptedException {
        DataHolder<T> val = backingQueue.take();

        this.sumLatency += (System.currentTimeMillis() - val.time);
        this.sumQueueSize += this.backingQueue.size();
        ++this.counter;

        if (this.counter == 1000) {
            AccumulatedLatencyMeasurement m = new AccumulatedLatencyMeasurement(MeasurementType.LATENCY, "Queue: " + this.name, -1, -1, (double) this.sumLatency / (double) this.counter, -1);
            this.measurementManager.add(m);

//            LOG.info(this.name + ": TIME_IN_QUEUE|" + Double.toString((double) this.sumLatency / (double) this.counter) + "|" +
//                    Double.toString((double) this.sumQueueSize / (double) this.counter));

            this.counter = 0;
            this.sumLatency = 0;
            this.sumQueueSize = 0;
        }

        return val.data;
    }

    @Override
    public void offer(T value) {

//      LOG.debug("PUT_QUEUE:" + Integer.toString(this.backingQueue.size()));
        DataHolder<T> holder = new DataHolder<>();
        holder.time = System.currentTimeMillis();
        holder.data = value;

        backingQueue.offer(holder);
    }

    @Override
    public int drainTo(Collection<? super T> dump) {

        // TODO: Fix this!
        return 0;
    }

    public static class Factory<F> implements BufferQueueFactory<F> {

        @Override
        public BufferQueue<F> newInstance(String name, MeasurementManager measurementManager) {
            return new BlockingBufferQueue<F>(name, measurementManager);
        }
    }

    public class DataHolder<T> {
        public long time;

        public T data;
    }
}
