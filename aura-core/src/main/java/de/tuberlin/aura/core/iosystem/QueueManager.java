package de.tuberlin.aura.core.iosystem;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;

import de.tuberlin.aura.core.statistic.MeasurementManager;

public class QueueManager<T> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final static Logger LOG = org.slf4j.LoggerFactory.getLogger(QueueManager.class);

    public static Map<UUID, QueueManager> BINDINGS = new HashMap<UUID, QueueManager>();

    private final Map<Integer, BufferQueue<T>> inputQueues;

    private final Map<LongKey, BufferQueue<T>> outputQueues;

    private final BufferQueue.FACTORY<T> queueFactory;

    private final MeasurementManager measurementManager;

    private int inputQueuesCounter;

    private int outputQueuesCounter;

    private QueueManager(BufferQueue.FACTORY<T> factory, MeasurementManager measurementManager) {
        this.inputQueues = new HashMap<>();
        this.outputQueues = new HashMap<>();
        this.queueFactory = factory;
        this.measurementManager = measurementManager;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    /**
     * @param taskID
     * @param queueFactory
     * @param <F>
     * @return
     */
    public static <F> QueueManager<F> newInstance(UUID taskID, BufferQueue.FACTORY<F> queueFactory, MeasurementManager measurementManager) {
        QueueManager<F> instance = new QueueManager<>(queueFactory, measurementManager);
        BINDINGS.put(taskID, instance);
        return instance;
    }

    /**
     * [Christian] TODO: Synchronized necessary -> concurrent access from ConsumerEventHandler?
     * 
     * @param gateIndex
     * @return
     */
    public synchronized BufferQueue<T> getInputQueue(int gateIndex) {

        if (inputQueues.containsKey(gateIndex)) {
            return inputQueues.get(gateIndex);
        }

        final BufferQueue<T> queue = queueFactory.newInstance("InputQueue " + Integer.toString(inputQueuesCounter), this.measurementManager);
        inputQueues.put(gateIndex, queue);
        ++this.inputQueuesCounter;

        return queue;
    }

    /**
     * [Christian] TODO: Synchronized -> concurrent access from ProducerEventHandler?
     * 
     * @param gateIndex
     * @param channelIndex
     * @return
     */
    public synchronized BufferQueue<T> getOutputQueue(int gateIndex, int channelIndex) {

        final LongKey key = new LongKey(gateIndex, channelIndex);
        if (outputQueues.containsKey(key)) {
            return outputQueues.get(key);
        }

        final BufferQueue<T> queue = queueFactory.newInstance("OutputQueue " + Integer.toString(outputQueuesCounter), this.measurementManager);
        outputQueues.put(key, queue);
        ++this.outputQueuesCounter;

        return queue;
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    /**
     *
     */
    public enum GATE {
        IN,
        OUT
    }

    /**
     * We assume here that values for gate and channel do not exceed 16 bit (which is reasonable as
     * then u can have up to 65535 bufferQueues per gate and 65535 gates).
     */
    private static class LongKey {

        int gateIndex;

        int channelIndex;

        LongKey(int gateIndex, int channelIndex) {
            this.gateIndex = gateIndex;
            this.channelIndex = channelIndex;
        }

        @Override
        public int hashCode() {
            return gateIndex ^ (channelIndex >>> 16);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null)
                return false;
            if (!(obj instanceof LongKey))
                return false;
            if (hashCode() != obj.hashCode())
                return false;

            LongKey other = (LongKey) obj;
            return gateIndex == other.gateIndex && channelIndex == other.channelIndex;
        }
    }
}
