package de.tuberlin.aura.core.task.spi;

import org.slf4j.Logger;

public abstract class AbstractInvokeable implements IExecutionLifecycle {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final ITaskDriver driver;

    protected final IDataProducer producer;

    protected final IDataConsumer consumer;

    protected final Logger LOG;

    protected boolean isRunning;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public AbstractInvokeable(final ITaskDriver driver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
        // sanity check.
        if (driver == null)
            throw new IllegalArgumentException("driver == null");
        if (producer == null)
            throw new IllegalArgumentException("producer == null");
        if (consumer == null)
            throw new IllegalArgumentException("consumer == null");
        if (LOG == null)
            throw new IllegalArgumentException("LOG == null");

        this.driver = driver;

        this.producer = producer;

        this.consumer = consumer;

        this.LOG = LOG;

        this.isRunning = true;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void create() throws Throwable {}

    public void open() throws Throwable {}

    public void close() throws Throwable {}

    public void release() throws Throwable {}

    public void stopInvokeable() {
        isRunning = false;
    }

    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------

    protected boolean isInvokeableRunning() {
        return isRunning;
    }
}
