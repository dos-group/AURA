package de.tuberlin.aura.core.task.spi;

import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import org.slf4j.Logger;

public abstract class AbstractInvokeable implements IExecutionLifecycle {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected ITaskDriver driver;

    protected IDataProducer producer;

    protected IDataConsumer consumer;

    protected Logger LOG;

    protected boolean isRunning;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public AbstractInvokeable() {
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

    public void setTaskDriver(final ITaskDriver driver) {
        // sanity check.
        if (driver == null)
            throw new IllegalArgumentException("driver == null");

        this.driver = driver;
    }

    public void setDataProducer(final IDataProducer producer) {
        // sanity check.
        if (producer == null)
            throw new IllegalArgumentException("producer == null");

        this.producer = producer;
    }

    public void setDataConsumer(final IDataConsumer consumer) {
        // sanity check.
        if (consumer == null)
            throw new IllegalArgumentException("consumer == null");

        this.consumer = consumer;
    }

    public void setLogger(final Logger LOG) {
        // sanity check.
        if (LOG == null)
            throw new IllegalArgumentException("LOG == null");

        this.LOG = LOG;
    }

    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------

    protected boolean isInvokeableRunning() {
        return isRunning;
    }
}
