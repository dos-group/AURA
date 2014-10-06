package de.tuberlin.aura.core.taskmanager.spi;

import org.slf4j.Logger;

public abstract class AbstractInvokeable implements IInvokeableLifecycle {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected ITaskRuntime runtime;

    protected IDataProducer producer;

    protected IDataConsumer consumer;

    protected Logger LOG;

    protected boolean isRunning;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public AbstractInvokeable() {

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

    public void setRuntime(final ITaskRuntime runtime) {
        // sanity check.
        if (runtime == null)
            throw new IllegalArgumentException("runtime == null");

        this.runtime = runtime;
    }

    public void setProducer(final IDataProducer producer) {
        // sanity check.
        if (producer == null)
            throw new IllegalArgumentException("producer == null");

        this.producer = producer;
    }

    public void setConsumer(final IDataConsumer consumer) {
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
