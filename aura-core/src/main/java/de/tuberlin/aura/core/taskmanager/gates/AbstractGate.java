package de.tuberlin.aura.core.taskmanager.gates;

import de.tuberlin.aura.core.taskmanager.spi.ITaskDriver;

public abstract class AbstractGate {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final ITaskDriver taskDriver;

    protected final int numChannels;

    protected final int gateIndex;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    /**
     * @param taskDriver
     * @param gateIndex
     * @param numChannels
     */
    public AbstractGate(final ITaskDriver taskDriver, int gateIndex, int numChannels) {
        // sanity check.
        if (taskDriver == null)
            throw new IllegalArgumentException("driver == null");

        this.taskDriver = taskDriver;

        this.numChannels = numChannels;

        this.gateIndex = gateIndex;
    }

    /**
     *
     * @return
     */
    public int getNumOfChannels() {
        return numChannels;
    }
}
