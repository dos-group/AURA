package de.tuberlin.aura.core.taskmanager.gates;

import de.tuberlin.aura.core.taskmanager.spi.ITaskRuntime;

public abstract class AbstractGate {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final ITaskRuntime runtime;

    protected final int numChannels;

    protected final int gateIndex;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public AbstractGate(final ITaskRuntime runtime, int gateIndex, int numChannels) {
        // sanity check.
        if (runtime == null)
            throw new IllegalArgumentException("runtime == null");

        this.runtime = runtime;

        this.numChannels = numChannels;

        this.gateIndex = gateIndex;
    }

    public int getNumOfChannels() {
        return numChannels;
    }
}
