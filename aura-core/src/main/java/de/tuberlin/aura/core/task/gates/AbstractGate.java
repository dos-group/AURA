package de.tuberlin.aura.core.task.gates;

import de.tuberlin.aura.core.task.common.TaskRuntimeContext;

public abstract class AbstractGate {

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public AbstractGate(final TaskRuntimeContext context, int gateIndex, int numChannels) {
        // sanity check.
        if (context == null)
            throw new IllegalArgumentException("context == null");

        this.context = context;

        this.numChannels = numChannels;

        this.gateIndex = gateIndex;
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final TaskRuntimeContext context;

    protected final int numChannels;

    protected final int gateIndex;

    // ---------------------------------------------------
    // Public.
    // ---------------------------------------------------

    public int getNumChannels() {
        return this.numChannels;
    }

}
