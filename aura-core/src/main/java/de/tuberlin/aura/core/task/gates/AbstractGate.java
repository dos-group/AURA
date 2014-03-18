package de.tuberlin.aura.core.task.gates;

import de.tuberlin.aura.core.task.common.TaskDriverContext;

public abstract class AbstractGate {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final TaskDriverContext context;

    protected final int numChannels;

    protected final int gateIndex;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    /**
     * @param context
     * @param gateIndex
     * @param numChannels
     */
    public AbstractGate(final TaskDriverContext context, int gateIndex, int numChannels) {
        // sanity check.
        if (context == null)
            throw new IllegalArgumentException("context == null");

        this.context = context;

        this.numChannels = numChannels;

        this.gateIndex = gateIndex;
    }
}
