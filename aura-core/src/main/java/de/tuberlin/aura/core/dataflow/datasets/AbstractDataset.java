package de.tuberlin.aura.core.dataflow.datasets;

import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;

import java.util.Collection;

public abstract class AbstractDataset<E> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    final protected IExecutionContext environment;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public AbstractDataset(final IExecutionContext environment) {
        // sanity check.
        if (environment == null)
            throw new IllegalArgumentException("environment == null");

        this.environment = environment;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract void add(final E element);

    public abstract Collection<E> getData();
}
