package de.tuberlin.aura.core.dataflow.datasets;

import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;

import java.util.Collection;

public abstract class AbstractDataset<E> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    final protected IExecutionContext context;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public AbstractDataset(final IExecutionContext context) {
        // sanity check.
        if (context == null)
            throw new IllegalArgumentException("context == null");

        this.context = context;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract void add(final E element);

    public abstract Collection<E> getData();
}
