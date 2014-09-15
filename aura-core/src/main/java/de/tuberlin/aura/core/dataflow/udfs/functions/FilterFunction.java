package de.tuberlin.aura.core.dataflow.udfs.functions;

import de.tuberlin.aura.core.dataflow.udfs.contracts.IFilterFunction;

/**
 *
 */
public abstract class FilterFunction<I> extends AbstractFunction implements IFilterFunction<I> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract boolean filter(final I in);
}
